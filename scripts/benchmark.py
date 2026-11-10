"""WordCount on HDFS/YARN: input sizes, repetitions, validation, and CSV. Run in a regular terminal."""
import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import platform
import re
from statistics import median, mean, stdev
import sys
import uuid

import cluster
from check_wordcount import read_counts
from run_wordcount import PROFILE

ROOT = Path(__file__).resolve().parents[1]


def schedule(sizes, runs):
    # Run both engines on each input size in every round.
    for repetition in range(1, runs + 1):
        for index, size in enumerate(sizes):
            engines = ('hadoop', 'spark') if (repetition + index) % 2 else ('spark', 'hadoop')
            for engine in engines:
                yield size, repetition, engine


def make_sample(source, target, size_mib):
    """Copy a prefix ending at a complete newline without loading the full file into RAM."""
    limit = size_mib * 1024 * 1024
    if source.stat().st_size < limit:
        raise ValueError(f'Not enough data in {source} for {size_mib} MiB')
    written = 0
    last_newline = 0
    with source.open('rb') as inp, target.open('xb') as out:
        while written < limit:
            block = inp.read(min(1024 * 1024, limit - written))
            if not block:
                raise ValueError('The source file changed while copying')
            newline = block.rfind(b'\n')
            if newline >= 0:
                last_newline = written + newline + 1
            out.write(block)
            written += len(block)
        if not last_newline:
            raise ValueError('The prefix contains no complete line')
        out.truncate(last_newline)
    with target.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    return {'requested_mib': size_mib, 'actual_bytes': last_newline, 'sha256': digest}


def write_csv(path, rows):
    with path.open('w', newline='', encoding='utf-8') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def summarize(rows):
    result = []
    for size in sorted({row['requested_mib'] for row in rows}):
        medians = {}
        for engine in ('hadoop', 'spark'):
            group = [row for row in rows if row['requested_mib'] == size and row['engine'] == engine]
            values = [row['elapsed_seconds'] for row in group]
            medians[engine] = median(values)
            result.append({'requested_mib': size, 'actual_bytes': group[0]['actual_bytes'],
                           'engine': engine, 'runs': len(values), 'median_seconds': median(values),
                           'mean_seconds': mean(values), 'min_seconds': min(values),
                           'max_seconds': max(values),
                           'stdev_seconds': stdev(values) if len(values) > 1 else 0.0})
        ratio = medians['hadoop'] / medians['spark']
        for row in result[-2:]:
            row['hadoop_over_spark_median'] = ratio
        print(f'{size:4d} MiB | Hadoop {medians["hadoop"]:.3f} s | '
              f'Spark {medians["spark"]:.3f} s | Hadoop/Spark {ratio:.3f}')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, default=ROOT / 'data/wikipedia.txt')
    parser.add_argument('--sizes-mib', type=int, nargs='+', default=[10, 100, 500])
    parser.add_argument('--runs', type=int, default=3)
    parser.add_argument('--plan', action='store_true', help='Show the schedule without running jobs')
    parser.add_argument('--output', type=Path, help='New result directory; default is unique')
    args = parser.parse_args()
    sizes = sorted(set(args.sizes_mib))
    if args.runs < 1 or not sizes or min(sizes) < 1:
        parser.error('Input sizes and runs must be positive')
    jobs = list(schedule(sizes, args.runs))
    print('Resources:', json.dumps(PROFILE), flush=True)
    print(f'Input sizes: {sizes} MiB; repetitions: {args.runs}; total jobs: {len(jobs)}', flush=True)
    if args.plan:
        for size, repetition, engine in jobs:
            print(f'{size} MiB, repetition {repetition}: {engine}')
        return
    source = args.input.resolve()
    if not source.is_file() or source.stat().st_size < max(sizes) * 1024 * 1024:
        parser.error('The input file is missing or smaller than the largest requested size')
    local = None
    try:
        cluster.runtime()
        cluster.require_install()
        yarn = cluster.HOME / 'bin/yarn'
        nodes = cluster.run([yarn, 'node', '-list', '-states', 'RUNNING'],
                            capture_output=True, text=True, timeout=30)
        if not re.search(r'Total Nodes:\s*1\b', nodes.stdout):
            raise ValueError('Exactly one RUNNING NodeManager is required; run cluster.py status')
        apps = cluster.run([yarn, 'application', '-list', '-appStates', 'RUNNING,ACCEPTED,SUBMITTED'],
                           capture_output=True, text=True, timeout=30)
        if not re.search(r'Total number of applications[^\n]*:\s*0\s*$', apps.stdout, re.MULTILINE):
            raise ValueError('YARN already has active applications; wait for them to finish')
        identifier = 'wordcount-' + datetime.now().strftime('%Y%m%d-%H%M%S-') + uuid.uuid4().hex[:6]
        local = args.output.resolve() if args.output else ROOT / 'results' / identifier
        local.mkdir(parents=True, exist_ok=False)
        base = f'hdfs://localhost:9000/experiment/{identifier}'
        hdfs = cluster.HOME / 'bin/hdfs'
        import getpass
        cluster.run([hdfs, 'dfs', '-mkdir', '-p', f'/user/{getpass.getuser()}', base])
        manifest = {'started_utc': datetime.now(timezone.utc).isoformat(), 'source': str(source),
                    'source_bytes': source.stat().st_size, 'sizes_mib': sizes, 'runs': args.runs,
                    'mode': 'yarn-client', 'resource_profile': PROFILE,
                    'python': sys.version, 'platform': platform.platform(),
                    'cpu': Path('/proc/cpuinfo').read_text().split('model name')[1].split('\n')[0].strip(': \t'),
                    'memory': Path('/proc/meminfo').read_text().splitlines()[0],
                    'hdfs_base': base, 'samples': {},
                    'timing': 'process launch to exit, including startup, read, shuffle, output',
                    'cache_policy': 'no explicit warm-up; no OS cache reset; each job is a new process',
                    'code_sha256': {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                                    for folder in ('common', 'hadoop', 'spark', 'scripts')
                                    for p in (ROOT / folder).glob('*.py')}}
        for label, invocation in {
            'hadoop': [cluster.HOME / 'bin/hadoop', 'version'],
            'spark': [ROOT / '.venv/bin/python3', '-c', 'import pyspark; print(pyspark.__version__)'],
            'java': [Path(cluster.environment()['JAVA_HOME']) / 'bin/java', '-version']
        }.items():
            completed = cluster.run(invocation, capture_output=True, text=True, timeout=30)
            manifest[label + '_version'] = completed.stdout + completed.stderr
        for config in cluster.CONF.glob('*.xml'):
            (local / config.name).write_bytes(config.read_bytes())
        (local / 'wordcount.json').write_text(json.dumps(PROFILE, indent=2))
        for size in sizes:
            sample = local / f'input-{size}MiB.txt'
            manifest['samples'][str(size)] = make_sample(source, sample, size)
            with sample.open('rb') as stream:
                cluster.run([hdfs, 'dfs', '-put', '-', base + f'/input-{size}MiB.txt'], stdin=stream)
        (local / 'manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2))
        rows = []
        references = {}
        for number, (size, repetition, engine) in enumerate(jobs, 1):
            name = f'{size}MiB-run{repetition}-{engine}'
            print(f'[{number}/{len(jobs)}] {name}; log: {local / (name + ".log")}', flush=True)
            with (local / f'{name}.log').open('w') as log:
                cluster.run([sys.executable, ROOT / 'scripts/run_wordcount.py', engine,
                             '--mode', 'yarn', '--input', base + f'/input-{size}MiB.txt',
                             '--output', base + '/' + name, '--metrics', local / f'{name}.json'],
                            stdout=log, stderr=log)
            # Download and validate the output after the timed interval.
            cluster.run([hdfs, 'dfs', '-get', base + '/' + name, name], cwd=local)
            counts = read_counts(local / name)
            if size in references and references[size] != counts:
                raise ValueError(f'Output mismatch: {name}; stopping the experiment')
            if size not in references:
                references[size] = counts
            metrics = json.loads((local / f'{name}.json').read_text())
            if metrics['exit_code'] != 0:
                raise ValueError(f'Failed run: {name}')
            rows.append({'requested_mib': size,
                         'actual_bytes': manifest['samples'][str(size)]['actual_bytes'],
                         'run': repetition, 'engine': engine,
                         'elapsed_seconds': metrics['elapsed_seconds'],
                         'unique_words': len(counts), 'total_words': sum(counts.values()),
                         'verified': True})
            write_csv(local / 'measurements.csv', rows)
        write_csv(local / 'summary.csv', summarize(rows))
        (local / '_SUCCESS').touch()
        print(f'Experiment complete. Tables and logs: {local}', flush=True)
    except (ValueError, OSError, cluster.subprocess.SubprocessError) as error:
        parser.exit(1, f'Error: {error}\nResults and logs: {local or ROOT / "results"}\n')


if __name__ == '__main__':
    main()
