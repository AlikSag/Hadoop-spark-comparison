"""Run fixed Lloyd K-Means iterations as real Hadoop Streaming jobs on YARN."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import uuid

import cluster
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'common'))
from kmeans import load_centers, merge_center_updates, nearest_center, validate_vector


def sha256(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def inspect_input(path, dimension):
    ids = set()
    with path.open(encoding='utf-8') as stream:
        for line in stream:
            vector = validate_vector(json.loads(line), dimension)
            if vector['id'] in ids:
                raise ValueError('Input contains duplicate document IDs')
            ids.add(vector['id'])
    if not ids:
        raise ValueError('Input contains no vectors')
    return len(ids)


def build_command(home, staging, source, target, profile):
    jars = list((home / 'share/hadoop/tools/lib').glob('hadoop-streaming-*.jar'))
    if len(jars) != 1:
        raise ValueError('Expected exactly one Hadoop Streaming JAR')
    options = {
        'mapreduce.framework.name': 'yarn',
        'mapreduce.job.reduces': profile['partitions'],
        'mapreduce.job.running.map.limit': profile['workers'],
        'mapreduce.job.running.reduce.limit': profile['workers'],
        'mapreduce.job.reduce.slowstart.completedmaps': 1.0,
        'mapreduce.map.memory.mb': profile['task_memory_mb'],
        'mapreduce.reduce.memory.mb': profile['task_memory_mb'],
        'mapreduce.map.java.opts': f"-Xmx{profile['task_heap_mb']}m",
        'mapreduce.reduce.java.opts': f"-Xmx{profile['task_heap_mb']}m",
        'mapreduce.map.cpu.vcores': 1, 'mapreduce.reduce.cpu.vcores': 1,
        'yarn.app.mapreduce.am.resource.mb': profile['application_master_memory_mb'],
        'yarn.app.mapreduce.am.command-opts': '-Xmx768m',
        'mapreduce.map.speculative': 'false', 'mapreduce.reduce.speculative': 'false',
        'mapreduce.input.fileinputformat.split.maxsize': profile['split_bytes'],
        'mapreduce.job.name': 'ComparisonKMeansIteration',
    }
    command = [str(home / 'bin/hadoop'), 'jar', str(jars[0])]
    for key, value in options.items():
        command += ['-D', f'{key}={value}']
    files = ['kmeans_mapper.py', 'kmeans_reducer.py', 'kmeans.py', 'centers.json']
    return command + ['-files', ','.join((staging / name).as_uri() for name in files),
                      '-cmdenv', 'PYTHONIOENCODING=utf-8',
                      '-mapper', 'python3 kmeans_mapper.py --centers centers.json',
                      '-reducer', 'python3 kmeans_reducer.py --centers centers.json',
                      '-input', source, '-output', target]


def read_updates(directory, previous, expected_documents):
    if not (directory / '_SUCCESS').is_file():
        raise ValueError('Hadoop output is missing _SUCCESS')
    parts = sorted(directory.glob('part-*'))
    if not parts:
        raise ValueError('Hadoop output has no part-* files')
    records = []
    for part in parts:
        with part.open(encoding='utf-8') as stream:
            for line in stream:
                key, value = line.rstrip('\n').split('\t', 1)
                cluster_id = int(key)
                if key != str(cluster_id):
                    raise ValueError('Reducer output has a noncanonical cluster key')
                payload = json.loads(value)
                records.append({'cluster': cluster_id, 'count': payload['count'],
                                'center': payload['center']})
    centers, counts = merge_center_updates(previous, records)
    if sum(counts) != expected_documents:
        raise ValueError(f'Document count mismatch: {sum(counts)} != {expected_documents}')
    return centers, counts


def local_objective(path, centers):
    norms = [math.fsum(value * value for value in center) for center in centers]
    def distances():
        with path.open(encoding='utf-8') as stream:
            for line in stream:
                yield nearest_center(json.loads(line), centers, norms)[1]
    return math.fsum(distances())


def execute(args):
    profile = json.loads(args.config.read_text())
    for name in ('workers', 'partitions', 'task_memory_mb', 'task_heap_mb',
                 'application_master_memory_mb', 'split_bytes'):
        if type(profile.get(name)) is not int or profile[name] < 1:
            raise ValueError(f'Invalid resource profile field: {name}')
    source = args.input.resolve()
    initial_file = args.centers.resolve()
    centers = load_centers(initial_file)
    initial = json.loads(initial_file.read_text())
    input_hash = sha256(source)
    if initial.get('input_sha256') != input_hash:
        raise ValueError('Initial centers were prepared for a different input file')
    documents = inspect_input(source, len(centers[0]))
    cluster.runtime()
    cluster.require_install()
    env = cluster.environment()
    hdfs = cluster.HOME / 'bin/hdfs'
    identifier = 'kmeans-hadoop-' + datetime.now().strftime('%Y%m%d-%H%M%S-') + uuid.uuid4().hex[:6]
    output = (args.output or ROOT / 'results' / identifier).resolve()
    output.mkdir(parents=True, exist_ok=False)
    print(f'Results: {output}', flush=True)
    base = f'hdfs://localhost:9000/experiment/{identifier}'
    manifest = {'engine': 'hadoop', 'mode': 'yarn', 'iterations': args.iterations,
                'documents': documents, 'dimensions': len(centers[0]), 'clusters': len(centers),
                'input': str(source), 'input_sha256': input_hash,
                'initial_centers_sha256': sha256(initial_file), 'resource_profile': profile,
                'hdfs_base': base, 'created_utc': datetime.now(timezone.utc).isoformat(),
                'python_version': sys.version, 'early_stopping': False,
                'timing': 'total_algorithm_seconds includes jobs, center downloads, merging, and snapshots; '
                          'excludes input validation/upload, code staging, and final local objective check',
                'source_sha256': {name: sha256(ROOT / name) for name in
                                  ('hadoop/kmeans_mapper.py', 'hadoop/kmeans_reducer.py',
                                   'common/kmeans.py', 'scripts/run_hadoop_kmeans.py')}}
    (output / 'manifest.json').write_text(json.dumps(manifest, indent=2))
    shutil.copyfile(initial_file, output / 'initial_centers.json')
    for config in cluster.CONF.glob('*.xml'):
        shutil.copyfile(config, output / config.name)
    cluster.run([hdfs, 'dfs', '-mkdir', '-p', base], timeout=30)
    with source.open('rb') as stream:
        cluster.run([hdfs, 'dfs', '-put', '-', base + '/vectors.jsonl'], stdin=stream, timeout=120)
    rows = []
    with tempfile.TemporaryDirectory(prefix='hscompare-kmeans-', dir='/tmp') as temporary:
        staging = Path(temporary)
        for source_name, target_name in [('hadoop/kmeans_mapper.py', 'kmeans_mapper.py'),
                                          ('hadoop/kmeans_reducer.py', 'kmeans_reducer.py'),
                                          ('common/kmeans.py', 'kmeans.py')]:
            shutil.copyfile(ROOT / source_name, staging / target_name)
        started = time.perf_counter()
        for iteration in range(1, args.iterations + 1):
            step_started = time.perf_counter()
            state = {'clusters': len(centers), 'dimensions': len(centers[0]), 'centers': centers}
            (staging / 'centers.json').write_text(json.dumps(state, allow_nan=False))
            name = f'iteration-{iteration:03d}'
            command = build_command(cluster.HOME, staging, base + '/vectors.jsonl', base + '/' + name, profile)
            print(f'Iteration {iteration}/{args.iterations}; log: {output / (name + ".log")}', flush=True)
            job_started = time.perf_counter()
            with (output / f'{name}.log').open('w') as log:
                log.write(json.dumps(command) + '\n')
                log.flush()
                completed = subprocess.run(command, cwd=staging, env=env, stdout=log, stderr=log)
            job_seconds = time.perf_counter() - job_started
            job_record = {'iteration': iteration, 'command': command,
                          'job_seconds': job_seconds, 'exit_code': completed.returncode}
            (output / f'{name}-job.json').write_text(json.dumps(job_record, indent=2))
            if completed.returncode:
                raise ValueError(f'Hadoop iteration {iteration} failed; see {output / (name + ".log")}')
            cluster.run([hdfs, 'dfs', '-get', base + '/' + name, name], cwd=output, timeout=60)
            centers, counts = read_updates(output / name, centers, documents)
            state.update(centers=centers, counts=counts, iteration=iteration,
                         counts_note='assignments to the previous centers used in this iteration')
            (output / f'{name}-centers.json').write_text(json.dumps(state, allow_nan=False, indent=2))
            row = {**job_record, 'iteration_seconds': time.perf_counter() - step_started,
                   'cumulative_seconds': time.perf_counter() - started, 'counts': counts}
            rows.append(row)
            print(f'Counts: {counts}; job: {job_seconds:.3f} s; cumulative: {row["cumulative_seconds"]:.3f} s', flush=True)
        total = time.perf_counter() - started
    # These local scans check correctness, not engine performance.
    initial_objective = local_objective(source, initial['centers'])
    final_objective = local_objective(source, centers)
    if final_objective > initial_objective + 1e-8 * max(1.0, initial_objective):
        raise ValueError('K-Means objective increased; inspect saved centers')
    result = {'engine': 'hadoop', 'iterations': args.iterations,
              'documents': documents, 'clusters': len(centers), 'dimensions': len(centers[0]),
              'total_algorithm_seconds': total, 'job_seconds_sum': sum(row['job_seconds'] for row in rows),
              'initial_objective': initial_objective, 'final_objective': final_objective,
              'objective_check': 'local post-run scan, excluded from engine timing', 'steps': rows}
    (output / 'final_centers.json').write_text(json.dumps(state, allow_nan=False, indent=2))
    (output / 'metrics.json').write_text(json.dumps(result, allow_nan=False, indent=2))
    (output / '_SUCCESS').touch()
    print(f'Completed {args.iterations} Hadoop iterations in {total:.3f} s')
    print(f'Objective: {initial_objective:.6f} -> {final_objective:.6f}')
    print('Results:', output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, default=ROOT / 'data/kmeans/vectors.jsonl')
    parser.add_argument('--centers', type=Path, default=ROOT / 'data/kmeans/initial_centers.json')
    parser.add_argument('--config', type=Path, default=ROOT / 'config/kmeans.json')
    parser.add_argument('--iterations', type=int, default=1)
    parser.add_argument('--output', type=Path, help='New local result directory; default is unique')
    args = parser.parse_args()
    if args.iterations < 1:
        parser.error('iterations must be positive')
    try:
        execute(args)
    except (OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError) as error:
        parser.exit(1, f'K-Means run failed: {error}\nNo _SUCCESS means the run is incomplete.\n')


if __name__ == '__main__':
    main()
