"""Launch one persisted PySpark K-Means application on the project's YARN cluster."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import uuid

import cluster
from run_wordcount import executable, spark_environment
from run_hadoop_kmeans import sha256, inspect_input, local_objective

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'common'))
from kmeans import load_centers


def build_command(staging, source, target, output, iterations, documents, profile):
    workers = profile['workers']
    options = {
        'spark.dynamicAllocation.enabled': 'false', 'spark.executor.instances': 1,
        'spark.executor.cores': workers,
        'spark.executor.memory': f"{profile['task_heap_mb'] * workers}m",
        'spark.executor.memoryOverhead': (profile['task_memory_mb'] - profile['task_heap_mb']) * workers,
        'spark.yarn.am.memory': f"{profile['application_master_memory_mb'] - 512}m",
        'spark.yarn.am.memoryOverhead': 512, 'spark.yarn.am.cores': 1,
        'spark.speculation': 'false',
        'spark.hadoop.mapreduce.input.fileinputformat.split.maxsize': profile['split_bytes'],
    }
    command = [executable('spark-submit'), '--master', 'yarn', '--deploy-mode', 'client',
               '--driver-memory', '1024m']
    for key, value in options.items():
        command += ['--conf', f'{key}={value}']
    return command + ['--py-files', str(staging / 'kmeans.py'), str(staging / 'kmeans_job.py'),
                      '--input', source, '--output', target,
                      '--centers', str(staging / 'centers.json'), '--local-results', str(output),
                      '--iterations', str(iterations), '--documents', str(documents),
                      '--partitions', str(profile['partitions'])]


def read_final(directory, clusters, dimension, documents, iterations):
    if not (directory / '_SUCCESS').is_file():
        raise ValueError('Spark output is missing _SUCCESS')
    records = []
    for part in sorted(directory.glob('part-*')):
        records.extend(json.loads(line) for line in part.read_text().splitlines())
    if len(records) != 1:
        raise ValueError('Expected one final center-state record')
    state = records[0]
    # Validate all coordinates via the same reader used by Hadoop.
    path = directory / 'center-state.json'
    path.write_text(json.dumps(state, allow_nan=False))
    centers = load_centers(path)
    if len(centers) != clusters or len(centers[0]) != dimension or state['iteration'] != iterations:
        raise ValueError('Final center shape or iteration count differs')
    counts = state['counts']
    if len(counts) != clusters or any(type(n) is not int or n < 0 for n in counts) or sum(counts) != documents:
        raise ValueError('Final document counts differ')
    return state


def execute(args):
    profile = json.loads(args.config.read_text())
    for key in ('workers', 'partitions', 'task_memory_mb', 'task_heap_mb',
                'application_master_memory_mb', 'split_bytes'):
        if type(profile.get(key)) is not int or profile[key] < 1:
            raise ValueError(f'Invalid resource profile field: {key}')
    if profile['task_memory_mb'] <= profile['task_heap_mb'] or profile['application_master_memory_mb'] <= 512:
        raise ValueError('Resource profile does not leave enough overhead memory')
    source, initial_file = args.input.resolve(), args.centers.resolve()
    initial_centers = load_centers(initial_file)
    initial = json.loads(initial_file.read_text())
    input_hash = sha256(source)
    if initial.get('input_sha256') != input_hash:
        raise ValueError('Initial centers were prepared for a different input file')
    documents = inspect_input(source, len(initial_centers[0]))
    cluster.runtime()
    cluster.require_install()
    identifier = 'kmeans-spark-' + datetime.now().strftime('%Y%m%d-%H%M%S-') + uuid.uuid4().hex[:6]
    output = (args.output or ROOT / 'results' / identifier).resolve()
    output.mkdir(parents=True, exist_ok=False)
    print('Results:', output, flush=True)
    base = f'hdfs://localhost:9000/experiment/{identifier}'
    manifest = {'engine': 'spark', 'mode': 'yarn-client', 'iterations': args.iterations,
                'documents': documents, 'clusters': len(initial_centers), 'dimensions': len(initial_centers[0]),
                'input': str(source), 'input_sha256': input_hash,
                'initial_centers_sha256': sha256(initial_file), 'resource_profile': profile,
                'created_utc': datetime.now(timezone.utc).isoformat(), 'python_version': sys.version,
                'hdfs_base': base, 'early_stopping': False, 'persistence': 'MEMORY_AND_DISK',
                'timing': 'total_algorithm_seconds includes JVM/session startup, reading/persistence, all '
                          'iterations, HDFS output, shutdown, download and state validation; excludes '
                          'input validation/upload, code staging, and local objective checks',
                'source_sha256': {name: sha256(ROOT / name) for name in
                                  ('common/kmeans.py', 'spark/kmeans_job.py', 'scripts/run_spark_kmeans.py')}}
    (output / 'manifest.json').write_text(json.dumps(manifest, indent=2))
    shutil.copyfile(initial_file, output / 'initial_centers.json')
    for config in cluster.CONF.glob('*.xml'):
        shutil.copyfile(config, output / config.name)
    hdfs = cluster.HOME / 'bin/hdfs'
    cluster.run([hdfs, 'dfs', '-mkdir', '-p', base], timeout=30)
    with source.open('rb') as stream:
        cluster.run([hdfs, 'dfs', '-put', '-', base + '/vectors.jsonl'], stdin=stream, timeout=120)
    with tempfile.TemporaryDirectory(prefix='hscompare-spark-kmeans-', dir='/tmp') as folder:
        staging = Path(folder)
        for original, name in [('spark/kmeans_job.py', 'kmeans_job.py'),
                               ('common/kmeans.py', 'kmeans.py')]:
            shutil.copyfile(ROOT / original, staging / name)
        shutil.copyfile(initial_file, staging / 'centers.json')
        command = build_command(staging, base + '/vectors.jsonl', base + '/output', output,
                                args.iterations, documents, profile)
        env = cluster.environment()
        # Resolve Spark/Python without losing the Hadoop/YARN environment.
        env.update({key: value for key, value in spark_environment(staging).items()
                    if key in ('SPARK_HOME', 'PYSPARK_DRIVER_PYTHON')})
        started = time.perf_counter()
        with (output / 'spark.log').open('w') as log:
            log.write(json.dumps(command) + '\n')
            log.flush()
            completed = subprocess.run(command, env=env, cwd=staging, stdout=log, stderr=log)
        process_seconds = time.perf_counter() - started
        (output / 'spark-job.json').write_text(json.dumps({'command': command,
            'process_seconds': process_seconds, 'exit_code': completed.returncode}, indent=2))
        if completed.returncode:
            raise ValueError(f'Spark failed; see {output / "spark.log"}')
        cluster.run([hdfs, 'dfs', '-get', base + '/output', 'spark-output'], cwd=output, timeout=60)
        state = read_final(output / 'spark-output', len(initial_centers), len(initial_centers[0]),
                           documents, args.iterations)
        (output / 'final_centers.json').write_text(json.dumps(state, allow_nan=False, indent=2))
        total = time.perf_counter() - started
    initial_objective = local_objective(source, initial_centers)
    final_objective = local_objective(source, state['centers'])
    if final_objective > initial_objective + 1e-8 * max(1.0, initial_objective):
        raise ValueError('K-Means objective increased')
    internal = json.loads((output / 'spark-internal.json').read_text())
    metrics = {'engine': 'spark', 'iterations': args.iterations, 'documents': documents,
               'clusters': len(initial_centers), 'dimensions': len(initial_centers[0]),
               'total_algorithm_seconds': total, 'process_seconds': process_seconds,
               'initial_objective': initial_objective, 'final_objective': final_objective,
               'objective_check': 'local post-run scan, excluded from engine timing', **internal}
    (output / 'metrics.json').write_text(json.dumps(metrics, allow_nan=False, indent=2))
    (output / '_SUCCESS').touch()
    print(f'Completed {args.iterations} Spark iterations in {total:.3f} s')
    print('Counts:', state['counts'])
    print(f'Objective: {initial_objective:.6f} -> {final_objective:.6f}')
    print('Results:', output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, default=ROOT / 'data/kmeans/vectors.jsonl')
    parser.add_argument('--centers', type=Path, default=ROOT / 'data/kmeans/initial_centers.json')
    parser.add_argument('--config', type=Path, default=ROOT / 'config/kmeans.json')
    parser.add_argument('--iterations', type=int, default=1)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    if args.iterations < 1:
        parser.error('iterations must be positive')
    try:
        execute(args)
    except (OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError) as error:
        parser.exit(1, f'Spark K-Means failed: {error}\nNo _SUCCESS means the run is incomplete.\n')


if __name__ == '__main__':
    main()
