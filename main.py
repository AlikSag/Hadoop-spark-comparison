"""Run both experimental series and generate reports from their new results."""
import argparse
from datetime import datetime
import importlib.util
import json
from pathlib import Path
import shlex
import socket
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parent


def commands(output):
    """Build the reproducible schedule without running or changing anything."""
    def script(name, *args):
        return [sys.executable, str(ROOT / 'scripts' / name), *map(str, args)]

    yield script('cluster.py', 'smoke')
    yield script('benchmark.py', '--output', output / 'wordcount')
    yield script('analyze_wordcount.py', output / 'wordcount',
                 '--output', output / 'reports')
    for repetition in (1, 2, 3):
        for iterations in (1, 3, 5, 10):
            engines = ('spark', 'hadoop') if repetition == 2 else ('hadoop', 'spark')
            for engine in engines:
                target = output / 'kmeans' / f'{iterations}iter-run{repetition}-{engine}'
                yield script(f'run_{engine}_kmeans.py', '--iterations', iterations,
                             '--output', target)
    yield script('analyze_kmeans.py', output / 'kmeans',
                 '--output', output / 'reports')


def run(command):
    print('\n> ' + shlex.join(command), flush=True)
    subprocess.run(command, cwd=ROOT, check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan', action='store_true', help='Print commands without executing')
    parser.add_argument('--output', type=Path, help='New directory for all results and reports')
    args = parser.parse_args()
    identifier = datetime.now().strftime('%Y%m%d-%H%M%S-') + uuid.uuid4().hex[:6]
    output = (args.output or ROOT / 'results' / f'full-{identifier}').resolve()
    if args.plan:
        print('Infrastructure: reuse a healthy cluster; otherwise setup/start it.')
        for command in commands(output):
            print(shlex.join(command))
        return
    try:
        if output.exists():
            raise ValueError(f'Output already exists: {output}')
        for name in ('pyspark', 'matplotlib'):
            if importlib.util.find_spec(name) is None:
                raise ValueError(f'Missing {name}; activate .venv and install requirements.txt '
                                 'and requirements-report.txt')
        for relative in ('data/wikipedia.txt', 'data/kmeans/vectors.jsonl',
                         'data/kmeans/initial_centers.json'):
            if not (ROOT / relative).is_file():
                raise ValueError(f'Missing prepared input: {relative}; see README.md')
        if (ROOT / 'data/wikipedia.txt').stat().st_size < 500 * 1024 * 1024:
            raise ValueError('WordCount requires at least 500 MiB of source text')
        output.mkdir(parents=True)
        print(f'All new results: {output}', flush=True)
        # Import only after preflight; reuse the existing infrastructure manager.
        sys.path.insert(0, str(ROOT / 'scripts'))
        import cluster
        cluster.runtime()
        if not (cluster.HOME / 'bin/hdfs').is_file():
            run([sys.executable, str(ROOT / 'scripts/cluster.py'), 'setup'])
        else:
            try:
                with socket.create_connection(('127.0.0.1', 9000), timeout=2):
                    pass
                with socket.create_connection(('127.0.0.1', 8088), timeout=2):
                    pass
            except OSError:
                run([sys.executable, str(ROOT / 'scripts/cluster.py'), 'start'])
        for command in commands(output):
            run(command)
        (output / '_SUCCESS').touch()
        (output / 'run.json').write_text(json.dumps({
            'wordcount': str(output / 'wordcount'),
            'kmeans': str(output / 'kmeans'),
            'reports': str(output / 'reports'),
        }, indent=2) + '\n', encoding='utf-8')
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        parser.exit(1, f'Full experiment failed: {error}\nResults so far: {output}\n')
    print(f'Completed. Reports: {output / "reports"}\n'
          'The cluster remains running; stop with: python scripts/cluster.py stop')


if __name__ == '__main__':
    main()
