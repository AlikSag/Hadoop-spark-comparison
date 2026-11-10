"""Hadoop Streaming K-Means update: cluster TAB vector -> cluster TAB center."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path.cwd()))
sys.path.append(str(Path(__file__).resolve().parents[1] / 'common'))
from kmeans import load_centers, update_centers, validate_vector


def reduce_records(lines, centers, output):
    """Keep one running sum; Hadoop supplies consecutive records for each key."""
    previous_key = None
    previous_cluster = None
    total = None
    count = 0

    def emit():
        center = update_centers([centers[previous_cluster]], [total], [count])[0]
        payload = {'count': count, 'center': center}
        output.write(f'{previous_cluster}\t{json.dumps(payload, allow_nan=False)}\n')

    for line_number, line in enumerate(lines, 1):
        try:
            key, payload = line.rstrip('\n').split('\t', 1)
            cluster = int(key)
            if key != str(cluster) or not 0 <= cluster < len(centers):
                raise ValueError('Cluster key must be a canonical in-range integer')
            # Streaming Text keys sort lexically: "10" precedes "2".
            if previous_key is not None and key < previous_key:
                raise ValueError('Input keys are not sorted; Hadoop must perform shuffle/sort')
            vector = validate_vector(json.loads(payload), len(centers[cluster]))
            if previous_key is not None and key != previous_key:
                emit()
                total = None
                count = 0
            if total is None:
                total = [0.0] * len(centers[cluster])
            previous_key, previous_cluster = key, cluster
            for index, value in zip(vector['indices'], vector['values']):
                total[index] += value
            count += 1
        except (ValueError, TypeError) as error:
            raise ValueError(f'Input line {line_number}: {error}') from error
    if previous_key is not None:
        emit()
    # Empty clusters have no input and no output here. The iteration driver
    # restores their previous centers after merging all reducer outputs.


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--centers', type=Path, default=Path('centers.json'),
                        help='The same previous-center file used by the mapper')
    args = parser.parse_args()
    try:
        reduce_records(sys.stdin, load_centers(args.centers), sys.stdout)
    except (OSError, ValueError, TypeError) as error:
        parser.exit(1, f'K-Means reducer failed: {error}\n')


if __name__ == '__main__':
    main()
