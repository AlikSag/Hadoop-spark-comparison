"""Hadoop Streaming K-Means assignment: sparse JSONL -> cluster TAB vector JSON."""
import argparse
import json
import math
from pathlib import Path
import sys

# Hadoop places links to separately localized modules in the working directory.
sys.path.insert(0, str(Path.cwd()))
sys.path.append(str(Path(__file__).resolve().parents[1] / 'common'))
from kmeans import load_centers, nearest_center, validate_vector


def assign_records(lines, centers, output):
    # Reuse center norms for every document handled by this mapper process.
    norms = [math.fsum(value * value for value in center) for center in centers]
    for line_number, line in enumerate(lines, 1):
        try:
            vector = validate_vector(json.loads(line), len(centers[0]))
            cluster, _ = nearest_center(vector, centers, norms)
        except (ValueError, TypeError) as error:
            raise ValueError(f'Input line {line_number}: {error}') from error
        payload = {key: vector[key] for key in ('id', 'size', 'indices', 'values')}
        # Only the cluster is the Hadoop key; the complete vector is its value.
        output.write(f'{cluster}\t{json.dumps(payload, ensure_ascii=False, allow_nan=False)}\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--centers', type=Path, default=Path('centers.json'),
                        help='Localized center file, or a local path for a standalone check')
    args = parser.parse_args()
    try:
        centers = load_centers(args.centers)
        assign_records(sys.stdin, centers, sys.stdout)
    except (OSError, ValueError, TypeError) as error:
        parser.exit(1, f'K-Means mapper failed: {error}\n')


if __name__ == '__main__':
    main()
