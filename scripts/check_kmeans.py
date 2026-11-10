"""Compare all iteration centers and counts from completed Hadoop and Spark runs."""
import argparse
import json
import math
from pathlib import Path


def compare_runs(left, right, rel_tol=1e-9, abs_tol=1e-10):
    folders = [Path(left), Path(right)]
    for folder in folders:
        if not (folder / '_SUCCESS').is_file():
            raise ValueError(f'Run is incomplete: {folder}')
    manifests = [json.loads((folder / 'manifest.json').read_text()) for folder in folders]
    if {manifest['engine'] for manifest in manifests} != {'hadoop', 'spark'}:
        raise ValueError('Compare one Hadoop run and one Spark run')
    for field in ('iterations', 'documents', 'dimensions', 'clusters',
                  'input_sha256', 'initial_centers_sha256', 'resource_profile'):
        if manifests[0][field] != manifests[1][field]:
            raise ValueError(f'Experiment conditions differ: {field}')
    largest_difference = 0.0
    for iteration in range(1, manifests[0]['iterations'] + 1):
        states = [json.loads((folder / f'iteration-{iteration:03d}-centers.json').read_text())
                  for folder in folders]
        if states[0]['counts'] != states[1]['counts']:
            raise ValueError(f'Cluster counts differ at iteration {iteration}')
        for state in states:
            if (len(state['centers']) != manifests[0]['clusters'] or
                    any(len(center) != manifests[0]['dimensions'] for center in state['centers'])):
                raise ValueError('Center shape differs from manifest')
        for a, b in zip(states[0]['centers'], states[1]['centers']):
            for x, y in zip(a, b):
                if not math.isfinite(x) or not math.isfinite(y) or not math.isclose(x, y, rel_tol=rel_tol, abs_tol=abs_tol):
                    raise ValueError(f'Center coordinates differ at iteration {iteration}')
                largest_difference = max(largest_difference, abs(x - y))
    return {'iterations': manifests[0]['iterations'], 'documents': manifests[0]['documents'],
            'maximum_absolute_center_difference': largest_difference,
            'relative_tolerance': rel_tol, 'absolute_tolerance': abs_tol}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('hadoop_output', type=Path)
    parser.add_argument('spark_output', type=Path)
    args = parser.parse_args()
    try:
        result = compare_runs(args.hadoop_output, args.spark_output)
    except (OSError, ValueError, TypeError, KeyError) as error:
        parser.exit(1, f'K-Means comparison failed: {error}\n')
    print('All iteration centers and counts match within tolerance:')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
