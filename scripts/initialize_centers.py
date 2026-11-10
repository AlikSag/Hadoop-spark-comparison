"""Select and save common initial centers; this does not run K-Means."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import random
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'common'))
from kmeans import dense_vector, validate_vector


def choose_centers(vectors, clusters, seed):
    if type(clusters) is not int or clusters < 1:
        raise ValueError('clusters must be positive')
    if not vectors:
        raise ValueError('No document vectors found')
    size = vectors[0]['size']
    ids = set()
    unique = {}
    # Sorting makes selection independent of JSONL record order.
    for vector in sorted(vectors, key=lambda item: item['id']):
        validate_vector(vector, size)
        if vector['id'] in ids:
            raise ValueError('Duplicate document ID')
        ids.add(vector['id'])
        signature = tuple((index, value) for index, value in
                          zip(vector['indices'], vector['values']) if value != 0)
        if signature:
            unique.setdefault(signature, vector)
    if len(unique) < clusters:
        raise ValueError('Not enough distinct nonzero vectors for the requested centers')
    selected = random.Random(seed).sample(list(unique.values()), clusters)
    return {'algorithm': 'lloyd_kmeans', 'initialization': 'uniform sample of distinct nonzero document vectors',
            'seed': seed, 'clusters': clusters, 'dimensions': size,
            'source_documents': len(vectors), 'eligible_distinct_vectors': len(unique),
            'source_document_ids': [vector['id'] for vector in selected],
            'centers': [dense_vector(vector) for vector in selected],
            'distance': 'squared_euclidean', 'tie_break': 'lowest_cluster_index',
            'empty_cluster_policy': 'keep_previous_center',
            'center_update': 'arithmetic_mean_without_renormalization'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, default=ROOT / 'data/kmeans/vectors.jsonl')
    parser.add_argument('--output', type=Path, default=ROOT / 'data/kmeans/initial_centers.json')
    parser.add_argument('--clusters', type=int, default=5)
    parser.add_argument('--seed', type=int, default=42)
    args = parser.parse_args()
    if args.output.exists():
        parser.error('Output already exists; choose a new --output path')
    try:
        with args.input.open(encoding='utf-8') as stream:
            vectors = [json.loads(line) for line in stream]
        result = choose_centers(vectors, args.clusters, args.seed)
        with args.input.open('rb') as stream:
            result['input_sha256'] = hashlib.file_digest(stream, 'sha256').hexdigest()
        result['created_utc'] = datetime.now(timezone.utc).isoformat()
        result['python_version'] = sys.version
        result['implementation_sha256'] = hashlib.sha256((ROOT / 'common/kmeans.py').read_bytes()).hexdigest()
        result['initializer_sha256'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open('x', encoding='utf-8') as stream:
            json.dump(result, stream, ensure_ascii=False, allow_nan=False, indent=2)
    except (ValueError, OSError, KeyError, TypeError) as error:
        parser.exit(1, f'Initialization failed: {error}\n')
    print(f"Saved {result['clusters']} centers with {result['dimensions']} dimensions; seed={args.seed}")
    print('Source document IDs:', ', '.join(result['source_document_ids']))
    print('Output:', args.output.resolve())


if __name__ == '__main__':
    main()
