"""Prepare one shared TF-IDF dataset for Hadoop and Spark K-Means."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'common'))
from tokenization import tokenize


def read_documents(path):
    documents = []
    seen = set()
    with path.open(encoding='utf-8') as stream:
        for line_number, line in enumerate(stream, 1):
            record = json.loads(line)
            for field in ('id', 'title', 'text', 'url'):
                if not isinstance(record.get(field), str):
                    raise ValueError(f'Line {line_number}: {field} must be a string')
            if not record['id'] or record['id'] in seen:
                raise ValueError(f'Line {line_number}: missing or duplicate article ID')
            seen.add(record['id'])
            documents.append(record)
    if not documents:
        raise ValueError('The input contains no articles')
    return documents


def fit_vectors(documents, settings):
    from sklearn.feature_extraction.text import TfidfVectorizer
    vectorizer = TfidfVectorizer(tokenizer=tokenize, token_pattern=None,
                                 lowercase=False, **settings)
    matrix = vectorizer.fit_transform(document['text'] for document in documents).tocsr()
    matrix.sort_indices()
    retained = [i for i in range(matrix.shape[0]) if matrix.indptr[i] < matrix.indptr[i + 1]]
    if not retained:
        raise ValueError('All document vectors are zero after feature filtering')
    return vectorizer, matrix, retained


def file_hash(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def prepare(source, output, settings):
    import sklearn
    started = time.perf_counter()
    documents = read_documents(source)
    vectorizer, matrix, retained = fit_vectors(documents, settings)
    output.mkdir(parents=True, exist_ok=False)
    vector_file = output / 'vectors.jsonl'
    document_file = output / 'documents.jsonl'
    nonzero = 0
    with vector_file.open('x', encoding='utf-8') as vectors, document_file.open('x', encoding='utf-8') as descriptions:
        for index in retained:
            row = matrix.getrow(index)
            nonzero += row.nnz
            record = {'id': documents[index]['id'], 'size': matrix.shape[1],
                      'indices': row.indices.tolist(), 'values': row.data.tolist()}
            vectors.write(json.dumps(record, ensure_ascii=False, allow_nan=False) + '\n')
            descriptions.write(json.dumps({key: documents[index][key] for key in ('id', 'title', 'url')},
                                          ensure_ascii=False) + '\n')
    vocabulary = {'terms': vectorizer.get_feature_names_out().tolist(),
                  'idf': vectorizer.idf_.tolist(),
                  'english_stop_words': sorted(vectorizer.get_stop_words()) if vectorizer.get_stop_words() else []}
    (output / 'vocabulary.json').write_text(json.dumps(vocabulary, ensure_ascii=False, indent=2), encoding='utf-8')
    kept = set(retained)
    excluded = [{'id': doc['id'], 'reason': 'zero_vector_after_filtering'}
                for i, doc in enumerate(documents) if i not in kept]
    (output / 'excluded.json').write_text(json.dumps(excluded, indent=2))
    source_metadata = source.with_name(source.name + '.metadata.json')
    metadata = {'created_utc': datetime.now(timezone.utc).isoformat(),
                'input': str(source), 'input_sha256': file_hash(source),
                'input_documents': len(documents), 'retained_documents': len(retained),
                'excluded_documents': len(excluded), 'dimensions': matrix.shape[1],
                'nonzero_values': nonzero, 'settings': settings,
                'tokenizer': 'common/tokenization.py:tokenize',
                'tokenizer_sha256': file_hash(ROOT / 'common/tokenization.py'),
                'script_sha256': file_hash(Path(__file__)),
                'sklearn_version': sklearn.__version__, 'python_version': sys.version,
                'tf': '1 + ln(count) when sublinear_tf is true, otherwise raw count',
                'idf': 'ln((1 + document_count) / (1 + document_frequency)) + 1 with smooth_idf',
                'normalization': 'L2: divide each nonzero row by its Euclidean norm',
                'zero_vector_policy': 'excluded from both engines; see excluded.json',
                'output_sha256': {name: file_hash(output / name) for name in
                                  ('vectors.jsonl', 'documents.jsonl', 'vocabulary.json', 'excluded.json')},
                'source_metadata': json.loads(source_metadata.read_text()) if source_metadata.is_file() else None,
                'preparation_seconds': time.perf_counter() - started,
                'timing_note': 'local preparation only; excluded from future engine benchmark times'}
    (output / 'metadata.json').write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding='utf-8')
    (output / '_SUCCESS').touch()
    return metadata


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, default=ROOT / 'data/wikipedia_articles.jsonl')
    parser.add_argument('--output', type=Path, default=ROOT / 'data/kmeans')
    parser.add_argument('--config', type=Path, default=ROOT / 'config/tfidf.json')
    args = parser.parse_args()
    if args.output.exists():
        parser.error('Output already exists; choose a new --output directory')
    try:
        metadata = prepare(args.input.resolve(), args.output.resolve(), json.loads(args.config.read_text()))
    except (OSError, ValueError, ImportError, TypeError) as error:
        parser.exit(1, f'Preparation failed: {error}\nInstall requirements-prepare.txt if needed.\n'
                    'An output directory without _SUCCESS is incomplete.\n')
    print(f"Input articles: {metadata['input_documents']}; retained: {metadata['retained_documents']}; "
          f"excluded zero vectors: {metadata['excluded_documents']}")
    print(f"Dimensions: {metadata['dimensions']}; nonzero values: {metadata['nonzero_values']}")
    print(f"Local preparation time: {metadata['preparation_seconds']:.3f} s")
    print(f'Output: {args.output.resolve()}')


if __name__ == '__main__':
    main()
