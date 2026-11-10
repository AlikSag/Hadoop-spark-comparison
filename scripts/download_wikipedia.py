"""Stream complete Wikipedia articles into JSONL without changing WordCount inputs."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
DATASET = 'wikimedia/wikipedia'
CONFIG = '20231101.en'


def write_articles(articles, stream, max_articles=None, target_bytes=None, progress=None):
    """Write complete JSON records; newlines inside article text are escaped."""
    count = 0
    size = 0
    digest = hashlib.sha256()
    for article in articles:
        record = {key: article[key] for key in ('id', 'title', 'text', 'url')}
        if not all(isinstance(value, str) for value in record.values()):
            raise ValueError('Article id, title, text, and url must be strings')
        encoded = (json.dumps(record, ensure_ascii=False) + '\n').encode('utf-8')
        stream.write(encoded)
        digest.update(encoded)
        size += len(encoded)
        count += 1
        if progress and (count == 1 or count % 100 == 0):
            progress(count, size)
        if max_articles is not None and count >= max_articles:
            reason = 'article_limit'
            break
        if target_bytes is not None and size >= target_bytes:
            reason = 'size_limit'
            break
    else:
        reason = 'source_exhausted'
    if not count:
        raise ValueError('The source returned no articles')
    return {'articles': count, 'actual_bytes': size, 'sha256': digest.hexdigest(),
            'stop_reason': reason}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    limits = parser.add_mutually_exclusive_group()
    limits.add_argument('--articles', type=int, help='Number of articles; default: 1000')
    limits.add_argument('--size-mib', type=int, help='Approximate JSONL size; keep the final article intact')
    parser.add_argument('--output', type=Path, default=ROOT / 'data/wikipedia_articles.jsonl')
    parser.add_argument('--revision', default='main', help='Dataset branch or commit; resolved to a commit before loading')
    args = parser.parse_args()
    if args.articles is not None and args.articles < 1:
        parser.error('articles must be positive')
    if args.size_mib is not None and args.size_mib < 1:
        parser.error('size-mib must be positive')
    article_limit = args.articles if args.articles is not None else (None if args.size_mib else 1000)
    target_bytes = args.size_mib * 1024 * 1024 if args.size_mib else None
    output = args.output.resolve()
    partial = output.with_name(output.name + '.part')
    metadata = output.with_name(output.name + '.metadata.json')
    for path in (output, partial, metadata):
        if path.exists():
            parser.error(f'Path already exists: {path}. Choose a new --output path.')
    output.parent.mkdir(parents=True, exist_ok=True)
    # Keep all Hugging Face caches inside the project's ignored runtime directory.
    os.environ.setdefault('HF_HOME', str(ROOT / '.runtime/huggingface'))
    try:
        from datasets import load_dataset
        from huggingface_hub import HfApi
        print(f'Source: {DATASET}, subset: {CONFIG}; resolving revision {args.revision}', flush=True)
        revision = HfApi().dataset_info(DATASET, revision=args.revision).sha
        articles = load_dataset(DATASET, CONFIG, split='train', streaming=True, revision=revision)
        print(f'Commit: {revision}; downloading complete articles...', flush=True)
        with partial.open('xb') as stream:
            stats = write_articles(
                articles, stream, max_articles=article_limit, target_bytes=target_bytes,
                progress=lambda count, size: print(f'{count} articles; {size / 1024 / 1024:.2f} MiB saved', flush=True))
        details = {'dataset': DATASET, 'config': CONFIG, 'split': 'train',
                   'requested_revision': args.revision, 'resolved_revision': revision,
                   'created_utc': datetime.now(timezone.utc).isoformat(),
                   'format': 'jsonl', 'encoding': 'utf-8',
                   'selection': 'prefix of the dataset in source order; not a random sample',
                   'requested_articles': article_limit, 'requested_mib': args.size_mib,
                   **stats}
        # Publish a completed file without replacing an existing output.
        os.link(partial, output)
        partial.unlink()
        with metadata.open('x', encoding='utf-8') as stream:
            json.dump(details, stream, ensure_ascii=False, indent=2)
    except KeyboardInterrupt:
        parser.exit(130, f'Interrupted. Any incomplete output is at {partial}; it is not a finished dataset.\n')
    except Exception as error:
        parser.exit(1, f'Download failed: {error}\nInstall dependencies from requirements-download.txt if needed.\nAny incomplete output is at {partial}; it is not a finished dataset.\n')
    print(f"Saved {stats['articles']} articles ({stats['actual_bytes'] / 1024 / 1024:.2f} MiB): {output}")
    print(f'Metadata: {metadata}')


if __name__ == '__main__':
    main()
