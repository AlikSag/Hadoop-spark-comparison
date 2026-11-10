"""Check JSONL document boundaries and limits without network access."""
import hashlib
import io
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from download_wikipedia import write_articles


class DownloadTest(unittest.TestCase):
    def articles(self):
        return [{'id': str(index), 'title': 'Example',
                 'text': 'First paragraph\n\nSecond paragraph: caf\u00e9 "quoted"',
                 'url': f'https://en.wikipedia.org/?curid={index}'} for index in range(3)]

    def test_one_physical_line_per_article_and_preserved_text(self):
        stream = io.BytesIO()
        stats = write_articles(iter(self.articles()), stream, max_articles=2)
        raw = stream.getvalue()
        records = [json.loads(line) for line in raw.splitlines()]
        self.assertEqual(records, self.articles()[:2])
        self.assertEqual(stats['articles'], 2)
        self.assertEqual(stats['actual_bytes'], len(raw))
        self.assertEqual(stats['sha256'], hashlib.sha256(raw).hexdigest())
        self.assertEqual(stats['stop_reason'], 'article_limit')

    def test_size_limit_keeps_complete_final_article(self):
        stream = io.BytesIO()
        stats = write_articles(self.articles(), stream, target_bytes=1)
        self.assertEqual(stats['articles'], 1)
        self.assertEqual(json.loads(stream.getvalue()), self.articles()[0])
        self.assertEqual(stats['stop_reason'], 'size_limit')

    def test_empty_source_is_not_success(self):
        with self.assertRaises(ValueError):
            write_articles([], io.BytesIO(), max_articles=2)
