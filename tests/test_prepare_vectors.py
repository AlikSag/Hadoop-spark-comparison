"""Check exported document identity, sparse vectors, and zero-row policy."""
import importlib.util
import json
import math
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from prepare_vectors import prepare, read_documents


class InputTest(unittest.TestCase):
    def test_duplicate_ids_are_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'input.jsonl'
            doc = {'id': '1', 'title': 'Example', 'text': 'cat', 'url': 'example'}
            path.write_text((json.dumps(doc) + '\n') * 2)
            with self.assertRaises(ValueError):
                read_documents(path)


@unittest.skipUnless(importlib.util.find_spec('sklearn'), 'Install requirements-prepare.txt')
class ExportTest(unittest.TestCase):
    def test_shared_sparse_export_and_zero_rows(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source = root / 'articles.jsonl'
            docs = [{'id': str(i), 'title': f'Article {i}', 'text': text, 'url': 'example'}
                    for i, text in enumerate(['cat cat dog', 'cat fish', 'the and'])]
            source.write_text(''.join(json.dumps(doc) + '\n' for doc in docs))
            settings = {'max_features': 100, 'min_df': 1, 'max_df': 1.0,
                        'stop_words': 'english', 'sublinear_tf': True,
                        'smooth_idf': True, 'norm': 'l2'}
            output = root / 'vectors'
            metadata = prepare(source, output, settings)
            vectors = [json.loads(line) for line in (output / 'vectors.jsonl').read_text().splitlines()]
            descriptions = [json.loads(line) for line in (output / 'documents.jsonl').read_text().splitlines()]
            self.assertEqual([v['id'] for v in vectors], ['0', '1'])
            self.assertEqual([d['id'] for d in descriptions], ['0', '1'])
            self.assertEqual(metadata['excluded_documents'], 1)
            self.assertTrue((output / '_SUCCESS').is_file())
            terms = json.loads((output / 'vocabulary.json').read_text())['terms']
            for vector in vectors:
                self.assertEqual(vector['size'], len(terms))
                self.assertEqual(len(vector['indices']), len(vector['values']))
                self.assertAlmostEqual(sum(value ** 2 for value in vector['values']), 1)
                self.assertTrue(all(math.isfinite(value) and value > 0 for value in vector['values']))
            first = {terms[i]: value for i, value in zip(vectors[0]['indices'], vectors[0]['values'])}
            self.assertEqual(set(first), {'cat', 'dog'})
            self.assertGreater(first['cat'], first['dog'])
