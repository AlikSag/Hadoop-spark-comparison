"""Check Streaming aggregation, final-group output, and empty-cluster merging."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'common'))
from kmeans import merge_center_updates


class ReducerTest(unittest.TestCase):
    def execute(self, records, centers):
        with tempfile.TemporaryDirectory() as folder:
            work = Path(folder)
            path = work / 'centers.json'
            path.write_text(json.dumps({'dimensions': len(centers[0]),
                                        'clusters': len(centers), 'centers': centers}))
            # Reproduce separately localized script/module links in YARN.
            cache = work / 'cache'
            cache.mkdir()
            (cache / 'script.py').write_bytes((ROOT / 'hadoop/kmeans_reducer.py').read_bytes())
            (work / 'reducer.py').symlink_to(cache / 'script.py')
            (work / 'kmeans.py').symlink_to(ROOT / 'common/kmeans.py')
            return subprocess.run([sys.executable, 'reducer.py'], cwd=work,
                                  input=''.join(f'{key}\t{json.dumps(value)}\n' for key, value in records),
                                  text=True, capture_output=True)

    def vector(self, name, x, y):
        return {'id': name, 'size': 2, 'indices': [0, 1], 'values': [x, y]}

    def test_means_last_group_and_missing_cluster(self):
        previous = [[0, 0], [10, 10], [100, 100]]
        records = [('0', self.vector('a', 0, 2)), ('0', self.vector('b', 2, 0)),
                   ('1', self.vector('c', 8, 10)), ('1', self.vector('d', 10, 8))]
        result = self.execute(records, previous)
        self.assertEqual(result.returncode, 0, result.stderr)
        updates = [dict(cluster=int(key), **json.loads(payload))
                   for key, payload in (line.split('\t', 1) for line in result.stdout.splitlines())]
        centers, counts = merge_center_updates(previous, updates)
        self.assertEqual(centers, [[1, 1], [9, 9], [100, 100]])
        self.assertEqual(counts, [2, 2, 0])
        self.assertEqual(previous, [[0, 0], [10, 10], [100, 100]])

    def test_empty_partition_does_not_duplicate_empty_clusters(self):
        result = self.execute([], [[0, 0], [1, 1]])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, '')

    def test_text_key_order_is_lexical(self):
        result = self.execute([('10', self.vector('a', 1, 1)),
                               ('2', self.vector('b', 2, 2))], [[0, 0] for _ in range(12)])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual([line.split('\t')[0] for line in result.stdout.splitlines()], ['10', '2'])

    def test_unsorted_input_is_rejected(self):
        result = self.execute([('1', self.vector('a', 1, 1)),
                               ('0', self.vector('b', 0, 0))], [[0, 0], [1, 1]])
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('not sorted', result.stderr)

    def test_duplicate_reducer_updates_are_rejected(self):
        update = {'cluster': 0, 'count': 1, 'center': [1, 1]}
        with self.assertRaises(ValueError):
            merge_center_updates([[0, 0]], [update, update])
