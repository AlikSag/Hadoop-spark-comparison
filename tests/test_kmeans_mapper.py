"""Check the actual Streaming mapper process, including YARN file localization."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class MapperTest(unittest.TestCase):
    def execute(self, lines, localized=False):
        with tempfile.TemporaryDirectory() as folder:
            work = Path(folder) / 'work'
            cache = Path(folder) / 'cache'
            work.mkdir()
            cache.mkdir()
            centers = {'dimensions': 2, 'clusters': 2, 'centers': [[0, 0], [10, 10]]}
            (work / 'centers.json').write_text(json.dumps(centers))
            script = ROOT / 'hadoop/kmeans_mapper.py'
            if localized:
                (cache / 'mapper.py').write_bytes(script.read_bytes())
                (work / 'mapper.py').symlink_to(cache / 'mapper.py')
                (work / 'kmeans.py').symlink_to(ROOT / 'common/kmeans.py')
                script = work / 'mapper.py'
            return subprocess.run([sys.executable, str(script)], cwd=work,
                                  input=''.join(json.dumps(row) + '\n' for row in lines),
                                  text=True, capture_output=True)

    def test_assignments_preserve_vectors_and_break_ties(self):
        vectors = [{'id': name, 'size': 2, 'indices': [0, 1], 'values': values}
                   for name, values in [('near-zero', [1, 1]), ('near-ten', [9, 9]), ('tie', [5, 5])]]
        result = self.execute(vectors, localized=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        output = [line.split('\t', 1) for line in result.stdout.splitlines()]
        self.assertEqual([int(key) for key, _ in output], [0, 1, 0])
        self.assertEqual([json.loads(value) for _, value in output], vectors)

    def test_invalid_dimension_fails_with_line_number(self):
        result = self.execute([{'id': 'bad', 'size': 3, 'indices': [0], 'values': [1]}])
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, '')
        self.assertIn('Input line 1', result.stderr)

    def test_empty_partition_has_no_output(self):
        result = self.execute([])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, '')
