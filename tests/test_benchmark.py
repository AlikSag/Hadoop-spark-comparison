"""Check experiment order, text boundaries, and statistics."""
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from benchmark import make_sample, schedule, summarize


class BenchmarkTest(unittest.TestCase):
    def test_each_pair_and_alternating_order(self):
        jobs = list(schedule([10, 100, 500], 3))
        self.assertEqual(len(jobs), 18)
        for size in (10, 100, 500):
            for repetition in (1, 2, 3):
                self.assertEqual({engine for s, r, engine in jobs if (s, r) == (size, repetition)},
                                 {'hadoop', 'spark'})
        self.assertEqual([e for s, r, e in jobs if s == 10 and r == 1], ['hadoop', 'spark'])
        self.assertEqual([e for s, r, e in jobs if s == 10 and r == 2], ['spark', 'hadoop'])

    def test_sample_ends_at_complete_unicode_line(self):
        with tempfile.TemporaryDirectory() as folder:
            source = Path(folder) / 'source.txt'
            target = Path(folder) / 'sample.txt'
            block = '\u041f\u0440\u0438\u0432\u0435\u0442, \u043c\u0438\u0440!\n'.encode()
            source.write_bytes(block * (2 * 1024 * 1024 // len(block)))
            metadata = make_sample(source, target, 1)
            self.assertLessEqual(metadata['actual_bytes'], 1024 * 1024)
            self.assertGreater(metadata['actual_bytes'], 1024 * 1024 - len(block))
            self.assertTrue(target.read_text().endswith('\n'))

    def test_summary_uses_median_and_measured_ratio(self):
        rows = [{'requested_mib': 10, 'actual_bytes': 100, 'engine': engine,
                 'elapsed_seconds': value} for engine, values in
                [('hadoop', [10, 100, 12]), ('spark', [5, 6, 7])] for value in values]
        output = summarize(rows)
        self.assertEqual(output[0]['median_seconds'], 12)
        self.assertEqual(output[1]['median_seconds'], 6)
        self.assertEqual(output[0]['hadoop_over_spark_median'], 2)
