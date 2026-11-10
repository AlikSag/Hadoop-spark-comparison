"""Check partition aggregation and Spark submission without a running cluster."""
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
for folder in ('common', 'scripts'):
    sys.path.insert(0, str(ROOT / folder))
from kmeans import partition_sums, combine_sums, update_centers
import run_spark_kmeans


class SparkKMeansTest(unittest.TestCase):
    def test_unequal_partition_sizes_use_sums_not_partial_means(self):
        def point(i, x):
            return {'id': str(i), 'size': 2, 'indices': [0], 'values': [x]}
        centers = [[0, 0], [1000, 1000]]
        partials = [dict(partition_sums(partition, centers)) for partition in
                    [[point(0, 0)], [point(1, 4), point(2, 8), point(3, 12)]]]
        total, count = combine_sums(partials[0][0], partials[1][0])
        self.assertEqual(count, 4)
        self.assertEqual(update_centers(centers, [total, [0, 0]], [count, 0]), [[6, 0], [1000, 1000]])
        self.assertEqual(list(partition_sums([], centers)), [])

    def test_submission_ships_shared_module_and_fixed_profile(self):
        profile = {'workers': 2, 'partitions': 2, 'task_heap_mb': 512,
                   'task_memory_mb': 1024, 'application_master_memory_mb': 1024,
                   'split_bytes': 67108864}
        with tempfile.TemporaryDirectory(prefix='spark-kmeans-test-', dir='/tmp') as folder:
            staging = Path(folder)
            with patch.object(run_spark_kmeans, 'executable', return_value='/fake/spark-submit'):
                command = run_spark_kmeans.build_command(staging, 'hdfs://localhost/input',
                    'hdfs://localhost/output', staging, 2, 1000, profile)
            self.assertEqual(command[command.index('--master') + 1], 'yarn')
            self.assertIn('spark.executor.memory=1024m', command)
            self.assertIn('spark.executor.memoryOverhead=1024', command)
            self.assertEqual(command[command.index('--py-files') + 1], str(staging / 'kmeans.py'))
            self.assertEqual(command[command.index('--iterations') + 1], '2')
