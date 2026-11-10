"""Check that submitted source paths contain no spaces."""
from argparse import Namespace
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import run_wordcount


class SubmissionTest(unittest.TestCase):
    def test_spark_home_is_explicit_when_workers_use_system_python(self):
        with tempfile.TemporaryDirectory(prefix='hscompare-env-', dir='/tmp') as folder:
            root = Path(folder)
            installed = root / 'installed spark'
            (installed / 'bin').mkdir(parents=True)
            (installed / 'bin/spark-submit').touch()
            from types import SimpleNamespace
            spec = SimpleNamespace(origin=str(installed / '__init__.py'))
            with patch.dict(os.environ, {'PYSPARK_PYTHON': '/usr/bin/python3'}, clear=True), \
                    patch.object(run_wordcount.importlib.util, 'find_spec', return_value=spec):
                env = run_wordcount.spark_environment(root)
                self.assertNotIn(' ', env['SPARK_HOME'])
                self.assertEqual(Path(env['SPARK_HOME']).resolve(), installed)
                self.assertEqual(env['PYSPARK_PYTHON'], '/usr/bin/python3')
                self.assertTrue(Path(env['PYSPARK_DRIVER_PYTHON']).is_file())

    def test_yarn_uses_staged_files_for_both_engines(self):
        with tempfile.TemporaryDirectory(prefix='hscompare-test-', dir='/tmp') as folder:
            root = Path(folder)
            jar = root / 'share/hadoop/tools/lib/hadoop-streaming-test.jar'
            jar.parent.mkdir(parents=True)
            jar.touch()
            args = Namespace(engine='hadoop', mode='yarn', workers=2, partitions=2,
                             input='hdfs://localhost:9000/input',
                             output='hdfs://localhost:9000/output')
            with patch.dict(os.environ, {'HADOOP_HOME': str(root)}), \
                    patch.object(run_wordcount, 'executable', return_value='/fake/command'):
                argv = run_wordcount.command(args, root)
                files = argv[argv.index('-files') + 1]
                self.assertNotIn(' ', files)
                self.assertNotIn('%20', files)
                self.assertIn((root / 'hadoop/mapper.py').as_uri(), files)
                self.assertIn('mapreduce.job.running.map.limit=2', argv)
                self.assertIn('mapreduce.job.running.reduce.limit=2', argv)
                self.assertIn('mapreduce.job.reduce.slowstart.completedmaps=1.0', argv)
                self.assertIn('mapreduce.map.memory.mb=1024', argv)
                args.engine = 'spark'
                argv = run_wordcount.command(args, root)
                self.assertIn('spark.executor.memory=1024m', argv)
                self.assertIn('spark.executor.memoryOverhead=1024', argv)
                self.assertEqual(argv[argv.index('--py-files') + 1],
                                 str(root / 'common/tokenization.py'))
                self.assertIn(str(root / 'spark/wordcount.py'), argv)
