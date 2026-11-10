"""Check iteration state transfer without pretending to run an actual YARN cluster."""
from argparse import Namespace
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
for folder in ('scripts', 'common', 'hadoop'):
    sys.path.insert(0, str(ROOT / folder))
import run_hadoop_kmeans as runner
from kmeans import load_centers
from kmeans_mapper import assign_records
from kmeans_reducer import reduce_records


class RunnerTest(unittest.TestCase):
    def test_two_iterations_transfer_updated_centers_and_keep_empty_cluster(self):
        with tempfile.TemporaryDirectory(prefix='hscompare-runner-', dir='/tmp') as folder:
            root = Path(folder)
            home = root / 'hadoop'
            jar = home / 'share/hadoop/tools/lib/hadoop-streaming-test.jar'
            jar.parent.mkdir(parents=True)
            jar.touch()
            conf = root / 'conf'
            conf.mkdir()
            source = root / 'vectors.jsonl'
            points = [{'id': str(i), 'size': 2, 'indices': [0, 1], 'values': xy}
                      for i, xy in enumerate([[0, 2], [2, 0], [8, 10], [10, 8]])]
            source.write_text(''.join(json.dumps(point) + '\n' for point in points))
            centers = root / 'centers.json'
            original = [[0, 0], [10, 10], [100, 100]]
            centers.write_text(json.dumps({'clusters': 3, 'dimensions': 2, 'centers': original,
                                            'input_sha256': runner.sha256(source)}))
            args = Namespace(input=source, centers=centers, iterations=2,
                             config=ROOT / 'config/kmeans.json', output=root / 'result')
            remote = {}
            used_centers = []

            def fake_job(command, **kwargs):
                staging = Path(kwargs['cwd'])
                previous = load_centers(staging / 'centers.json')
                used_centers.append(previous)
                mapped = io.StringIO()
                assign_records(source.read_text().splitlines(keepends=True), previous, mapped)
                ordered = sorted(mapped.getvalue().splitlines(keepends=True),
                                 key=lambda line: line.split('\t', 1)[0])
                reduced = io.StringIO()
                reduce_records(ordered, previous, reduced)
                remote[command[command.index('-output') + 1]] = reduced.getvalue()
                self.assertNotIn('-combiner', command)
                self.assertNotIn(' ', command[command.index('-files') + 1])
                return subprocess.CompletedProcess(command, 0)

            def fake_hdfs(command, **kwargs):
                if '-get' in command:
                    index = command.index('-get')
                    destination = Path(kwargs['cwd']) / command[index + 2]
                    destination.mkdir()
                    (destination / '_SUCCESS').touch()
                    (destination / 'part-00000').write_text(remote[command[index + 1]])
                return subprocess.CompletedProcess(command, 0)

            with patch.object(runner.cluster, 'runtime'), patch.object(runner.cluster, 'require_install'), \
                    patch.object(runner.cluster, 'environment', return_value={}), \
                    patch.object(runner.cluster, 'HOME', home), patch.object(runner.cluster, 'CONF', conf), \
                    patch.object(runner.cluster, 'run', side_effect=fake_hdfs), \
                    patch.object(runner.subprocess, 'run', side_effect=fake_job):
                runner.execute(args)
            expected = [[1, 1], [9, 9], [100, 100]]
            self.assertEqual(used_centers, [original, expected])
            final = json.loads((args.output / 'final_centers.json').read_text())
            self.assertEqual(final['centers'], expected)
            self.assertEqual(final['counts'], [2, 2, 0])
            self.assertTrue((args.output / '_SUCCESS').is_file())

    def test_document_loss_blocks_next_iteration(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / '_SUCCESS').touch()
            (root / 'part-00000').write_text('0\t{"count": 1, "center": [1, 1]}\n')
            with self.assertRaisesRegex(ValueError, 'Document count mismatch'):
                runner.read_updates(root, [[0, 0]], expected_documents=2)
