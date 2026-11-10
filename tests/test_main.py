"""Ensure the full runner schedules complete series into matching report inputs."""
import importlib.util
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('project_main', ROOT / 'main.py')
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class FullRunnerTest(unittest.TestCase):
    def test_complete_kmeans_series_and_analysis_paths(self):
        output = Path('/tmp/full experiment')
        schedule = list(runner.commands(output))
        jobs = [cmd for cmd in schedule if Path(cmd[1]).name in
                ('run_hadoop_kmeans.py', 'run_spark_kmeans.py')]
        self.assertEqual(len(jobs), 24)
        targets = {Path(cmd[cmd.index('--output') + 1]).name for cmd in jobs}
        self.assertEqual(targets, {
            f'{i}iter-run{r}-{e}' for i in (1, 3, 5, 10)
            for r in (1, 2, 3) for e in ('hadoop', 'spark')})
        self.assertEqual(Path(jobs[0][1]).name, 'run_hadoop_kmeans.py')
        self.assertEqual(Path(jobs[8][1]).name, 'run_spark_kmeans.py')
        analysis = schedule[-1]
        self.assertEqual(analysis[2], str(output / 'kmeans'))
        self.assertEqual(analysis[-1], str(output / 'reports'))


if __name__ == '__main__':
    unittest.main()
