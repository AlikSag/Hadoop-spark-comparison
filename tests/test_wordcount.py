"""Check the Streaming protocol and shared semantics without emulating the engines."""
from collections import Counter
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'common'))
sys.path.insert(0, str(ROOT / 'scripts'))
from tokenization import tokenize
from check_wordcount import read_counts


class WordCountTest(unittest.TestCase):
    def run_script(self, name, text):
        return subprocess.run([sys.executable, str(ROOT / 'hadoop' / name)],
                              input=text, text=True, encoding='utf-8',
                              capture_output=True, check=True).stdout

    def test_streaming_protocol_and_combiner(self):
        chunks = ['Spark, SPARK! Hadoop\n\u041f\u0440\u0438\u0432\u0435\u0442_\u043c\u0438\u0440 123\n',
                  'Hadoop spark\n\u043f\u0440\u0438\u0432\u0435\u0442 123 — \u043c\u0438\u0440\n']
        partials = []
        for chunk in chunks:
            mapped = self.run_script('mapper.py', chunk)
            # Protocol test only: Hadoop performs sorting in a real job.
            ordered = ''.join(sorted(mapped.splitlines(keepends=True)))
            partials.extend(self.run_script('reducer.py', ordered).splitlines(keepends=True))
        reduced = self.run_script('reducer.py', ''.join(sorted(partials)))
        actual = {word: int(value) for word, value in
                  (line.split('\t') for line in reduced.splitlines())}
        expected = {'spark': 3, 'hadoop': 2, '\u043f\u0440\u0438\u0432\u0435\u0442': 2, '\u043c\u0438\u0440': 2, '123': 2}
        self.assertEqual(actual, expected)
        self.assertEqual(Counter(tokenize(''.join(chunks))), expected)

    def test_empty_input(self):
        self.assertEqual(self.run_script('mapper.py', '---\n'), '')
        self.assertEqual(self.run_script('reducer.py', ''), '')

    def test_mapper_with_hadoop_localized_symlinks(self):
        with tempfile.TemporaryDirectory() as folder:
            work = Path(folder) / 'work'
            cache = Path(folder) / 'cache'
            work.mkdir()
            cache.mkdir()
            # YARN stores the script and module in separate cache directories.
            (cache / 'mapper.py').write_text((ROOT / 'hadoop/mapper.py').read_text())
            (work / 'mapper.py').symlink_to(cache / 'mapper.py')
            (work / 'tokenization.py').symlink_to(ROOT / 'common/tokenization.py')
            result = subprocess.run([sys.executable, 'mapper.py'], cwd=work,
                                    input='SPARK!', text=True, capture_output=True,
                                    check=True)
            self.assertEqual(result.stdout, 'spark\t1\n')

    def test_validation_rejects_duplicate_keys(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)
            (path / '_SUCCESS').touch()
            (path / 'part-00000').write_text('spark\t1\nspark\t2\n')
            with self.assertRaises(ValueError):
                read_counts(path)


if __name__ == '__main__':
    unittest.main()
