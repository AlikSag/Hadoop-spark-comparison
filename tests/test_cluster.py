"""Check configuration generation and protection against formatting existing HDFS data."""
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import cluster


class ClusterTest(unittest.TestCase):
    def test_configuration_with_spaces_and_env(self):
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder) / 'project with spaces'
            alias = Path(folder) / 'alias'
            with patch.multiple(cluster, STATE=state, ALIAS=alias,
                                HOME=alias / 'hadoop-3.5.0', CONF=alias / 'conf'), \
                    patch.object(cluster, 'java_home', return_value=Path('/fake/jdk17')):
                cluster.configure()
                self.assertEqual(alias.resolve(), state)
                props = {p.findtext('name'): p.findtext('value') for p in
                         ET.parse(state / 'conf/core-site.xml').getroot()}
                self.assertEqual(props['fs.defaultFS'], 'hdfs://localhost:9000')
                self.assertIn('HADOOP_CONF_DIR=', (state / 'env.sh').read_text())

    def test_existing_hdfs_is_not_formatted(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'namenode/current'
            path.mkdir(parents=True)
            (path / 'VERSION').write_text('existing')
            with patch.object(cluster, 'ALIAS', Path(folder)), \
                    patch.object(cluster, 'run') as run:
                cluster.initialize()
                run.assert_not_called()

    def test_unknown_hdfs_data_blocks_formatting(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'datanode'
            path.mkdir()
            (path / 'unknown').touch()
            with patch.object(cluster, 'ALIAS', Path(folder)), \
                    patch.object(cluster, 'run') as run:
                with self.assertRaises(ValueError):
                    cluster.initialize()
                run.assert_not_called()
