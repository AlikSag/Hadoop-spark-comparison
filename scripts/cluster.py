"""Local educational HDFS + YARN. Store the distribution and data in .runtime/."""
import argparse
import hashlib
import os
from pathlib import Path
import re
import shlex
import shutil
import socket
import subprocess
import sys
import tarfile
import time
import uuid
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / '.runtime'
VERSION = '3.5.0'
# Hadoop shell scripts require paths without spaces; data stays in the project.
KEY = hashlib.sha256(str(ROOT).encode()).hexdigest()[:10]
ALIAS = Path('/tmp') / f'hscompare-{os.getuid()}-{KEY}'
HOME = ALIAS / f'hadoop-{VERSION}'
CONF = ALIAS / 'conf'
URL = f'https://archive.apache.org/dist/hadoop/common/hadoop-{VERSION}/hadoop-{VERSION}.tar.gz'
DAEMONS = [('hdfs', 'namenode'), ('hdfs', 'datanode'),
           ('yarn', 'resourcemanager'), ('yarn', 'nodemanager')]


def runtime():
    STATE.mkdir(exist_ok=True, mode=0o700)
    if ALIAS.is_symlink():
        if ALIAS.resolve() != STATE.resolve():
            raise ValueError(f'Path belongs to another project: {ALIAS}')
    elif ALIAS.exists():
        raise ValueError(f'Path already exists: {ALIAS}')
    else:
        ALIAS.symlink_to(STATE, target_is_directory=True)


def java_home():
    java = shutil.which('java')
    if not java:
        raise ValueError('Java 17 not found')
    home = Path(java).resolve().parents[1]
    version = subprocess.run([str(home / 'bin/java'), '-version'],
                             capture_output=True, text=True, check=True)
    if not re.search(r'version "17[.\"]', version.stderr + version.stdout):
        raise ValueError('This configuration requires Java 17')
    return home


def environment():
    env = os.environ.copy()
    env.update(JAVA_HOME=str(java_home()), HADOOP_HOME=str(HOME),
               HADOOP_CONF_DIR=str(CONF), YARN_CONF_DIR=str(CONF),
               HADOOP_LOG_DIR=str(ALIAS / 'logs'), HADOOP_PID_DIR=str(ALIAS / 'pids'),
               YARN_LOG_DIR=str(ALIAS / 'logs'), YARN_PID_DIR=str(ALIAS / 'pids'),
               HADOOP_IDENT_STRING=f'hscompare-{KEY}',
               SPARK_LOCAL_IP='127.0.0.1', PYSPARK_PYTHON='/usr/bin/python3')
    for component in ('COMMON', 'HDFS', 'MAPRED', 'YARN'):
        env[f'HADOOP_{component}_HOME'] = str(HOME)
    env['PATH'] = f'{HOME}/bin:{ROOT}/.venv/bin:' + env.get('PATH', '')
    return env


def run(argv, **kwargs):
    print(shlex.join(map(str, argv)), flush=True)
    working_directory = kwargs.pop('cwd', ROOT)
    return subprocess.run(list(map(str, argv)), env=environment(), cwd=working_directory,
                          check=True, **kwargs)


def xml_file(name, values):
    tree = ET.Element('configuration')
    for key, value in values.items():
        prop = ET.SubElement(tree, 'property')
        ET.SubElement(prop, 'name').text = key
        ET.SubElement(prop, 'value').text = str(value)
    ET.indent(tree)
    ET.ElementTree(tree).write(CONF / name, encoding='utf-8', xml_declaration=True)


def configure():
    runtime()
    for folder in ('conf', 'logs', 'pids', 'tmp', 'yarn-local', 'yarn-logs'):
        (ALIAS / folder).mkdir(exist_ok=True)
    xml_file('core-site.xml', {'fs.defaultFS': 'hdfs://localhost:9000',
                              'hadoop.tmp.dir': str(ALIAS / 'tmp')})
    xml_file('hdfs-site.xml', {
        'dfs.replication': 1,
        'dfs.namenode.name.dir': (ALIAS / 'namenode').as_uri(),
        'dfs.datanode.data.dir': (ALIAS / 'datanode').as_uri(),
        'dfs.namenode.rpc-address': 'localhost:9000',
        'dfs.namenode.http-address': '127.0.0.1:9870',
        'dfs.datanode.address': '127.0.0.1:9866',
        'dfs.datanode.http.address': '127.0.0.1:9864',
        'dfs.datanode.ipc.address': '127.0.0.1:9867'})
    xml_file('mapred-site.xml', {
        'mapreduce.framework.name': 'yarn',
        'mapreduce.application.classpath':
            f'{HOME}/share/hadoop/mapreduce/*:{HOME}/share/hadoop/mapreduce/lib/*',
        'yarn.app.mapreduce.am.resource.mb': 1024,
        'mapreduce.map.memory.mb': 1024, 'mapreduce.reduce.memory.mb': 1024,
        'mapreduce.map.java.opts': '-Xmx768m',
        'mapreduce.reduce.java.opts': '-Xmx768m',
        'mapreduce.map.cpu.vcores': 1, 'mapreduce.reduce.cpu.vcores': 1})
    xml_file('yarn-site.xml', {
        'yarn.resourcemanager.hostname': 'localhost',
        'yarn.resourcemanager.bind-host': '127.0.0.1',
        'yarn.resourcemanager.scheduler.class':
            'org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler',
        'yarn.nodemanager.hostname': 'localhost',
        'yarn.nodemanager.bind-host': '127.0.0.1',
        'yarn.nodemanager.local-dirs': str(ALIAS / 'yarn-local'),
        'yarn.nodemanager.log-dirs': str(ALIAS / 'yarn-logs'),
        'yarn.nodemanager.resource.memory-mb': 8192,
        'yarn.nodemanager.resource.cpu-vcores': 4,
        'yarn.scheduler.minimum-allocation-mb': 512,
        'yarn.scheduler.maximum-allocation-mb': 8192,
        'yarn.scheduler.maximum-allocation-vcores': 4,
        'yarn.nodemanager.aux-services': 'mapreduce_shuffle',
        'yarn.nodemanager.aux-services.mapreduce_shuffle.class':
            'org.apache.hadoop.mapred.ShuffleHandler',
        'mapreduce.shuffle.port': 13562,
        'yarn.nodemanager.vmem-check-enabled': 'false',
        'yarn.nodemanager.env-whitelist':
            'JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,'
            'HADOOP_YARN_HOME,HADOOP_HOME,HADOOP_MAPRED_HOME,PATH,LANG,TZ,CLASSPATH_PREPEND_DISTCACHE'})
    xml_file('capacity-scheduler.xml', {
        'yarn.scheduler.capacity.root.queues': 'default',
        'yarn.scheduler.capacity.root.default.capacity': 100,
        'yarn.scheduler.capacity.root.default.maximum-capacity': 100,
        'yarn.scheduler.capacity.maximum-am-resource-percent': 0.5,
        'yarn.scheduler.capacity.root.default.acl_submit_applications': '*'})
    env = environment()
    keys = ['JAVA_HOME', 'HADOOP_HOME', 'HADOOP_CONF_DIR', 'YARN_CONF_DIR',
            'HADOOP_LOG_DIR', 'HADOOP_PID_DIR', 'YARN_LOG_DIR', 'YARN_PID_DIR',
            'HADOOP_IDENT_STRING', 'SPARK_LOCAL_IP', 'PYSPARK_PYTHON']
    keys += [f'HADOOP_{c}_HOME' for c in ('COMMON', 'HDFS', 'MAPRED', 'YARN')]
    exports = [f'export {key}={shlex.quote(env[key])}' for key in keys]
    exports.append(f'export PATH={shlex.quote(str(HOME / "bin"))}:"$PATH"')
    (STATE / 'env.sh').write_text('\n'.join(exports) + '\n')
    print(f'Configuration: {STATE}/conf; YARN: 4 vcores, 8192 MiB', flush=True)


def install():
    runtime()
    if (HOME / 'bin/hadoop').is_file():
        print('Hadoop is already installed')
        return
    if HOME.exists():
        raise ValueError(f'Incomplete installation: {HOME}; inspect it manually')
    archive = STATE / f'hadoop-{VERSION}.tar.gz'
    checksum = archive.with_suffix('.gz.sha512')
    for url, target in ((URL, archive), (URL + '.sha512', checksum)):
        if not target.is_file():
            partial = target.with_suffix(target.suffix + '.part')
            subprocess.run(['curl', '--fail', '--location', '--connect-timeout', '15',
                            '--speed-limit', '1024', '--speed-time', '30',
                            '--output', str(partial), url], check=True)
            partial.replace(target)
    expected = re.search(r'\b[0-9a-fA-F]{128}\b', checksum.read_text())
    with archive.open('rb') as stream:
        actual = hashlib.file_digest(stream, 'sha512').hexdigest()
    if not expected or actual != expected.group().lower():
        raise ValueError('Archive SHA512 mismatch; installation stopped')
    with tarfile.open(archive) as tar:
        tar.extractall(STATE, filter='data')
    if not (HOME / 'bin/hadoop').is_file():
        raise ValueError('The expected Hadoop distribution was not found in the archive')


def require_install():
    if not (HOME / 'bin/hadoop').is_file():
        raise ValueError('First run python3 scripts/cluster.py setup')
    # The custom configuration directory also needs the standard logging configuration.
    for name in ('log4j.properties', 'hadoop-metrics2.properties'):
        source = HOME / 'etc/hadoop' / name
        target = CONF / name
        if source.is_file() and not target.exists():
            shutil.copyfile(source, target)


def initialize():
    version = ALIAS / 'namenode/current/VERSION'
    if version.is_file():
        return
    for name in ('namenode', 'datanode'):
        path = ALIAS / name
        if path.exists() and any(path.iterdir()):
            raise ValueError(f'Refusing to format existing data: {path}')
    run([HOME / 'bin/hdfs', 'namenode', '-format', '-nonInteractive'])


def wait_port(port):
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(('127.0.0.1', port), timeout=1):
                return
        except OSError:
            time.sleep(1)
    raise ValueError(f'Port {port} did not open; check {STATE}/logs')


def start():
    require_install()
    initialize()
    for tool, daemon in DAEMONS:
        run([HOME / 'bin' / tool, '--daemon', 'start', daemon])
    wait_port(9000)
    wait_port(8088)
    run([HOME / 'bin/hdfs', 'dfsadmin', '-safemode', 'wait'], timeout=60)
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        nodes = run([HOME / 'bin/yarn', 'node', '-list', '-states', 'RUNNING'],
                    capture_output=True, text=True, timeout=15)
        if re.search(r'Total Nodes:\s*1\b', nodes.stdout):
            print(nodes.stdout)
            print('HDFS and YARN are ready. Web UIs: localhost:9870 and localhost:8088')
            return
        time.sleep(2)
    raise ValueError('NodeManager did not register with YARN; check the logs')


def smoke():
    require_install()
    from check_wordcount import read_counts
    test_id = 'smoke-' + uuid.uuid4().hex[:10]
    local = ROOT / 'results' / test_id
    local.mkdir(parents=True)
    source = local / 'input.txt'
    source.write_text('Spark, SPARK! Hadoop\n\u041f\u0440\u0438\u0432\u0435\u0442_\u043c\u0438\u0440 123\nHadoop spark\n\u043f\u0440\u0438\u0432\u0435\u0442 123 — \u043c\u0438\u0440\n', encoding='utf-8')
    base = f'hdfs://localhost:9000/experiment/{test_id}'
    hdfs = HOME / 'bin/hdfs'
    # Spark staging uses the current user's HDFS home directory.
    import getpass
    run([hdfs, 'dfs', '-mkdir', '-p', f'/user/{getpass.getuser()}', base])
    # Stream the bytes directly so Hadoop does not have to parse a local path
    # containing spaces. '-' reads the input file from stdin.
    with source.open('rb') as stream:
        run([hdfs, 'dfs', '-put', '-', base + '/input.txt'], stdin=stream)
    for engine in ('hadoop', 'spark'):
        with (local / f'{engine}.log').open('w') as log:
            run([sys.executable, ROOT / 'scripts/run_wordcount.py', engine,
                 '--mode', 'yarn', '--input', base + '/input.txt',
                 '--output', base + '/' + engine,
                 '--metrics', local / f'{engine}.json'],
                stdout=log, stderr=subprocess.STDOUT, timeout=300)
        run([hdfs, 'dfs', '-get', base + '/' + engine, engine], cwd=local)
    expected = {'spark': 3, 'hadoop': 2, '\u043f\u0440\u0438\u0432\u0435\u0442': 2, '\u043c\u0438\u0440': 2, '123': 2}
    for engine in ('hadoop', 'spark'):
        if read_counts(local / engine) != expected:
            raise ValueError(f'Incorrect output from {engine}: {local}')
    print(f'Both engines produced the expected output: 5 keys, 11 words. Logs: {local}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['configure', 'setup', 'start', 'stop', 'status', 'smoke'])
    args = parser.parse_args()
    try:
        runtime()
        if args.action == 'configure':
            configure()
        elif args.action == 'setup':
            configure()
            install()
            start()
        else:
            require_install()
            if args.action == 'start':
                start()
            elif args.action == 'smoke':
                smoke()
            elif args.action == 'stop':
                for tool, daemon in reversed(DAEMONS):
                    run([HOME / 'bin' / tool, '--daemon', 'stop', daemon])
            elif args.action == 'status':
                run([HOME / 'bin/hdfs', 'dfsadmin', '-report'], timeout=30)
                run([HOME / 'bin/yarn', 'node', '-list'], timeout=30)
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        parser.exit(1, f'Error: {error}\nLogs: {STATE}/logs and {ROOT}/results\n')


if __name__ == '__main__':
    main()
