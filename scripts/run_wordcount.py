"""Launch a real engine without overwriting existing outputs."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
PROFILE = json.loads((ROOT / "config/wordcount.json").read_text())


def spark_environment(code_root):
    env = os.environ.copy()
    if not env.get("SPARK_HOME"):
        spec = importlib.util.find_spec("pyspark")
        candidates = ([Path(spec.origin).parent] if spec and spec.origin else
                      list((ROOT / ".venv/lib").glob("python*/site-packages/pyspark")))
        if len(candidates) != 1 or not (candidates[0] / "bin/spark-submit").is_file():
            raise ValueError("PySpark installation not found; activate .venv")
        # Spark shell scripts also receive a path without spaces.
        alias = code_root / "spark-home"
        alias.symlink_to(candidates[0], target_is_directory=True)
        env["SPARK_HOME"] = str(alias)
    # The driver needs the environment containing PySpark; Spark distributes it to workers.
    driver = ROOT / ".venv/bin/python3"
    env["PYSPARK_DRIVER_PYTHON"] = str(driver if driver.is_file() else Path(sys.executable))
    return env


def executable(name):
    candidate = shutil.which(name)
    if not candidate and name == "spark-submit":
        local = ROOT / ".venv/bin/spark-submit"
        if local.is_file():
            candidate = str(local)
    if not candidate:
        raise ValueError(f"{name} not found. Install the engine and add its bin directory to PATH.")
    return candidate


def command(args, code_root=ROOT):
    if args.mode == "local":
        source_path = Path(args.input).resolve()
        target_path = Path(args.output).resolve()
        if not source_path.is_file():
            raise ValueError(f"Input file not found: {source_path}")
        if target_path.exists():
            raise ValueError(f"Output path already exists: {target_path}")
        source, target = source_path.as_uri(), target_path.as_uri()
    else:
        if not args.input.startswith("hdfs://") or not args.output.startswith("hdfs://"):
            raise ValueError("For yarn, provide full hdfs:// URIs for input and output")
        source, target = args.input, args.output
    if args.engine == "spark":
        options = {
            "spark.dynamicAllocation.enabled": "false",
            "spark.executor.instances": 1,
            "spark.executor.cores": args.workers,
            "spark.executor.memory": f"{PROFILE['task_heap_mb'] * args.workers}m",
            "spark.executor.memoryOverhead":
                (PROFILE['task_memory_mb'] - PROFILE['task_heap_mb']) * args.workers,
            "spark.yarn.am.memory": f"{PROFILE['application_master_memory_mb'] - 512}m",
            "spark.yarn.am.memoryOverhead": 512,
            "spark.yarn.am.cores": 1,
            "spark.speculation": "false",
            "spark.hadoop.mapreduce.input.fileinputformat.split.maxsize": PROFILE['split_bytes'],
        }
        result = [executable("spark-submit"), "--master",
                f"local[{args.workers}]" if args.mode == "local" else "yarn",
                "--deploy-mode", "client", "--driver-memory", "1024m"]
        for key, value in options.items():
            result += ["--conf", f"{key}={value}"]
        return result + ["--py-files", str(code_root / "common/tokenization.py"),
                str(code_root / "spark/wordcount.py"), "--input", source,
                "--output", target, "--partitions", str(args.partitions)]
    hadoop = executable("hadoop")
    home = Path(os.environ.get("HADOOP_HOME", Path(hadoop).resolve().parents[1]))
    jars = sorted((home / "share/hadoop/tools/lib").glob("hadoop-streaming-*.jar"))
    if len(jars) != 1:
        raise ValueError(f"Expected exactly one Hadoop Streaming JAR in {home}/share/hadoop/tools/lib")
    files = [code_root / "hadoop/mapper.py", code_root / "hadoop/reducer.py",
             code_root / "common/tokenization.py"]
    result = [hadoop, "jar", str(jars[0]),
              "-D", f"mapreduce.framework.name={args.mode}",
              "-D", f"mapreduce.job.reduces={args.partitions}",
              "-D", f"mapreduce.local.map.tasks.maximum={args.workers}",
              "-D", f"mapreduce.local.reduce.tasks.maximum={args.workers}"]
    if args.mode == "local":
        result += ["-D", "fs.defaultFS=file:///"]
    options = {
        "mapreduce.job.running.map.limit": args.workers,
        "mapreduce.job.running.reduce.limit": args.workers,
        "mapreduce.job.reduce.slowstart.completedmaps": 1.0,
        "mapreduce.map.memory.mb": PROFILE['task_memory_mb'],
        "mapreduce.reduce.memory.mb": PROFILE['task_memory_mb'],
        "mapreduce.map.java.opts": f"-Xmx{PROFILE['task_heap_mb']}m",
        "mapreduce.reduce.java.opts": f"-Xmx{PROFILE['task_heap_mb']}m",
        "mapreduce.map.cpu.vcores": 1,
        "mapreduce.reduce.cpu.vcores": 1,
        "yarn.app.mapreduce.am.resource.mb": PROFILE['application_master_memory_mb'],
        "yarn.app.mapreduce.am.command-opts": "-Xmx768m",
        "mapreduce.map.speculative": "false",
        "mapreduce.reduce.speculative": "false",
        "mapreduce.input.fileinputformat.split.maxsize": PROFILE['split_bytes'],
    }
    for key, value in options.items():
        result += ["-D", f"{key}={value}"]
    return result + ["-files", ",".join(path.as_uri() for path in files),
                     "-cmdenv", "PYTHONIOENCODING=utf-8",
                     "-mapper", "python3 mapper.py", "-combiner", "python3 reducer.py",
                     "-reducer", "python3 reducer.py", "-input", source, "-output", target]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("engine", choices=["hadoop", "spark"])
    parser.add_argument("--mode", choices=["local", "yarn"], default="local")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--workers", type=int, default=PROFILE['workers'])
    parser.add_argument("--partitions", type=int, default=PROFILE['partitions'])
    parser.add_argument("--metrics", type=Path, help="New JSON file for run timing")
    args = parser.parse_args()
    if args.workers < 1 or args.partitions < 1:
        parser.error("workers and partitions must be positive")
    if args.metrics and args.metrics.exists():
        parser.error("The metrics file already exists")
    # Hadoop reparses URIs and loses escaped spaces.
    # Copy only source files; keep the directory until the application finishes.
    with tempfile.TemporaryDirectory(prefix="hscompare-code-", dir="/tmp") as directory:
        code_root = Path(directory)
        for folder in ("common", "hadoop", "spark"):
            shutil.copytree(ROOT / folder, code_root / folder,
                            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        try:
            invocation = command(args, code_root)
            env = spark_environment(code_root) if args.engine == "spark" else os.environ.copy()
        except ValueError as error:
            parser.error(str(error))
        print(shlex.join(invocation), flush=True)
        started = time.perf_counter()
        completed = subprocess.run(invocation, cwd=code_root, env=env)
        elapsed = time.perf_counter() - started
    record = {"engine": args.engine, "mode": args.mode, "input": args.input,
              "output": args.output, "workers": args.workers,
              "partitions": args.partitions,
              "resource_profile": PROFILE,
              "elapsed_seconds": elapsed,
              "exit_code": completed.returncode, "command": invocation}
    if args.metrics:
        args.metrics.parent.mkdir(parents=True, exist_ok=True)
        with args.metrics.open("x", encoding="utf-8") as stream:
            json.dump(record, stream, ensure_ascii=False, indent=2)
    print(json.dumps(record, ensure_ascii=False), flush=True)
    sys.exit(completed.returncode)


if __name__ == "__main__":
    main()
