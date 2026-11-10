"""Fixed-iteration PySpark Lloyd K-Means over shared sparse document vectors."""
import argparse
import json
from pathlib import Path
import sys
import time

from pyspark import StorageLevel
from pyspark.sql import SparkSession

sys.path.append(str(Path(__file__).resolve().parents[1] / 'common'))
from kmeans import load_centers, validate_vector, partition_sums, combine_sums, update_centers


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--centers', type=Path, required=True)
    parser.add_argument('--local-results', type=Path, required=True,
                        help='Client-mode driver snapshot directory')
    parser.add_argument('--iterations', type=int, required=True)
    parser.add_argument('--documents', type=int, required=True)
    parser.add_argument('--partitions', type=int, required=True)
    args = parser.parse_args()
    if min(args.iterations, args.documents, args.partitions) < 1:
        parser.error('iterations, documents, and partitions must be positive')
    centers = load_centers(args.centers)
    dimension = len(centers[0])
    spark = SparkSession.builder.appName('ComparisonKMeans').getOrCreate()
    vectors = None
    try:
        sc = spark.sparkContext
        sc.setLogLevel('WARN')
        started = time.perf_counter()
        vectors = (sc.textFile(args.input, minPartitions=args.partitions)
                   .map(lambda line: validate_vector(json.loads(line), dimension))
                   .persist(StorageLevel.MEMORY_AND_DISK))
        count = vectors.count()  # Materialize the cache; this time is included.
        if count != args.documents:
            raise ValueError(f'Document count mismatch: {count} != {args.documents}')
        load_seconds = time.perf_counter() - started
        rows = []
        for iteration in range(1, args.iterations + 1):
            step_started = time.perf_counter()
            broadcast = sc.broadcast(centers)
            try:
                partials = (vectors.mapPartitions(lambda items: partition_sums(items, broadcast.value))
                            .reduceByKey(combine_sums, numPartitions=args.partitions).collect())
            finally:
                broadcast.destroy(blocking=True)
            sums = [[0.0] * dimension for _ in centers]
            counts = [0] * len(centers)
            seen = set()
            for cluster, (total, cluster_count) in partials:
                if cluster in seen or not 0 <= cluster < len(centers):
                    raise ValueError('Invalid or duplicate cluster aggregation')
                seen.add(cluster)
                sums[cluster], counts[cluster] = total, cluster_count
            if sum(counts) != args.documents:
                raise ValueError('Document count changed during iteration')
            centers = update_centers(centers, sums, counts)
            state = {'clusters': len(centers), 'dimensions': dimension, 'iteration': iteration,
                     'centers': centers, 'counts': counts,
                     'counts_note': 'assignments to the previous centers used in this iteration'}
            (args.local_results / f'iteration-{iteration:03d}-centers.json').write_text(
                json.dumps(state, allow_nan=False, indent=2))
            row = {'iteration': iteration, 'iteration_seconds': time.perf_counter() - step_started,
                   'cumulative_seconds': time.perf_counter() - started, 'counts': counts}
            rows.append(row)
            print(f'Iteration {iteration}: counts={counts}; cumulative={row["cumulative_seconds"]:.3f} s',
                  flush=True)
        # Only the small final center state goes to the driver and HDFS.
        sc.parallelize([json.dumps(state, allow_nan=False)], 1).saveAsTextFile(args.output)
        (args.local_results / 'spark-internal.json').write_text(json.dumps({
            'load_and_persist_seconds': load_seconds, 'input_partitions': vectors.getNumPartitions(),
            'persistence': 'MEMORY_AND_DISK', 'steps': rows,
            'session_seconds': time.perf_counter() - started,
            'timing_note': 'internal diagnostics exclude JVM/session startup; use wrapper total for comparison'
        }, indent=2))
    finally:
        if vectors is not None:
            vectors.unpersist()
        spark.stop()


if __name__ == '__main__':
    main()
