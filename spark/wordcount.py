"""Full WordCount; the master is configured through spark-submit."""
import argparse
from operator import add
from pathlib import Path
import sys
from pyspark.sql import SparkSession

sys.path.append(str(Path(__file__).resolve().parents[1] / "common"))
from tokenization import tokenize



def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--partitions", type=int, default=2)
    args = parser.parse_args()
    if args.partitions < 1:
        parser.error("partitions must be positive")
    spark = SparkSession.builder.appName("ComparisonWordCount").getOrCreate()
    try:
        sc = spark.sparkContext
        sc.setLogLevel("WARN")
        lines = sc.textFile(args.input, minPartitions=args.partitions)
        print(f'Input partitions: {lines.getNumPartitions()}; output partitions: {args.partitions}',
              flush=True)
        counts = (lines
                  .flatMap(tokenize).map(lambda word: (word, 1))
                  .reduceByKey(add, numPartitions=args.partitions))
        counts.map(lambda pair: f"{pair[0]}\t{pair[1]}").saveAsTextFile(args.output)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
