"""Compare complete local outputs regardless of record order."""
import argparse
from pathlib import Path


def read_counts(directory):
    directory = Path(directory)
    if not (directory / "_SUCCESS").is_file():
        raise ValueError(f"Missing _SUCCESS: {directory}")
    parts = list(directory.glob("part-*"))
    if not parts:
        raise ValueError(f"Missing part-* files: {directory}")
    counts = {}
    for part in parts:
        with part.open(encoding="utf-8") as stream:
            for line in stream:
                word, raw_count = line.rstrip("\n").split("\t")
                count = int(raw_count)
                if word in counts or count < 1:
                    raise ValueError(f"Duplicate key or invalid count: {word!r}")
                counts[word] = count
    return counts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("hadoop_output", type=Path)
    parser.add_argument("spark_output", type=Path)
    args = parser.parse_args()
    try:
        left, right = read_counts(args.hadoop_output), read_counts(args.spark_output)
    except (ValueError, OSError) as error:
        parser.error(str(error))
    if left != right:
        parser.exit(1, "Results differ\n")
    print(f"Results match: {len(left)} keys, {sum(left.values())} words")


if __name__ == "__main__":
    main()
