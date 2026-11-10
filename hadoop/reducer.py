"""Hadoop sorts input by key; this reducer also works as a combiner."""
import sys


def main():
    previous = None
    total = 0
    for line in sys.stdin:
        word, count = line.rstrip("\n").split("\t")
        if previous is not None and word != previous:
            print(f"{previous}\t{total}")
            total = 0
        previous = word
        total += int(count)
    if previous is not None:
        print(f"{previous}\t{total}")


if __name__ == "__main__":
    main()
