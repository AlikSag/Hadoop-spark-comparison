"""Hadoop Streaming mapper: stdin -> word TAB 1."""
from pathlib import Path
import sys

# Hadoop localizes files separately and links them into the working directory.
sys.path.insert(0, str(Path.cwd()))
sys.path.append(str(Path(__file__).resolve().parents[1] / "common"))
from tokenization import tokenize



def main():
    for line in sys.stdin:
        for word in tokenize(line):
            print(f"{word}\t1")


if __name__ == "__main__":
    main()
