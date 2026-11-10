# Interview walkthrough

## A short introduction

“I built a Python project comparing Hadoop Streaming and Apache Spark on two
workloads: WordCount and iterative K-Means. Both run through a real single-node
HDFS/YARN cluster. I used common inputs and numerical rules, repeated each condition
three times, and checked complete outputs before interpreting performance. Spark
was faster in this setup, especially when K-Means reused persisted input across
iterations. These measurements describe one machine, not multi-node scalability.”

## Code to walk through

| Topic | Entry points | What to explain |
|---|---|---|
| MapReduce | `hadoop/mapper.py`, `hadoop/reducer.py` | Streaming protocol, shuffle, sorting, and combiner |
| Spark WordCount | `spark/wordcount.py` | RDD transformations and `reduceByKey` |
| Shared behavior | `common/tokenization.py`, `common/kmeans.py` | Consistent tokens, tie breaking, empty clusters |
| Iterative processing | `spark/kmeans_job.py`, `scripts/run_hadoop_kmeans.py` | Persisted input versus one job per iteration |
| Experimental design | `scripts/benchmark.py`, `main.py` | Repetitions, engine order, timing boundaries |
| Verification | `tests/`, `scripts/check_wordcount.py`, `scripts/check_kmeans.py` | Exact counts and floating-point tolerances |

## Tradeoffs worth discussing

- The single-node setup is reproducible and accessible, but does not measure
  network behavior or horizontal scalability.
- Equal requested worker budgets do not imply equal total memory use. The Spark
  driver runs outside YARN, and the engines have different control processes.
- WordCount compares complete counts, not only the most frequent words.
- K-Means shares input vectors and initial centers. Fixed iteration counts avoid
  comparing different convergence decisions.
- Spark's advantage cannot be attributed solely to caching. Its local aggregation,
  startup behavior, serialization, and shuffle also differ from Hadoop Streaming.
- Three repetitions and min–max ranges describe observed variation; they do not
  establish a universal performance claim.
- A decreasing K-Means objective checks numerical consistency, not semantic quality
  against labeled ground truth.

## Possible future work

Measure peak memory, CPU, and disk traffic; add larger document samples and
multi-node experiments; investigate a Hadoop K-Means combiner; randomize and
balance run order; isolate persistence and aggregation effects with ablation runs.
These are ideas for extensions, not features already implemented.
