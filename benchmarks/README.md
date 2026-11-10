# Published benchmark measurements

Small snapshots copied from the completed local experimental series. Large input
files, engine outputs, cluster state, and execution logs remain excluded from Git.

| Workload | Original local series | Published files |
|---|---|---|
| WordCount | `wordcount-20261020-214202-2d74ef` | [Measurements](wordcount/measurements.csv), [summary](wordcount/summary.csv) |
| K-Means | `kmeans-series-0x5T5dCJ` | [Measurements](kmeans/measurements.csv), [summary](kmeans/summary.csv), [verification](kmeans/verification.json) |

Values are historical measurements, not newly executed benchmarks. See the
[WordCount](../reports/wordcount.md) and [K-Means](../reports/kmeans.md) reports for
hardware, timing boundaries, correctness checks, and limitations. These tables
support inspection of the reported results; they are not complete run directories
and cannot replace the original inputs and per-application artifacts when rerunning
analysis scripts.
