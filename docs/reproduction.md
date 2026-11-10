# Reproducing the experiments

Run all commands from the repository root. See the [project overview](../README.md)
for installation and the complete pipeline. Historical `results/` paths identify
local runs and are not included in Git; published measurements are in
[benchmarks/](../benchmarks/README.md).

## WordCount behavior

Both implementations convert text to lowercase and extract sequences of Unicode
letters and digits. Punctuation and underscores separate tokens. This is a simple
educational tokenizer rather than a linguistic word-segmentation algorithm.

The Hadoop mapper emits `word<TAB>1`. Hadoop performs shuffle and sorting, then
the reducer sums consecutive counts for each key. The same reducer serves as a
combiner to reduce intermediate output. Spark uses `flatMap`, `map`, and
`reduceByKey`, which also performs local aggregation.

Both engines save the complete result in `part-*` files, using the format
`word<TAB>count`. They do not substitute a top-ten query for a complete output.
`check_wordcount.py` compares outputs independently of record order and reads
them into memory. `_SUCCESS` indicates successful output completion.

## Requirements

- Linux, Python 3.12+, Java 17, and `curl`.
- PySpark 3.5.7, pinned in `requirements.txt`.
- Hadoop 3.5.0, installed by `cluster.py` with archive SHA512 verification.
- Enough free RAM for an 8192 MiB YARN worker allocation plus the OS, control
  services, and application clients. The measured computer has about 24 GiB RAM.

From the project root:

```bash
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Create the virtual environment with `python3 -m venv .venv` only if it does not
already exist. The optional article downloader uses `requirements-download.txt`.
The existing `data/wikipedia.txt` contains approximately 500 MiB of text and
remains the WordCount input. The downloader now saves a separate JSONL corpus
and refuses to overwrite existing output files.

## Install and manage the single-node cluster

Run these commands in a regular Ubuntu terminal:

```bash
python3 scripts/cluster.py setup
python3 scripts/cluster.py smoke
```

`setup` downloads Hadoop once, creates configuration files, initializes a new
HDFS, and starts NameNode, DataNode, ResourceManager, and NodeManager. It uses
direct daemon commands; neither SSH setup nor `sudo` is required. Existing HDFS
metadata is reused, and unknown nonempty storage directories block formatting.

`smoke` uploads a small Unicode fixture to HDFS, runs Hadoop and Spark on YARN,
downloads both outputs, and checks the known answer: five keys and 11 occurrences.
Each attempt has a unique directory under `results/smoke-<ID>/` with per-engine
logs and JSON metrics. Failed runs are not performance results.

```bash
python3 scripts/cluster.py status
python3 scripts/cluster.py stop
python3 scripts/cluster.py start
```

After stopping, use `start` rather than repeating installation. On a partially
failed startup, inspect logs, stop the remaining services, and then retry.

The distribution, HDFS storage, configuration, and logs live in `.runtime/`,
which is excluded from Git. Hadoop scripts need paths without spaces, so a
project-specific symlink under `/tmp/` points to `.runtime/`. Data stays inside
the project. Cluster commands recreate the symlink after a reboot.

Once running, the web interfaces are:

- HDFS NameNode: http://localhost:9870
- YARN ResourceManager: http://localhost:8088

## Configuration locations

`cluster.py` generates these files under `.runtime/conf/`:

| File | Purpose |
|---|---|
| `core-site.xml` | Default filesystem and temporary directory |
| `hdfs-site.xml` | Storage locations, replication factor 1, and service addresses |
| `yarn-site.xml` | NodeManager resources: 4 vcores, 8192 MiB; shuffle service |
| `mapred-site.xml` | General MapReduce container and JVM settings |
| `capacity-scheduler.xml` | One default queue with access to the declared resources |

`.runtime/env.sh` defines Java/Hadoop paths and configuration locations.
`config/wordcount.json` defines the experiment's application resource profile.
`run_wordcount.py` converts it to explicit Hadoop/Spark submission options,
which override general cluster defaults. No separate `spark-defaults.conf` is used.

With the default two workers:

| Setting | Hadoop MapReduce | Spark |
|---|---|---|
| Worker parallelism | Up to 2 maps or 2 reducers at a time | 1 executor, 2 cores |
| Worker memory requests | Up to 2 × 1024 MiB | 1024 MiB heap + 1024 MiB overhead |
| Worker Java heaps | 2 × 512 MiB | 1024 MiB |
| ApplicationMaster | 1024 MiB | 512 MiB heap + 512 MiB overhead |
| Aggregation output | 2 reducers | 2 partitions |
| Speculative execution | Disabled | Disabled |

Reducers start only after all mappers finish, preventing overlap of their worker
budgets. This is an experimental restriction, not the MapReduce default, and can
influence performance. Comparable worker requests do not mean equal total
application usage: the Spark driver runs outside YARN in client mode with a
1024 MiB heap, and the control processes differ. YARN vcores do not pin tasks to
physical CPU cores. Do not run unrelated YARN applications during the experiment.

Both applications set a 64 MiB input-split limit. Spark additionally requests at
least two reading partitions, so actual input partition counts can differ.
Spark logs its count, and Hadoop logs counters. WordCount does not use persistence.

## Submit one application manually

```bash
source .venv/bin/activate
source .runtime/env.sh
```

For local debugging with an existing small input:

```bash
python scripts/run_wordcount.py hadoop --input data/smoke.txt --output results/hadoop-local --metrics results/hadoop-local.json
python scripts/run_wordcount.py spark --input data/smoke.txt --output results/spark-local --metrics results/spark-local.json
python scripts/check_wordcount.py results/hadoop-local results/spark-local
```

For YARN, use `--mode yarn` and full `hdfs://localhost:9000/...` input/output URIs.
The input must already exist in HDFS. `--workers` sets Spark executor cores and
Hadoop task concurrency limits; `--partitions` sets output reducers/partitions.
Existing output paths and metrics files are not overwritten.

Before submission, application sources are copied to a temporary path without
spaces. They remain there until application exit. Spark's installation path and
driver Python are specified explicitly to locate PySpark inside `.venv`.

## Run the WordCount experiment

First check the resource profile on 1 MiB:

```bash
python3 scripts/benchmark.py --sizes-mib 1 --runs 1
```

Then run the full series:

```bash
python3 scripts/benchmark.py
```

Defaults: 10, 100, and 500 **MiB**, three repetitions per engine per size,
18 jobs in total. `--plan` prints the schedule without contacting the cluster.
`--sizes-mib` and `--runs` override the defaults.

Input prefixes end at complete lines and are slightly smaller than the requested
sizes. Actual bytes and SHA256 hashes are recorded. Preparation and HDFS uploads
happen before timing. Engine order alternates. Complete output dictionaries must
match across all runs of the same input size; a mismatch stops the series.

Each `results/wordcount-<date>-<ID>/` directory contains:

- `manifest.json`: versions, hardware, resource profile, source and input hashes.
- Configuration snapshots, input prefixes, complete outputs, logs, and metrics.
- `measurements.csv`: successful, validated runs, updated during execution.
- `summary.csv`: median, mean, minimum, maximum, and sample standard deviation.
- `_SUCCESS`: created only after the entire series completes successfully.

The elapsed time includes process/JVM startup, resource allocation, reading,
shuffle, counting, and writing. Source staging, uploading input, downloading
output, and correctness validation are excluded. A nonzero exit code indicates a
failed run; its duration must not be interpreted as a valid measurement.

The OS cache is not cleared, and no separate warm-up is performed. Each job starts
as a new process, but this does not guarantee cold reads. Three repetitions give
an initial estimate rather than statistical proof. With one repetition, the
reported standard deviation of zero is a placeholder, not evidence of stability.

## Completed results and reports

The full series `wordcount-20261020-214202-2d74ef` completed with all 18 runs
validated. Spark's median time was lower at each tested size:

| Requested input | Hadoop median | Spark median | Hadoop/Spark |
|---|---:|---:|---:|
| 10 MiB | 18.380 s | 13.618 s | 1.350× |
| 100 MiB | 31.332 s | 20.759 s | 1.509× |
| 500 MiB | 98.047 s | 54.926 s | 1.785× |

Materials: [report](../reports/wordcount.md), [PNG chart](../reports/wordcount.png),
[PDF chart](../reports/wordcount.pdf), and [SVG chart](../reports/wordcount.svg).
This is an adapted reproduction using Wikipedia rather than the paper's exact
WordCount datasets and sizes. These results do not establish that Spark is always
faster or that in-memory computation alone explains the difference.

To regenerate the English report and figures without rerunning the experiment:

```bash
python -m pip install -r requirements-report.txt
python scripts/analyze_wordcount.py results/wordcount-20261020-214202-2d74ef
```

Charts use Matplotlib. Bars show medians, error bars show observed min–max ranges,
and points show individual runs. They are not statistical confidence intervals.
Reporting dependencies are not needed by Hadoop/Spark workers.

## Verification and execution environment

```bash
python3 -m unittest discover -s tests -v
python3 -m compileall -q common hadoop spark scripts tests
```

Checks cover the Streaming protocol, combiner behavior, empty text, Unicode,
localized Hadoop file links, output validation, paths containing spaces,
configuration generation, protection of existing HDFS storage, experiment
scheduling, sample boundaries, and statistics. Unit tests do not replace actual
engine execution.

Hadoop setup, smoke tests, and the full benchmark require a terminal with
network access and permission to run Java services. Historical raw logs, input text, numerical measurements, and manifest
hashes are retained as recorded. Unicode test values are preserved through escape
sequences in the English source code. Both experimental series are now complete; see the reports below.

## Official references

- [Hadoop 3.5.0 and Java 17](https://hadoop.apache.org/docs/r3.5.0/index.html)
- [Hadoop single-node setup](https://hadoop.apache.org/docs/r3.5.0/hadoop-project-dist/hadoop-common/SingleCluster.html)
- [Hadoop Streaming](https://hadoop.apache.org/docs/r3.5.0/hadoop-streaming/HadoopStreaming.html)
- [Spark application submission](https://spark.apache.org/docs/3.5.7/submitting-applications.html)

## Download complete articles for K-Means preparation

The old plain-text corpus cannot reliably recover article boundaries. Download
structured records from the same English Wikipedia snapshot instead:

```bash
source .venv/bin/activate
python -m pip install -r requirements-download.txt
python scripts/download_wikipedia.py --articles 1000
```

This streams the `20231101.en` subset of `wikimedia/wikipedia` into
`data/wikipedia_articles.jsonl`. Each physical line is one complete article:

```json
{"id": "123", "title": "Example", "text": "First paragraph\n\nSecond paragraph", "url": "https://en.wikipedia.org/?curid=123"}
```

Article text, including internal newlines, is preserved through JSON escaping.
The original ID, title, and URL are retained for traceability; they are not
numerical features. The first 1000 articles are a prefix in source order, not a
random or representative sample. The downloader does not produce vectors or K-Means results.

Alternatively, download approximately 10 MiB of complete JSONL records:

```bash
python scripts/download_wikipedia.py --size-mib 10 --output data/wikipedia_articles_10MiB.jsonl
```

The final article is never cut, so the saved size may exceed the requested
limit. The limit concerns saved JSONL bytes, not network transfer size: dataset
metadata and Parquet range requests can require additional traffic.

A sidecar `.metadata.json` records the source, resolved dataset commit, number of
articles, exact bytes, selection rule, and SHA256. `--revision` can reuse a recorded
commit. Hugging Face caches stay under `.runtime/huggingface` by default.
Only a completed download is published under the final filename. Failed or
interrupted writes may leave a `.part` file; it is not a completed dataset.
Automatic resume is not implemented. Existing output, metadata, or partial files
are not overwritten; choose a new output path when necessary.

The JSONL writer is tested offline for article boundaries, quotes, Unicode,
size limits, and checksums. Actual downloads need network access. Source schema:
[Wikimedia Wikipedia dataset](https://huggingface.co/datasets/wikimedia/wikipedia).
Streaming behavior:
[Hugging Face Datasets documentation](https://huggingface.co/docs/datasets/stream).

## Prepare shared TF-IDF vectors

The downloaded 1000-article sample has been converted successfully to 1000
nonzero vectors with 1000 features and 243248 stored values. See
[feature preparation report](../reports/vector_preparation.md).

```bash
python -m pip install -r requirements-prepare.txt
python scripts/prepare_vectors.py
```

The current output already exists under `data/kmeans/`; use a new `--output`
directory to repeat preparation. Settings are in `config/tfidf.json`. The script
uses the shared tokenizer, English stop words, `min_df=2`, `max_df=0.9`, at most
1000 features, sublinear TF, smoothed IDF, and L2 normalization. Original article
IDs are preserved. Zero vectors are excluded and recorded for both engines.

The main output is `vectors.jsonl`, containing records with `id`, `size`,
`indices`, and `values`. This sparse format omits zero positions. The vocabulary
and IDF weights, document titles/URLs, excluded IDs, checksums, and local
preparation time are also saved. The preparer loads the selected corpus into RAM;
this is local preprocessing, excluded from the future K-Means engine timer.
The completed K-Means implementation and results are described below.

## Shared K-Means rules and initial centers

`common/kmeans.py` contains validation, sparse-to-dense conversion, squared
Euclidean distance, nearest-center selection, and arithmetic-mean updates.
These are shared numerical rules, not a custom distributed executor.

`initialize_centers.py` selects five distinct nonzero document vectors using
seed 42. Documents are sorted by ID before sampling, making selection independent
of input record order. Identical coordinates are deduplicated. The resulting
`data/kmeans/initial_centers.json` is the common input for both engines.
The file records source IDs, input SHA256, Python version, and numerical rules.

```bash
python3 scripts/initialize_centers.py
# To repeat without replacing the existing initial centers:
python3 scripts/initialize_centers.py --clusters 5 --seed 42 --output data/kmeans/centers-repeat.json
```

Squared Euclidean distance preserves nearest-center ordering without square
roots. Exact ties choose the smallest cluster index. Nonempty clusters use their
arithmetic mean; empty clusters keep their previous center. Centers are not
renormalized, so this is ordinary Euclidean Lloyd K-Means, not spherical K-Means.
The input document vectors remain L2-normalized. Sparse input coordinates and
finite values are validated when loaded.

The shared functions passed a small hand-checkable Lloyd iteration test. All
1000 prepared vectors were assigned successfully to the five initial centers as
an initialization check. Both distributed implementations and their measurements are complete.
The Hadoop and Spark K-Means runners are described below.

## K-Means Hadoop mapper

`hadoop/kmeans_mapper.py` reads sparse vectors from stdin, loads a shared center
file once, precomputes center squared norms, and emits `cluster<TAB>vector JSON`.
The document ID, dimension, indices, and values are preserved. Exact distance
ties select the lowest cluster index. Input records are validated, and malformed
records fail the task with a line number on stderr. Empty partitions emit nothing.
The mapper does not calculate new centers or sort/group its output.

Standalone check from the project root:

```bash
python3 hadoop/kmeans_mapper.py --centers data/kmeans/initial_centers.json < data/kmeans/vectors.jsonl > /tmp/kmeans-assignments.tsv
```

In Hadoop Streaming, the center file will be localized as `centers.json` and
`common/kmeans.py` as `kmeans.py`. The mapper can then run as
`python3 kmeans_mapper.py --centers centers.json`. The reducer and iteration launcher are described below; the standalone mapper command does not run YARN.

Tests cover preserved payloads, nearest-center assignments, exact ties, invalid
dimensions, empty partitions, and localized file links. The real 1000-document
input was checked locally; outputs are in `results/kmeans-mapper-check/`.

## K-Means Hadoop reducer

`hadoop/kmeans_reducer.py` reads sorted `cluster<TAB>vector JSON` records.
It keeps a coordinate sum and document count for one group at a time, then
outputs `cluster<TAB>{"count": ..., "center": [...]}`. The center is the
arithmetic mean without normalization. The final group is emitted at end of input.
Keys must use Hadoop Text's lexical order (for example, `10` precedes `2`).

Both mapper and reducer use the same previous-center file. Empty reducer
partitions emit nothing. Empty clusters have no records; the reducer does not
emit copies of their centers, since multiple reducers could duplicate them.
`common/kmeans.py:merge_center_updates` combines outputs, rejects duplicate
updates, and keeps previous centers for missing clusters. The future iteration
driver will call this after collecting all reducer outputs.

A standalone mapper/sort/reducer protocol check processed all 1000 documents.
The resulting means matched an independent reference and the K-Means objective
(sum of squared nearest-center distances) decreased. Check artifacts are under
`results/kmeans-reducer-check/`. The sorting in this check is test scaffolding;
Hadoop will perform shuffle/sort in the actual distributed job. The Hadoop iteration launcher is now available; the Spark K-Means implementation is described below.

## Run Hadoop K-Means iterations on YARN

`hadoop/kmeans_mapper.py` and `hadoop/kmeans_reducer.py` are connected by
`scripts/run_hadoop_kmeans.py`. Each iteration submits a genuine Hadoop Streaming
job, reads the same HDFS vector dataset, downloads the new centers, merges
updates, restores empty clusters, verifies the total document count, and passes
the result to the next job. Fixed iteration counts are used without early stopping.
No combiner is configured: this reducer computes final means, not mergeable
partial sums. The workers depend only on the Python standard library.

Start with two iterations to verify transfer of updated centers:

```bash
python3 scripts/run_hadoop_kmeans.py --iterations 2
```

HDFS/YARN must already be running (`python3 scripts/cluster.py status`).
The script sets up its own project environment; sourcing `.runtime/env.sh` is not
required. Inputs default to `data/kmeans/vectors.jsonl` and
`data/kmeans/initial_centers.json`. A mismatched source SHA256 is rejected.
Input validation and upload happen before the algorithm timer starts.

Resources are defined separately in `config/kmeans.json`, initially using the
same worker budget as WordCount. Existing result directories are not overwritten.
Each invocation creates a unique `results/kmeans-hadoop-<date>-<ID>/` directory.
HDFS input and intermediate output directories are also unique and retained.

The result directory contains:

- `manifest.json`, source hashes, configuration snapshots, and initial centers;
- an `iteration-NNN.log` and job-command/exit-code JSON for each iteration;
- downloaded reducer outputs and merged center snapshots for each iteration;
- `final_centers.json`, final metrics, and `_SUCCESS` on complete success.

Per-iteration counts describe assignments to the previous centers used in that
iteration, not a separate reassignment to the resulting centers. Empty clusters
have count zero and retain their preceding center. The total count must match
the validated number of input documents before another iteration can start.

`metrics.json` distinguishes raw job times from `total_algorithm_seconds`.
The latter includes all job launches, intermediate center downloads, validation,
merging, and snapshots. Those transfers are part of the iterative Hadoop
algorithm. Input preparation/upload and code staging are excluded. Final
nearest-center objectives are evaluated locally after the timer for a correctness
check; these scans are not engine benchmark measurements. The final objective
must not exceed the initial objective beyond a small numerical tolerance.

Two-iteration state transfer and document-count validation have been tested with
an in-process test harness. That harness is not a YARN benchmark. The actual
launcher still needs verification in the user's normal terminal because the
agent environment prohibits sockets. Do not interpret unit-test execution times
as measured Hadoop performance. The Spark counterpart is described below; the comparative series is not implemented yet.

## Run persisted Spark K-Means

The Spark counterpart is `spark/kmeans_job.py`, launched through
`scripts/run_spark_kmeans.py`:

```bash
python3 scripts/run_spark_kmeans.py --iterations 2
```

The launcher validates the shared vector/center fingerprints and uploads the
input outside timing, then submits one client-mode application to YARN. Resource
requests come from `config/kmeans.json`, the same profile as Hadoop K-Means.
The application parses and validates vectors, persists them with
`MEMORY_AND_DISK`, and materializes the persisted RDD with `count()`. This first
read and persistence are included in the complete application time, not hidden
in a separate untimed warm-up. Spark may spill persisted data to disk; persistence
does not guarantee entirely in-memory execution.

Each iteration broadcasts the current centers, forms partition-local coordinate
sums/counts, combines them with `reduceByKey`, and collects only the small set of
cluster aggregates. Shared nearest-center and update rules preserve empty
clusters and deterministic exact-tie behavior. Partial sums are added before
averaging; partial means are never averaged. Unlike the current Hadoop mapper,
Spark aggregates within each partition, so performance differences include this
implementation difference as well as persistence and job-launch behavior.

Per-iteration center snapshots, counts, a Spark log, internal timing diagnostics,
final HDFS output, final centers, manifests, and metrics are stored under a unique
`results/kmeans-spark-<date>-<ID>/`. `_SUCCESS` appears only after local objective
validation succeeds. Internal iteration/session times exclude parts of startup;
use the wrapper's `total_algorithm_seconds` for comparison. It includes JVM and
session startup, reading/persisting, iterations, writing, shutdown, and downloading
and validating final centers. Local objective scans are outside timing for both
engines.

Once both real runs complete, compare the full iteration history:

```bash
python3 scripts/check_kmeans.py results/<hadoop-run> results/<spark-run>
```

The checker requires identical inputs, initial centers, iteration count, resource
profile, cluster count, and dimensions. Cluster document counts must match exactly;
center coordinates allow relative tolerance 1e-9 and absolute tolerance 1e-10 for
floating-point summation order. The checker does not reorder clusters: identical
initial center labels are retained.

Hadoop's two-iteration YARN run completed successfully in
`results/kmeans-hadoop-20261020-224846-1794b6/`. Spark's partition arithmetic was
checked locally against both saved Hadoop iterations. Both actual YARN runners
were subsequently verified and the complete comparative series is summarized below.

## Completed K-Means experiment

The series `results/kmeans-series-0x5T5dCJ/` completed 24 applications:
1, 3, 5, and 10 fixed iterations, three repetitions for both engines. Full
per-iteration center matrices and cluster counts were rechecked between engines
and across repetitions. The largest observed coordinate difference is approximately
7e-17. All runs used the same 1000 documents, 1000 features, five initial centers,
and resource profile.

| Iterations | Hadoop median, s | Spark median, s | Hadoop/Spark |
|---:|---:|---:|---:|
| 1 | 17.729 | 14.494 | 1.223× |
| 3 | 55.308 | 14.639 | 3.778× |
| 5 | 89.993 | 15.539 | 5.791× |
| 10 | 181.219 | 17.158 | 10.562× |

Materials: [English K-Means report](../reports/kmeans.md),
[PNG chart](../reports/kmeans.png), [PDF chart](../reports/kmeans.pdf),
and [SVG chart](../reports/kmeans.svg). Raw and summary CSV files and pairwise
verification details are saved in the series directory.

To regenerate without rerunning either engine:

```bash
python scripts/analyze_kmeans.py results/kmeans-series-0x5T5dCJ
```

The graph shows total algorithm times, including initial loading/startup, not
Spark's internal iteration timers. The speedup reflects this small single-node
workload and implementation differences, including partition-local aggregation
in Spark but no Hadoop combiner. It cannot be attributed solely to memory caching
or generalized to arbitrary workloads. See the report for full limitations.

## Final English report and presentation

The supplied 13-page presentation was used as a structure/style reference. The
original Ketu et al. PDF was checked locally for experiment details and figure
values. The resulting documents distinguish the paper's results from this
project's adapted reproduction:

- [Final report, A4 PDF](../reports/final_report.pdf), seven pages.
- [Final report source, Markdown](../reports/final_report.md).
- [Presentation PDF](../reports/presentation.pdf), 17 landscape slides.

Title pages identify Alikhan Sagymbekov, group IFMU-5, supervised by
Assoc. Prof. Germanas Budnikas. Regenerate all three files with:

```bash
python scripts/build_final_pdf.py --student "Full name" --group "Group" --supervisor "Supervisor name"
```

This only renders recorded results; it does not rerun the experiments. It requires
the Matplotlib dependency from `requirements-report.txt`. The PDFs embed their
fonts and include selectable text. The original GraphQL topic, other student's
identity, and faculty logo were not copied. The structural template does not imply
author approval or exact reproduction of the source paper's setup.

The presentation now includes four dedicated **Own implementation** slides
(12–15): actual WordCount source, Hadoop K-Means mapper/reducer, Spark persistence
and iteration aggregation, and numerical output verification with run commands.
Snippets are extracted from project source files with original line numbers;
long lines are wrapped for display. The final two slides cover limitations and
conclusions. Regenerating the presentation refreshes these excerpts automatically.
