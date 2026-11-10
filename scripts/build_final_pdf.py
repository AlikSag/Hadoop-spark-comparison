"""Build an English A4 report and a 17-slide PDF from verified experiment data."""
import argparse
import csv
import json
import os
from pathlib import Path
import re
import textwrap

os.environ.setdefault('MPLCONFIGDIR', '/tmp/hscompare-matplotlib')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.patches import Rectangle

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / 'reports'
WORDCOUNT = ROOT / 'results/wordcount-20261020-214202-2d74ef'
KMEANS = ROOT / 'results/kmeans-series-0x5T5dCJ'
DARK = '#292638'
ACCENT = '#d30086'
BLUE = '#33658a'
ORANGE = '#e58023'


def read_csv(path):
    with path.open(newline='') as stream:
        return list(csv.DictReader(stream))


def result_table(rows, field, label):
    lines = [f'| {label} | Hadoop median, s | Spark median, s | Hadoop/Spark |',
             '|---:|---:|---:|---:|']
    for value in sorted({int(row[field]) for row in rows}):
        h, s = [next(row for row in rows if int(row[field]) == value and row['engine'] == engine)
                for engine in ('hadoop', 'spark')]
        lines.append(f"| {value} | {float(h['median_seconds']):.3f} | {float(s['median_seconds']):.3f} | "
                     f"{float(h['hadoop_over_spark_median']):.3f}× |")
    return '\n'.join(lines)


def build_markdown(args, wc, km):
    return f'''# Performance Analysis of Hadoop MapReduce and Apache Spark

## Scientific paper analysis and adapted experimental reproduction

Course: T120M337 Distributed Systems and Algorithms

Student: {args.student}

Group: {args.group}

Supervisor: {args.supervisor}

Experiment date: 20 October 2026

## Abstract

This work analyzes Ketu, Mishra, and Agarwal (2020) and adapts two of its experiments
using real Hadoop Streaming and PySpark on a single-node HDFS/YARN environment.
WordCount was evaluated on 10, 100, and 500 MiB of English Wikipedia text.
Lloyd K-Means was evaluated on 1000 Wikipedia article vectors with 1, 3, 5, and
10 fixed iterations. Every condition was repeated three times. Complete
WordCount outputs and all K-Means iteration centers and counts were validated.
Spark had lower median time in both series. The observed Hadoop/Spark ratios
were 1.35–1.79 for WordCount and 1.22–10.56 for K-Means. These results describe
this hardware, workload, and implementation; they are not universal speedup claims.

## 1. Source paper and research scope

Source: Shwet Ketu, Pramod Kumar Mishra, and Sonali Agarwal. Performance Analysis
of Distributed Computing Frameworks for Big Data Analytics: Hadoop Vs Spark.
Computación y Sistemas, 24(2), 669–686, 2020. DOI: 10.13053/CyS-24-2-3401 [1].

The paper combines a qualitative comparison of Hadoop MapReduce and Spark with
experimental evaluation of WordCount, logistic regression, and distributed K-Means.
The present project selects WordCount (§5.1) and iterative K-Means (§5.3).
It reproduces the experimental ideas, not the authors' exact setup or numerical results.
No logistic regression experiment is claimed.

The questions below organize this reproduction; they are not quotations of formally
numbered research questions from the paper:

- RQ1: How do the frameworks execute the same data-processing task?
- RQ2: How does WordCount time change with input size?
- RQ3: How does K-Means time change with iteration count?
- RQ4: Do both implementations produce equivalent numerical results?
- RQ5: Which configuration and implementation differences limit the conclusions?

## 2. Original experiments versus this reproduction

The paper's WordCount Figures 8–9 compare Wikilinks and Wikilanguage at sizes
labelled 500 MB through 40 GB. Its surrounding prose lists inconsistent size
ranges, so figure labels are the reference here. Iterative K-Means Figures 14–15
use four corpora of 5 GB with one to three iterations; Figure 16 uses a 40 GB
corpus with 1, 2, 3, 5, 7, and 9 iterations.

This reproduction uses 10, 100, and 500 MiB of Wikipedia for WordCount and a
1000-document Wikipedia subset for K-Means. Our K-Means features, cluster count,
initialization seed, and timing boundaries are explicitly recorded. The paper does
not provide enough of these details to reconstruct its text-clustering pipeline
exactly. The text refers to hardware in Table 14, but that table contains framework
key features rather than a complete hardware specification.

Figure 8 reports Hadoop/Spark times of 102/54 s at 500 MB and 1762/1630 s at 40 GB.
Thus Spark's relative advantage narrows across those endpoints. Our WordCount
advantage grows with size. Both studies rank Spark faster on the tested inputs,
but the scaling trends and absolute times are not the same. MB/GB labels in the
paper must not be silently treated as the precise MiB units used here.

Figure 16 labels the first-iteration on-disk/in-memory times as 1721/1821 s and
nine-iteration times as 15489/1981 s. The figure therefore illustrates an increasing
iterative advantage, with an initially slower in-memory case. Our one-iteration
Spark run is already faster. These are qualitative comparisons, not matched benchmarks.

## 3. Implementation and architecture

The implementation uses Python application logic with the actual engines.
Hadoop Streaming launches Python mapper/reducer subprocesses and performs
partitioning, shuffle, sorting, and task scheduling through Hadoop MapReduce.
PySpark expresses processing through RDD transformations and actions.

HDFS provides common storage. YARN allocates resources for both frameworks.
NameNode, DataNode, ResourceManager, and NodeManager run as separate JVM
processes on one computer. This is pseudo-distributed operation, not a multi-node cluster.

The supplied example presentation was used only as a structural guide: research
questions, fundamentals, study design, results, own implementation, and conclusions.
Its GraphQL subject, author details, and faculty logo were not copied into this work.

## 4. Hardware, software, and resources

CPU: Intel Core i5-10300H at 2.50 GHz. Linux reports approximately 23.3 GiB of usable
RAM (MemTotal: 24436484 KiB; approximately 24 GB installed). Python 3.12.3,
Java 17.0.20.1, Hadoop 3.5.0, Spark 3.5.7, and scikit-learn 1.9.1 were used.
Detailed version strings and Linux platform information are preserved in manifests.

The YARN worker advertises 4 vcores and 8192 MiB. Applications are run sequentially.
The experimental worker budget is up to 2 vcores and 2048 MiB: Hadoop allows
up to two map or two reduce tasks, each requesting 1024 MiB with a 512 MiB Java heap.
Spark requests one two-core executor with a 1024 MiB heap and 1024 MiB overhead.
ApplicationMasters request 1024 MiB. Spark's client-mode driver runs outside YARN
with a 1024 MiB heap. Speculative execution and Spark dynamic allocation are disabled.

These are comparable worker resource requests, not equal total application memory
or strict CPU isolation. Hadoop reducers start only after all maps finish; this
experimental restriction can affect performance. Both pipelines use two output
aggregation partitions and a 64 MiB input-split limit. Actual reading partitions
can differ. Configurations are in config/wordcount.json and config/kmeans.json;
XML and code snapshots are recorded with runs.

## 5. Experiment 1: WordCount

The input consists of prefixes of the existing English Wikipedia corpus. Each
prefix ends on a complete line; exact bytes and hashes are recorded. Both systems
use the same lowercase Unicode-letter/digit tokenizer, with punctuation and
underscores separating tokens.

Hadoop emits word/count pairs, optionally combines local counts, and reduces by
word. Spark uses flatMap, map, and reduceByKey. Both save complete dictionaries
to HDFS. No top-ten operation substitutes for a full result and no persistence is used.

Each size was measured three times per engine: 18 applications. Engine order
alternated. Timing includes application startup, reading, counting, shuffle, and
writing. Preparing and uploading inputs, downloading outputs, and correctness
checks are excluded. No separate warm-up or OS cache reset was performed.

{result_table(wc, 'requested_mib', 'Requested input, MiB')}

![WordCount timing and speedup](wordcount.png)

Full output dictionaries matched across all runs at each size. At 500 MiB, each
result contained 969289 unique tokens and 84735062 token occurrences.
Observed ranges, individual measurements, means, and standard deviations are
available in the experiment CSV files and the detailed WordCount report.

## 6. Document preparation for K-Means

A separate JSONL download preserves one complete article per record, including
its original ID, title, text, and URL. The 1000 articles are a prefix in source order,
not a random or representative sample. The dataset snapshot is 20231101.en;
its resolved repository commit and source checksum are recorded.

Only text is vectorized. TfidfVectorizer uses the shared tokenizer, English stop
words, min_df=2, max_df=0.9, at most 1000 features, sublinear term frequency,
smoothed IDF, and L2 row normalization. Each feature index has the same meaning
for every document and for both engines. IDs and titles are not numerical features.

The transform uses TF=1+ln(count), IDF=ln((1+N)/(1+df))+1, followed by division
by the vector's Euclidean norm. All 1000 documents produced nonzero 1000-dimensional
vectors, containing 243248 stored values. Sparse JSONL stores only nonzero positions.
The vocabulary, IDF weights, document links, checksums, and preprocessing time are saved.
Preparation ran locally in about 2.7 seconds and is excluded from engine timings.

## 7. Experiment 2: iterative Lloyd K-Means

Five distinct document vectors are sampled with seed 42 as initial centers.
Both engines read the same vectors and exact center file. Squared Euclidean
distance determines assignments. Exact ties select the lowest center index.
Each new center is the arithmetic mean; centers are not renormalized.
An empty cluster retains its previous center. Early stopping is disabled.

Hadoop submits a new Streaming job per iteration. The mapper assigns documents,
Hadoop shuffles them by cluster, and the reducer computes means. The launcher
downloads updates, verifies document counts, merges empty clusters, and passes
the new centers to the next job. This implementation has no K-Means combiner.

Spark runs one application, materializes a MEMORY_AND_DISK persisted RDD,
broadcasts centers, aggregates partition-local sums/counts, reduces by cluster,
and collects only the small center aggregates. Initial loading and persistence
are included; persistence may spill to disk rather than being purely in memory.

There are 1, 3, 5, and 10 fixed iterations, with three repetitions per engine:
24 applications, including 57 Hadoop iteration jobs and 12 Spark applications.
Hadoop ran first in repetitions 1 and 3; Spark ran first in repetition 2.
Order alternates but is not perfectly balanced.

Timing includes startup and initial reading. Hadoop includes intermediate center
downloads, merging, and snapshots. Spark includes shutdown and final-center
writing/download/validation. Input preprocessing, validation/upload, and source
staging are excluded. Final local objective scans are outside the engine timer.
Internal Spark iteration timers are diagnostics, not the comparison metric.

{result_table(km, 'iterations', 'Fixed iterations')}

![K-Means timing and speedup](kmeans.png)

Every iteration's full center matrix and counts match between engines and across
repetitions. Counts match exactly; coordinate checks use relative tolerance 1e-9
and absolute tolerance 1e-10. The largest observed difference is about 7e-17.
The objective falls from 1346.435812 initially to 722.709028 after ten iterations.
This is a consistency check, not an external semantic-quality evaluation.

## 8. Discussion and threats to validity

Spark had lower median complete time in both series. K-Means shows a much
stronger relative advantage as iterations increase. Reusing an application and
persisted vectors avoids repeated Hadoop job launches and repeated input processing.
On this small sample, startup and coordination form a substantial part of total time.

The experiment does not isolate caching as a cause. Spark uses partition-local
aggregation whereas Hadoop K-Means shuffles complete sparse vectors without a
combiner. Serialization, Python subprocesses, shuffle, task partitions, center
transfers, and scheduling differ. A separate ablation study would be needed to
quantify each contribution.

Only one machine, one corpus prefix, and three repetitions were tested. OS caches
were not reset and execution order was not randomized. Resource requests are
not identical total resource usage. Peak RAM, CPU, disk traffic, and multi-node
scalability were not measured. The small K-Means sample does not reproduce the
paper's multi-gigabyte scale, and no class labels assess clustering quality.
Observed min–max ranges are not confidence intervals.

Some broad claims in the 2020 paper should not be repeated as universal rules.
Our Hadoop logic is Python through Streaming, so Hadoop is not Java-only from an
application developer's perspective. Spark can spill to disk and does not keep
all computations exclusively in RAM. Security, cost, and failure tolerance were
not empirically compared in this project.

## 9. Reproducibility and project deliverables

The completed source series are:

- WordCount: results/wordcount-20261020-214202-2d74ef/
- K-Means: results/kmeans-series-0x5T5dCJ/

They retain per-run logs, metrics, manifests, input/initialization hashes,
configuration snapshots, and validation artifacts. Summary CSV files contain
medians, means, ranges, and sample standard deviations. The README documents
installation, smoke checks, submission, and analysis commands. The code has 34
passing checks; actual engine experiments ran in the user's regular terminal.

Regenerate figures without rerunning the experiments:

```bash
python scripts/analyze_wordcount.py results/wordcount-20261020-214202-2d74ef
python scripts/analyze_kmeans.py results/kmeans-series-0x5T5dCJ
```

## 10. Conclusions

Two experimental ideas from the paper were implemented using real Hadoop and
Spark rather than a custom engine simulation. Identical results were verified.
WordCount median speedups range from 1.35 to 1.79; K-Means median speedups range
from 1.22 to 10.56 under the recorded conditions.

The iterative result supports the practical benefit of reusing an application and
persisted inputs, while the measurements do not attribute all differences solely
to memory caching. Future work should add larger, representative corpora, more
repetitions, Hadoop local aggregation, controlled ablations, resource measurements,
and a multi-node environment.

## References

[1] Ketu, S., Mishra, P. K., and Agarwal, S. (2020). Performance Analysis of Distributed
Computing Frameworks for Big Data Analytics: Hadoop Vs Spark. Computación y Sistemas,
24(2), 669–686. https://doi.org/10.13053/CyS-24-2-3401

[2] Apache Hadoop 3.5.0: Single Node Setup and Hadoop Streaming documentation.
https://hadoop.apache.org/docs/r3.5.0/

[3] Apache Spark 3.5.7: configuration, RDD programming, and YARN documentation.
https://spark.apache.org/docs/3.5.7/

[4] scikit-learn: TfidfVectorizer documentation.
https://scikit-learn.org/stable/modules/generated/sklearn.feature_extraction.text.TfidfVectorizer.html

[5] Wikimedia Wikipedia dataset, 20231101.en; resolved source revision is recorded
in the download metadata. https://huggingface.co/datasets/wikimedia/wikipedia

[6] Denys Krysanov, Migrating to GraphQL: A Practical Assessment, course presentation
provided by the user, 12 November 2025. Used only as a presentation structure example.
'''


def plain(text):
    text = re.sub(r'\[([^]]+)\]\([^)]+\)', r'\1', text)
    return text.replace('**', '').replace('`', '')


class A4Report:
    def __init__(self, pdf):
        self.pdf = pdf
        self.page = 0
        self.fig = None
        self.y = 0
        self.width, self.height = 595, 842
        self.new_page()

    def new_page(self):
        if self.fig is not None:
            self.pdf.savefig(self.fig)
            plt.close(self.fig)
        self.page += 1
        self.fig = plt.figure(figsize=(self.width/72, self.height/72), facecolor='white')
        self.fig.text(48/self.width, 1-31/self.height, 'HADOOP vs SPARK  |  ADAPTED REPRODUCTION',
                      fontsize=8, color=BLUE)
        self.fig.text(0.92, 0.035, str(self.page), fontsize=9, ha='right', color='#666666')
        self.y = 67

    def ensure(self, height):
        if self.y + height > self.height - 55:
            self.new_page()

    def text(self, text, size=10.5, color='#222222', bold=False, gap=7, indent=0):
        # Conservative wrapping also accommodates long paths and URLs.
        limit = int((self.width-96-indent)/(size*0.55))
        lines = textwrap.wrap(plain(text), width=limit, break_long_words=True, break_on_hyphens=False) or ['']
        line_height = size*1.43
        for line in lines:
            self.ensure(line_height)
            self.fig.text((48+indent)/self.width, 1-self.y/self.height, line,
                          fontsize=size, color=color, weight='bold' if bold else 'normal', va='top')
            self.y += line_height
        self.y += gap

    def table(self, lines):
        rows = [[plain(cell.strip()) for cell in line.strip().strip('|').split('|')]
                for line in lines if not re.match(r'^\|[\s:|\-]+\|$', line)]
        columns = len(rows[0])
        width = (self.width-96)/columns
        for index, row in enumerate(rows):
            wrapped = [textwrap.wrap(cell, width=max(10,int(width/5.1))) or [''] for cell in row]
            height = max(map(len, wrapped))*13+14
            self.ensure(height)
            for column, cell in enumerate(wrapped):
                x = 48+column*width
                self.fig.patches.append(Rectangle((x/self.width,1-(self.y+height)/self.height),
                    width/self.width,height/self.height,transform=self.fig.transFigure,
                    facecolor='#e9eff3' if index==0 else 'white',edgecolor='#c5cbd0',linewidth=.5))
                self.fig.text((x+5)/self.width,1-(self.y+6)/self.height,'\n'.join(cell),
                    fontsize=9,weight='bold' if index==0 else 'normal',va='top',linespacing=1.4)
            self.y += height
        self.y += 12

    def image(self, name):
        path = REPORTS/name
        image = plt.imread(path)
        width = self.width-96
        height = width*image.shape[0]/image.shape[1]
        self.ensure(height+24)
        ax = self.fig.add_axes([48/self.width,1-(self.y+height)/self.height,width/self.width,height/self.height])
        ax.imshow(image)
        ax.axis('off')
        self.y += height+18

    def close(self):
        self.pdf.savefig(self.fig)
        plt.close(self.fig)


def render_report(markdown, author):
    lines = markdown.splitlines()
    with PdfPages(REPORTS/'final_report.pdf', metadata={'Title':'Hadoop vs Spark — Scientific Paper Analysis',
                                                     'Author':author}) as pdf:
        layout = A4Report(pdf)
        index = 0
        while index < len(lines):
            line = lines[index].strip()
            if not line:
                index += 1
                continue
            if line.startswith('!['):
                layout.image(re.search(r'\(([^)]+)\)',line)[1])
            elif line.startswith('|'):
                table=[]
                while index<len(lines) and lines[index].strip().startswith('|'):
                    table.append(lines[index].strip()); index+=1
                layout.table(table)
                continue
            elif line.startswith('# '):
                layout.text(line[2:],size=23,color=DARK,bold=True,gap=20)
            elif line.startswith('## '):
                layout.ensure(76)
                layout.text(line[3:],size=14,color=BLUE,bold=True,gap=12)
            elif line.startswith('```'):
                index+=1
                while index<len(lines) and not lines[index].startswith('```'):
                    layout.text(lines[index],size=9,gap=2)
                    index+=1
                layout.y+=8
            elif line.startswith('- '):
                layout.text('• '+line[2:],indent=8)
            else:
                paragraph=[line]
                while index+1<len(lines) and lines[index+1].strip() and not lines[index+1].startswith(('#','|','![','```','- ')):
                    index+=1;paragraph.append(lines[index].strip())
                layout.text(' '.join(paragraph))
            index+=1
        layout.close()


def slide_base(number, title, source='Own implementation and recorded experiments'):
    fig=plt.figure(figsize=(13.333333,7.5),facecolor='white')
    fig.patches.append(Rectangle((0,.90),1,.10,transform=fig.transFigure,facecolor=DARK,edgecolor='none'))
    fig.text(.035,.94,title,fontsize=23,color='white',weight='bold',va='center')
    fig.text(.035,.038,source,fontsize=9,color='#666666')
    fig.text(.965,.035,str(number),fontsize=11,color=DARK,ha='right')
    return fig


def bullets(fig, items, x=.055, y=.83, width=77, size=21, spacing=.125):
    for item in items:
        lines=textwrap.wrap(item,width=width,break_long_words=False)
        fig.text(x,y,'• '+('\n  '.join(lines)),fontsize=size,color='#222222',va='top',linespacing=1.35)
        y-=max(spacing,len(lines)*.055+.035)
    return y


def image_slide(pdf, number, title, name, caption):
    fig=slide_base(number,title)
    ax=fig.add_axes([.025,.16,.95,.72]); ax.imshow(plt.imread(REPORTS/name));ax.axis('off')
    fig.text(.055,.105,caption,fontsize=14,color=DARK)
    pdf.savefig(fig);plt.close(fig)


def source_excerpt(path, sections):
    lines = (ROOT / path).read_text().splitlines()
    output = []
    for section, (start_marker, end_marker) in enumerate(sections):
        start = next(i for i, line in enumerate(lines) if start_marker in line)
        end = next(i for i in range(start + 1, len(lines)) if end_marker in lines[i])
        if section:
            output.append("     ...")
        block = textwrap.dedent("\n".join(lines[start:end])).splitlines()
        for number, line in enumerate(block, start + 1):
            parts = textwrap.wrap(line, width=59, subsequent_indent="    ",
                                  replace_whitespace=False, drop_whitespace=False) or [""]
            output.append(f"{number:3d}  {parts[0]}")
            output.extend("     " + part for part in parts[1:])
    return "\n".join(output).rstrip()


def code_panel(fig, title, code, x, width=.435):
    lines = code.splitlines()
    if len(lines) > 29:
        raise ValueError(f"Code excerpt is too long for a readable slide: {title} ({len(lines)} lines)")
    fig.text(x,.837,title,fontsize=12.5,color=BLUE,weight='bold',va='top')
    fig.patches.append(Rectangle((x-.012,.19),width,.60,transform=fig.transFigure,
                                 facecolor='#f2f4f7',edgecolor='#d9dfe5',linewidth=.7))
    fig.text(x,.768,code,fontsize=10.0,fontfamily='DejaVu Sans Mono',
             color='#202433',va='top',linespacing=1.25)


def implementation_slides(pdf):
    fig=slide_base(12,'Own implementation: WordCount source code',
                   'Verbatim source excerpts with line numbers; long lines wrapped')
    left=source_excerpt('hadoop/mapper.py', [('def main():', 'if __name__')])
    left+='\n\n# hadoop/reducer.py\n'+source_excerpt('hadoop/reducer.py', [('def main():','if __name__')])
    right=source_excerpt('spark/wordcount.py', [('lines = sc.textFile','finally:')])
    code_panel(fig,'hadoop/mapper.py + reducer.py',left,.045)
    code_panel(fig,'spark/wordcount.py',right,.535)
    fig.text(.045,.135,'Our Python counts words; Hadoop performs shuffle/sort. Spark expresses the same task through its RDD API.',
             fontsize=13,color=DARK)
    pdf.savefig(fig);plt.close(fig)

    fig=slide_base(13,'Own implementation: Hadoop K-Means',
                   'Actual mapper/reducer functions; full files are included in the project')
    left=source_excerpt('hadoop/kmeans_mapper.py', [('norms = [math.fsum','def main():')])
    right=source_excerpt('hadoop/kmeans_reducer.py', [('def emit():','for line_number'),
                                                       ('if total is None:','except (ValueError')])
    code_panel(fig,'hadoop/kmeans_mapper.py',left,.045)
    code_panel(fig,'hadoop/kmeans_reducer.py',right,.535)
    fig.text(.045,.135,'Assign to a cluster → Hadoop groups vectors → reducer calculates the coordinate mean.',fontsize=14,color=DARK)
    pdf.savefig(fig);plt.close(fig)

    fig=slide_base(14,'Own implementation: persisted Spark K-Means',
                   'spark/kmeans_job.py; common/kmeans.py holds shared numerical rules')
    left=source_excerpt('spark/kmeans_job.py', [('vectors = (sc.textFile','if count != args.documents:')])
    left+='\n\n'+source_excerpt('spark/kmeans_job.py', [('broadcast = sc.broadcast','sums = [[0.0]')])
    code_panel(fig,'spark/kmeans_job.py',left,.045)
    fig.text(.535,.83,'What this code actually does',fontsize=20,color=BLUE,weight='bold')
    bullets(fig,[
        'Parse, validate, and persist input vectors.',
        'count() includes cache materialization.',
        'Broadcast current centers to workers.',
        'Combine sums/counts with reduceByKey.',
        'Collect only small center aggregates.',
        'Release the broadcast after each action.'
    ],x=.535,y=.75,width=44,size=16,spacing=.09)
    fig.text(.045,.135,'One application, fixed iterations; initial loading/persistence is included, not an untimed warm-up.',fontsize=14,color=DARK)
    pdf.savefig(fig);plt.close(fig)

    fig=slide_base(15,'Own implementation: execution and verification',
                   'Recorded artifacts, source commands, and per-iteration numerical checks')
    left=source_excerpt('scripts/check_kmeans.py', [("if states[0]['counts']",'for state in states:'),
                                                  ("for a, b in zip(states[0]['centers']",'return {')])
    code_panel(fig,'scripts/check_kmeans.py',left,.045)
    fig.text(.535,.83,'Executable evidence',fontsize=20,color=BLUE,weight='bold')
    fig.text(.535,.75,'python scripts/run_hadoop_kmeans.py --iterations 2\n\n'
             'python scripts/run_spark_kmeans.py --iterations 2\n\n'
             'python scripts/check_kmeans.py\n  <hadoop-output> <spark-output>',
             fontsize=12,fontfamily='DejaVu Sans Mono',va='top',linespacing=1.45)
    fig.text(.535,.43,'Retained for every run:',fontsize=18,color=BLUE,weight='bold')
    bullets(fig,['manifest.json + source/input hashes','Per-iteration centers and counts','Engine logs, timings, and _SUCCESS'],
            x=.535,y=.37,width=36,size=16,spacing=.065)
    fig.text(.045,.105,'34 automated checks passed. All 18 WordCount and 24 K-Means applications completed and were validated.',
             fontsize=13,color=DARK)
    pdf.savefig(fig);plt.close(fig)


def render_slides(args):
    with PdfPages(REPORTS/'presentation.pdf',metadata={'Title':'Hadoop vs Spark — Scientific Paper Analysis',
                                                    'Author':args.student}) as pdf:
        fig=plt.figure(figsize=(13.333333,7.5),facecolor='white')
        fig.text(.5,.86,'HADOOP  vs  SPARK',fontsize=42,color=BLUE,ha='center',weight='bold')
        fig.patches.append(Rectangle((0,.34),1,.37,transform=fig.transFigure,facecolor=DARK))
        fig.text(.05,.65,'T120M337 Distributed Systems and Algorithms',fontsize=19,color='white')
        fig.text(.05,.55,'Performance Analysis of Distributed Computing\nFrameworks for Big Data Analytics: Hadoop vs Spark',
                 fontsize=26,color='#ff51ba',weight='bold',va='center',linespacing=1.5)
        fig.text(.95,.38,'Shwet Ketu · Pramod Kumar Mishra · Sonali Agarwal\nComputación y Sistemas 24(2), 2020 · DOI: 10.13053/CyS-24-2-3401',
                 fontsize=13,color='white',ha='right')
        fig.text(.95,.23,f'Student: {args.student}\nGroup: {args.group}\nSupervisor: {args.supervisor}',
                 fontsize=18,ha='right',va='top',linespacing=1.5)
        fig.text(.05,.06,'Scientific paper analysis + two adapted experiments',fontsize=14,color=BLUE)
        pdf.savefig(fig);plt.close(fig)
        slides=[
        (2,'Paper scope and selected reproduction',[
         'The paper compares execution models, capabilities, and performance.',
         'Original experiments: WordCount, logistic regression, and K-Means.',
         'Selected here: WordCount input-size scaling and iterative K-Means.',
         'Real Hadoop Streaming and PySpark; one computer with HDFS/YARN.',
         'Adapted datasets and sizes; no claim of exact numerical replication.'
         ],'Ketu et al. (2020), §§4–5; own reproduction scope'),
        (3,'Research questions for this reproduction',[
         'RQ1: How do Hadoop MapReduce and Spark execute the same task?',
         'RQ2: How does WordCount time change with input size?',
         'RQ3: How does K-Means time change with iteration count?',
         'RQ4: Do the computed outputs and centers agree?',
         'RQ5: Which settings and implementation differences limit our conclusions?'
         ],'Questions formulated for this project, not verbatim questions from the paper'),
        (4,'Framework fundamentals',[
         'HDFS stores inputs and outputs; YARN allocates task resources.',
         'Hadoop: Python mapper → Hadoop shuffle/sort → Python reducer.',
         'Spark: RDD transformations form a plan; actions execute it.',
         'Hadoop K-Means launches a new job for every iteration.',
         'Spark K-Means reuses one application and a persisted RDD; spilling is possible.'
         ],'Hadoop/Spark documentation; own implementation'),
        (5,'Original experimental design',[
         'WordCount: Wikilinks and Wikilanguage; figure sizes 500 MB–40 GB.',
         'K-Means: four 5 GB corpora at 1–3 iterations; a 40 GB corpus at up to 9.',
         'Figure 8: WordCount advantage narrows with size (102/54 → 1762/1630 s).',
         'Figure 16: iterative advantage grows; first in-memory iteration is slower.',
         'Our data, feature pipeline, versions, and scale differ from the paper.'
         ],'Ketu et al. (2020), Fig. 8–9 and 14–16; values shown as Hadoop/Spark'),
        (6,'Own environment and controlled resource requests',[
         'Intel Core i5-10300H; ~23.3 GiB usable RAM; Linux; one node.',
         'Python 3.12.3 · Java 17 · Hadoop 3.5.0 · Spark 3.5.7.',
         'Workers: up to 2 vcores and 2048 MiB requested by each engine.',
         'Hadoop: up to 2 tasks × 1024 MiB; Spark: one two-core executor.',
         'Same HDFS inputs; 3 repetitions; recorded configurations and checksums.',
         'Worker requests are comparable; total process resources are not identical.'
         ],'Recorded manifests; config/wordcount.json and config/kmeans.json'),
        (7,'Study design: WordCount',[
         'Wikipedia prefixes: 10, 100, and 500 MiB; three repeats per engine.',
         'One shared tokenizer: lowercase Unicode letters/digits.',
         'Count every token and save the complete dictionary to HDFS.',
         'Timer includes startup, reading, shuffle, processing, and writing.',
         'Input preparation/upload and output verification are outside timing.',
         '18 applications; engine order alternated; no persistence or cache reset.'
         ],'Own implementation: hadoop/mapper.py, reducer.py; spark/wordcount.py'),
        ]
        for number,title,items,source in slides:
            fig=slide_base(number,title,source);bullets(fig,items,size=20,spacing=.115)
            pdf.savefig(fig);plt.close(fig)
        image_slide(pdf,8,'WordCount results', 'wordcount.png',
                    'All complete outputs matched. Median speedup: 1.35× → 1.79× in this configuration.')
        for number,title,items,source in [
        (9,'Own implementation: text → shared vectors',[
         '1000 JSONL articles preserve IDs, titles, URLs, and document boundaries.',
         'TF-IDF: English stop words; min_df=2; max_df=0.9; 1000 features.',
         'Sublinear TF, smoothed IDF, and L2 normalization; no zero vectors.',
         'One sparse vector file is shared by Hadoop and Spark.',
         '5 distinct initial centers; seed 42; preprocessing excluded from timing.'
         ],'scripts/prepare_vectors.py; config/tfidf.json; initial_centers.json'),
        (10,'Study design: iterative Lloyd K-Means',[
         'Assign documents to nearest centers, then recompute arithmetic means.',
         'Same initial centers; exact ties use lowest index; empty clusters keep centers.',
         'Fixed 1, 3, 5, and 10 iterations; no early stopping; three repeats.',
         'Hadoop: new job and center transfer each iteration; no combiner.',
         'Spark: materialize MEMORY_AND_DISK once; aggregate partition sums.',
         '24 applications; total time includes initial loading and startup.'
         ],'Own Hadoop/Spark runners; common/kmeans.py'),
        ]:
            fig=slide_base(number,title,source);bullets(fig,items,size=20,spacing=.115)
            pdf.savefig(fig);plt.close(fig)
        image_slide(pdf,11,'K-Means results', 'kmeans.png',
                    'At 10 iterations: 181.219 s vs 17.158 s; median ratio 10.56×. Three repeats per condition.')
        implementation_slides(pdf)
        fig=slide_base(16,'Correctness, interpretation, and limitations')
        bullets(fig,[
         'All K-Means iteration centers/counts agree; coordinate differences ~7e-17.',
         'Objective decreases: 1346.436 initially → 722.709 after ten iterations.',
         'Repeated jobs grow Hadoop time; Spark reuses its application and vectors.',
         'Not a cache-only test: aggregation, startup, shuffle, and scheduling differ.',
         'Small corpus; one node; three repeats; no RAM/CPU profiling or quality labels.',
         'WordCount ranking agrees with the paper; its size-scaling trend does not.'
        ],size=19,spacing=.115)
        pdf.savefig(fig);plt.close(fig)
        fig=slide_base(17,'Conclusions and references')
        bullets(fig,[
         'Two paper-inspired experiments completed using real distributed frameworks.',
         'WordCount median speedup: 1.35–1.79×; K-Means: 1.22–10.56×.',
         'Results validated; source code, commands, manifests, and logs retained.',
         'The iterative advantage is clear for this workload, not a universal guarantee.',
         'Next: larger samples, more repeats, local Hadoop aggregation, and ablations.'
        ],size=20,spacing=.115)
        fig.text(.055,.16,'Ketu, Mishra & Agarwal (2020), Computación y Sistemas 24(2), 669–686.\n'
                 'DOI: 10.13053/CyS-24-2-3401 · Apache Hadoop/Spark docs · scikit-learn docs',
                 fontsize=12,color=BLUE,linespacing=1.5)
        pdf.savefig(fig);plt.close(fig)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--student',default='Alikhan Sagymbekov')
    parser.add_argument('--group',default='IFMU-5')
    parser.add_argument('--supervisor',default='Assoc. Prof. Germanas Budnikas')
    args=parser.parse_args()
    REPORTS.mkdir(exist_ok=True)
    wc,km=read_csv(WORDCOUNT/'summary.csv'),read_csv(KMEANS/'summary.csv')
    markdown=build_markdown(args,wc,km)
    (REPORTS/'final_report.md').write_text(markdown,encoding='utf-8')
    render_report(markdown, args.student)
    render_slides(args)
    print('Created reports/final_report.md, final_report.pdf, and presentation.pdf')


if __name__=='__main__':
    main()
