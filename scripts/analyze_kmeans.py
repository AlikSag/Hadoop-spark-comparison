"""Validate a completed K-Means series and generate English tables and charts."""
import argparse
import csv
import json
import math
import os
from pathlib import Path
import re
from statistics import median, mean, stdev

from check_kmeans import compare_runs

ROOT = Path(__file__).resolve().parents[1]


def collect(series):
    runs = {}
    for folder in series.iterdir():
        match = re.fullmatch(r'(\d+)iter-run(\d+)-(hadoop|spark)', folder.name)
        if match and folder.is_dir():
            iterations, repetition, engine = int(match[1]), int(match[2]), match[3]
            runs[iterations, repetition, engine] = folder
    if not runs:
        raise ValueError('No experiment runs found')
    sizes = sorted({key[0] for key in runs})
    repetitions = sorted({key[1] for key in runs})
    expected = {(i, r, e) for i in sizes for r in repetitions for e in ('hadoop', 'spark')}
    if set(runs) != expected:
        raise ValueError('Incomplete series: each iteration/repetition needs both engines')
    rows = []
    conditions = None
    manifests = {}
    for (iterations, repetition, engine), folder in sorted(runs.items()):
        if not (folder / '_SUCCESS').is_file():
            raise ValueError(f'Incomplete run: {folder}')
        manifest = json.loads((folder / 'manifest.json').read_text())
        metrics = json.loads((folder / 'metrics.json').read_text())
        if manifest['iterations'] != iterations or metrics['iterations'] != iterations:
            raise ValueError('Iteration count differs from directory name')
        if manifest['engine'] != engine or metrics['engine'] != engine:
            raise ValueError('Engine differs from directory name')
        fields = ('documents', 'dimensions', 'clusters', 'input_sha256',
                  'initial_centers_sha256', 'resource_profile')
        identity = {key: manifest[key] for key in fields}
        if conditions is not None and identity != conditions:
            raise ValueError('Input, initialization, or resource profile changed within series')
        conditions = identity
        for key in ('total_algorithm_seconds', 'initial_objective', 'final_objective'):
            if not math.isfinite(metrics[key]) or metrics[key] < 0:
                raise ValueError(f'Invalid metric: {key}')
        if metrics['total_algorithm_seconds'] <= 0:
            raise ValueError('Elapsed time must be positive')
        if metrics['final_objective'] > metrics['initial_objective'] + 1e-8 * max(1, metrics['initial_objective']):
            raise ValueError('Objective increased')
        manifests[engine] = manifest
        rows.append({'iterations': iterations, 'repetition': repetition, 'engine': engine,
                     'total_algorithm_seconds': metrics['total_algorithm_seconds'],
                     'initial_objective': metrics['initial_objective'],
                     'final_objective': metrics['final_objective'], 'directory': folder.name,
                     'verified': True})
    comparisons = []
    for iterations in sizes:
        baseline = runs[iterations, repetitions[0], 'hadoop']
        for repetition in repetitions:
            h, s = runs[iterations, repetition, 'hadoop'], runs[iterations, repetition, 'spark']
            result = compare_runs(h, s)
            # Also require agreement across repetitions, not only within each pair.
            compare_runs(baseline, s)
            comparisons.append({'iterations': iterations, 'repetition': repetition, **result})
    return rows, comparisons, conditions, manifests


def summarize(rows):
    summary = []
    for iterations in sorted({row['iterations'] for row in rows}):
        pair = []
        for engine in ('hadoop', 'spark'):
            values = [row['total_algorithm_seconds'] for row in rows
                      if row['iterations'] == iterations and row['engine'] == engine]
            pair.append({'iterations': iterations, 'engine': engine, 'runs': len(values),
                         'median_seconds': median(values), 'mean_seconds': mean(values),
                         'min_seconds': min(values), 'max_seconds': max(values),
                         'stdev_seconds': stdev(values) if len(values) > 1 else 0.0})
        ratio = pair[0]['median_seconds'] / pair[1]['median_seconds']
        for row in pair:
            row['hadoop_over_spark_median'] = ratio
        summary.extend(pair)
    return summary


def write_csv(path, rows):
    with path.open('w', newline='', encoding='utf-8') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def chart(summary, rows, output):
    os.environ.setdefault('MPLCONFIGDIR', '/tmp/hscompare-matplotlib')
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    iterations = sorted({row['iterations'] for row in rows})
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.8), gridspec_kw={'width_ratios': [1.6, 1]})
    for engine, color, offset, label in [('hadoop', '#33658a', -0.04, 'Hadoop MapReduce'),
                                         ('spark', '#e58023', 0.04, 'Spark')]:
        group = [row for row in summary if row['engine'] == engine]
        meds = [row['median_seconds'] for row in group]
        errors = [[row['median_seconds']-row['min_seconds'] for row in group],
                  [row['max_seconds']-row['median_seconds'] for row in group]]
        axes[0].errorbar(iterations, meds, yerr=errors, marker='o', color=color,
                         capsize=4, linewidth=2, label=label)
        for iteration, value in zip(iterations, meds):
            axes[0].annotate(f'{value:.2f}', (iteration, value), xytext=(0, 10) if engine == 'hadoop' else (18, 4),
                             textcoords='offset points', ha='center', fontsize=9)
        points = [row for row in rows if row['engine'] == engine]
        axes[0].scatter([row['iterations']+offset for row in points],
                         [row['total_algorithm_seconds'] for row in points],
                         color=color, s=16, alpha=0.6)
    axes[0].set_ylabel('Total algorithm time, s')
    axes[0].set_title('Median, individual runs, and min–max range')
    axes[0].legend(frameon=False)
    axes[0].set_ylim(0, max(row['max_seconds'] for row in summary) * 1.15)
    ratios = [row['hadoop_over_spark_median'] for row in summary if row['engine'] == 'hadoop']
    axes[1].plot(iterations, ratios, marker='o', color='#3c8d78', linewidth=2)
    axes[1].axhline(1, color='gray', linestyle='--', linewidth=1)
    for x, ratio in zip(iterations, ratios):
        axes[1].annotate(f'{ratio:.2f}×', (x, ratio), xytext=(0, 9),
                         textcoords='offset points', ha='center')
    axes[1].set_ylim(0, max(ratios) * 1.2)
    axes[1].set_xlim(min(iterations) - 0.7, max(iterations) + 0.7)
    axes[1].set_ylabel('Hadoop median / Spark median')
    axes[1].set_title('Spark speedup in this series')
    for ax in axes:
        ax.set_xticks(iterations)
        ax.set_xlabel('Fixed number of Lloyd iterations')
        ax.grid(axis='y', alpha=0.2)
        ax.spines[['top', 'right']].set_visible(False)
    fig.suptitle('K-Means · 1000 Wikipedia articles · 5 clusters · single-node HDFS/YARN', fontsize=12)
    fig.text(0.5, 0.015, 'Complete algorithm time includes startup and initial reading; input preparation/upload excluded.',
             ha='center', fontsize=9)
    fig.tight_layout(rect=(0, 0.05, 1, 0.95))
    for extension in ('png', 'svg', 'pdf'):
        fig.savefig(output / f'kmeans.{extension}', dpi=180)
    plt.close(fig)


def report(series, summary, comparisons, conditions, output, rows):
    link = Path(os.path.relpath(series, output)).as_posix()
    iterations = sorted({row['iterations'] for row in rows})
    repetitions = len({row['repetition'] for row in rows})
    max_difference = max(row['maximum_absolute_center_difference'] for row in comparisons)
    text = ['# Experiment 2: iterative K-Means — Hadoop MapReduce vs Spark', '',
            f'Source series: [{series.name}]({link}/).', '', '![K-Means results](kmeans.png)', '',
            '## Method', '',
            'This adapts the iterative K-Means experiment in Section 5.3 of Ketu, Mishra, '
            'and Agarwal (2020), DOI: 10.13053/CyS-24-2-3401. It does not reproduce the '
            'original corpus sizes or the paper\'s exact feature pipeline.', '',
            f"The workload contains {conditions['documents']} English Wikipedia articles, "
            f"{conditions['dimensions']}-dimensional L2-normalized TF-IDF vectors, and "
            f"{conditions['clusters']} clusters. The sample is a prefix in source order, not "
            'a random or representative selection. Article IDs link vectors back to source documents.', '',
            'Both implementations use identical input vectors and initial centers selected '
            'from distinct document vectors with seed 42. They use squared Euclidean distance, '
            'arithmetic means without renormalization, lowest-index exact-tie resolution, '
            'and preservation of previous centers for empty clusters. There is no early stopping.', '',
            f'Fixed iteration counts: {iterations}; repetitions per condition: {repetitions}; '
            f'applications: {len(rows)}. Within each repetition, both engines run for every '
            'iteration count. Hadoop runs first in repetitions 1 and 3; Spark runs first in '
            'repetition 2. This alternates order but is not perfectly balanced.', '',
            '## Execution and resources', '',
            'Both engines run on the same single-node HDFS/YARN cluster. The shared worker '
            'budget requests two vcores and 2048 MiB: Hadoop allows up to two tasks with '
            '1024 MiB containers and 512 MiB heaps; Spark uses one two-core executor with '
            '1024 MiB heap plus 1024 MiB overhead. ApplicationMasters request 1024 MiB. '
            'The Spark client-mode driver runs separately with a 1024 MiB heap. These are '
            'comparable worker requests, not identical total memory usage or physical CPU isolation.', '',
            'Hadoop launches a separate Streaming job for every iteration. It reads vectors '
            'from HDFS, shuffles assignments, reduces groups, then downloads and merges '
            'the new centers before the next job. Reducers start after all maps complete.', '',
            'Spark launches one application per run, reads and validates the vectors once, '
            'and materializes a MEMORY_AND_DISK persisted RDD. Each iteration broadcasts '
            'centers, aggregates local sums, reduces by cluster, and updates centers. '
            'Persistence may spill to disk. The initial read/persistence is included in timing.', '',
            '## Timing definition', '',
            'The reported total includes application/job startup, reading, processing, '
            'and output. Hadoop includes intermediate center downloads and merging; Spark '
            'includes shutdown and downloading the final center state. Local input '
            'validation, TF-IDF preparation, HDFS input upload, and code staging are excluded '
            'for both. Local final-objective scans are correctness checks outside the timer. '
            'Internal Spark iteration timers are not used for this comparison.', '',
            '## Results', '',
            '| Iterations | Hadoop median [min–max], s | Spark median [min–max], s | Hadoop/Spark |',
            '|---:|---:|---:|---:|']
    for iteration in iterations:
        h, s = [next(row for row in summary if row['iterations'] == iteration and row['engine'] == engine)
                for engine in ('hadoop', 'spark')]
        text.append(f"| {iteration} | {h['median_seconds']:.3f} [{h['min_seconds']:.3f}–{h['max_seconds']:.3f}] | "
                    f"{s['median_seconds']:.3f} [{s['min_seconds']:.3f}–{s['max_seconds']:.3f}] | "
                    f"{h['hadoop_over_spark_median']:.3f}× |")
    text += ['', 'Error bars show observed min–max ranges, not confidence intervals. '
             'Speedup is the ratio of medians. Individual measurements and sample standard '
             'deviations are saved in the CSV files.', '', '## Correctness', '',
             'Every application completed successfully. Full per-iteration center matrices '
             'and cluster counts match between engines and across repetitions. Counts match '
             'exactly; coordinates use relative tolerance 1e-9 and absolute tolerance 1e-10. '
             f'The largest observed absolute coordinate difference is {max_difference:.3g}.', '',
             '| Iterations | Final objective (sum of squared nearest-center distances) |', '|---:|---:|']
    for iteration in iterations:
        row = next(row for row in rows if row['iterations'] == iteration and row['engine'] == 'hadoop')
        text.append(f"| {iteration} | {row['final_objective']:.6f} |")
    first = next(row for row in summary if row['iterations'] == iterations[0] and row['engine'] == 'hadoop')
    last = next(row for row in summary if row['iterations'] == iterations[-1] and row['engine'] == 'hadoop')
    text += ['', '## Interpretation and limitations', '',
             f"Spark had lower median time at every tested iteration count. The ratio of "
             f"Hadoop to Spark median time increased from {first['hadoop_over_spark_median']:.2f} "
             f"at {iterations[0]} iteration(s) to {last['hadoop_over_spark_median']:.2f} "
             f"at {iterations[-1]} iterations.", '',
             'Hadoop time grows roughly with the number of jobs, while Spark reuses one '
             'application and its persisted vectors. On this small workload, startup and '
             'coordination costs are substantial, making Spark\'s total time comparatively flat. '
             'This is consistent with the iterative-performance trend discussed in the paper.', '',
             'The observed difference cannot be attributed solely to caching: Spark also '
             'uses partition-local aggregation, while the current Hadoop implementation '
             'has no combiner and shuffles full sparse vectors. Python process startup, '
             'serialization, task partitioning, reducer scheduling, and center transfers '
             'also differ. No ablation study isolates these effects.', '',
             'This 1000-document workload is small, not a demonstration of multi-node '
             'scalability or the paper\'s multi-gigabyte results. There are only three '
             'repetitions, OS caches were not reset, and workloads were not randomized. '
             'Actual peak RAM, CPU usage, disk traffic, and clustering quality against '
             'ground truth were not measured. The objective decreasing is an algorithm '
             'consistency check, not evidence of semantic clustering quality.', '',
             '## Presentation summary', '',
             '“I compared the same fixed-iteration Lloyd K-Means on identical TF-IDF '
             'vectors and initial centers using real Hadoop Streaming and PySpark on YARN. '
             'Each configuration was repeated three times, and all iteration centers '
             'and counts were verified. Spark was faster in this configuration, with '
             'its relative advantage increasing as more iterations reused the persisted '
             'input. The result includes application startup and initial loading and '
             'does not establish a universal speedup.”', '']
    (output / 'kmeans.md').write_text('\n'.join(text), encoding='utf-8')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('series', type=Path)
    parser.add_argument('--output', type=Path, default=ROOT / 'reports')
    args = parser.parse_args()
    try:
        series, output = args.series.resolve(), args.output.resolve()
        rows, comparisons, conditions, manifests = collect(series)
        summary = summarize(rows)
        output.mkdir(parents=True, exist_ok=True)
        write_csv(series / 'measurements.csv', rows)
        write_csv(series / 'summary.csv', summary)
        (series / 'verification.json').write_text(json.dumps(comparisons, indent=2))
        chart(summary, rows, output)
        report(series, summary, comparisons, conditions, output, rows)
        for row in summary:
            print(f"{row['iterations']:2d} iterations | {row['engine']:6s} | median {row['median_seconds']:.3f} s | "
                  f"Hadoop/Spark {row['hadoop_over_spark_median']:.3f}")
    except (OSError, ValueError, TypeError, KeyError, ImportError) as error:
        parser.exit(1, f'Analysis failed: {error}\n')
    print(f'Validated {len(rows)} applications and {len(comparisons)} pairs. Reports: {output}')


if __name__ == '__main__':
    main()
