"""Generate charts and a report from a completed WordCount experiment."""
import argparse
import csv
import json
import math
import os
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parents[1]


def load_results(folder):
    if not (folder / '_SUCCESS').is_file():
        raise ValueError('The experiment is incomplete: missing _SUCCESS')
    manifest = json.loads((folder / 'manifest.json').read_text())
    with (folder / 'measurements.csv').open(newline='') as stream:
        rows = list(csv.DictReader(stream))
    expected = {(size, run, engine) for size in manifest['sizes_mib']
                for run in range(1, manifest['runs'] + 1) for engine in ('hadoop', 'spark')}
    seen = set()
    groups = {}
    for row in rows:
        size, run, engine = int(row['requested_mib']), int(row['run']), row['engine']
        key = size, run, engine
        if key in seen or key not in expected or row['verified'] != 'True':
            raise ValueError('Invalid, duplicate, or unverified measurement')
        elapsed = float(row['elapsed_seconds'])
        if not math.isfinite(elapsed) or elapsed <= 0:
            raise ValueError('Invalid elapsed time')
        if int(row['actual_bytes']) != manifest['samples'][str(size)]['actual_bytes']:
            raise ValueError('Input size does not match the manifest')
        seen.add(key)
        groups.setdefault((size, engine), []).append(elapsed)
    if seen != expected:
        raise ValueError('The measurements table is missing runs')
    for size in manifest['sizes_mib']:
        answers = {(row['unique_words'], row['total_words']) for row in rows
                   if int(row['requested_mib']) == size}
        if len(answers) != 1:
            raise ValueError('Word-count consistency checks differ')
    return manifest, rows, groups


def plot(groups, sizes, output):
    os.environ.setdefault('MPLCONFIGDIR', '/tmp/hscompare-matplotlib')
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.8), gridspec_kw={'width_ratios': [1.7, 1]})
    colors = {'hadoop': '#33658a', 'spark': '#e58023'}
    for engine, offset, label in [('hadoop', -0.19, 'Hadoop MapReduce'), ('spark', 0.19, 'Spark')]:
        values = [groups[size, engine] for size in sizes]
        meds = [median(v) for v in values]
        positions = [i + offset for i in range(len(sizes))]
        errors = [[m - min(v) for m, v in zip(meds, values)],
                  [max(v) - m for m, v in zip(meds, values)]]
        bars = axes[0].bar(positions, meds, width=0.34, color=colors[engine], label=label,
                           yerr=errors, capsize=4, error_kw={'elinewidth': 1})
        axes[0].bar_label(bars, labels=[f'{m:.2f}' for m in meds], padding=7, fontsize=9)
        for x, v in zip(positions, values):
            axes[0].scatter([x] * len(v), v, s=13, color='black', zorder=4)
    axes[0].set_ylabel('Total execution time, s')
    axes[0].set_title('Median, individual runs, and min–max range')
    axes[0].legend(loc='upper left', frameon=False)
    axes[0].set_ylim(0, max(max(v) for v in groups.values()) * 1.2)
    ratios = [median(groups[size, 'hadoop']) / median(groups[size, 'spark']) for size in sizes]
    bars = axes[1].bar(range(len(sizes)), ratios, width=0.5, color='#3c8d78')
    axes[1].bar_label(bars, labels=[f'{r:.2f}×' for r in ratios], padding=5)
    axes[1].axhline(1, color='gray', linestyle='--', linewidth=1)
    axes[1].set_ylabel('Hadoop median / Spark median')
    axes[1].set_title('Spark speedup in this experiment')
    axes[1].set_ylim(0, max(1.2, max(ratios) * 1.2))
    for ax in axes:
        ax.set_xticks(range(len(sizes)), [str(size) for size in sizes])
        ax.set_xlabel('Requested input size, MiB')
        ax.set_axisbelow(True)
        ax.grid(axis='y', alpha=0.2)
        ax.spines[['top', 'right']].set_visible(False)
    fig.suptitle('WordCount · Wikipedia · single-node HDFS/YARN', fontsize=14)
    fig.text(0.5, 0.015, '3 repetitions; includes application startup, reading, processing, and output writing.'
             if all(len(v) == 3 for v in groups.values()) else
             'Includes application startup, reading, processing, and output writing.', ha='center', fontsize=9)
    fig.tight_layout(rect=(0, 0.05, 1, 0.95))
    for extension in ('png', 'svg', 'pdf'):
        fig.savefig(output / f'wordcount.{extension}', dpi=180)
    plt.close(fig)


def report(folder, output, manifest, rows, groups):
    sizes = sorted(manifest['sizes_mib'])
    source_link = Path(os.path.relpath(folder, output)).as_posix()
    profile = manifest['resource_profile']
    workers = profile['workers']
    memory = profile['task_memory_mb']
    heap = profile['task_heap_mb']
    text = ['# Experiment 1: WordCount — Hadoop MapReduce vs Spark', '',
            f'Measurements: [{folder.name}]({source_link}/measurements.csv).', '',
            '![Execution time and speedup](wordcount.png)', '',
            '## Method', '',
            'Real Hadoop Streaming and PySpark applications ran sequentially on a single '
            'computer through YARN, reading identical input files from HDFS. This adapts '
            'Section 5.1 of Ketu, Mishra, and Agarwal (2020), DOI: 10.13053/CyS-24-2-3401. '
            'The corpus and input sizes differ from the original paper.', '',
            f"Hardware: {manifest['cpu']}; RAM: {manifest['memory']}. "
            f"Python {manifest['python'].split()[0]}; Spark {manifest['spark_version'].strip()}; "
            f"{manifest['hadoop_version'].splitlines()[0]}; Java 17.", '',
            f"Requested input sizes: {sizes} MiB; repetitions: {manifest['runs']}; "
            f"total jobs: {len(rows)}. Prefixes end at complete lines. Exact byte counts "
            'and SHA256 hashes are recorded in manifest.json. Engine order alternated. '
            'There was no separate warm-up and the OS file cache was not cleared.', '',
            '**The timer measures the complete application launch:** JVM startup, resource '
            'allocation, reading, counting, shuffle, and output writing. Input preparation, '
            'uploading inputs to HDFS, downloading outputs, and validation are excluded.', '',
            '## Resource configuration', '',
            f'Requested worker budget: up to {workers} vcores and {workers * memory} MiB. '
            f'Hadoop runs up to {workers} map tasks or {workers} reduce tasks concurrently. '
            f'Each task requests one vcore and {memory} MiB with a {heap} MiB Java heap. '
            'Reduce tasks start after all map tasks finish. '
            f'Spark uses one executor with {workers} cores, a {workers * heap} MiB heap, '
            f'and {workers * (memory - heap)} MiB overhead. '
            f"Both engines use {profile['partitions']} output partitions. "
            f"The ApplicationMaster requests {profile['application_master_memory_mb']} MiB. "
            'Speculative execution is disabled.', '',
            'These are comparable worker resource requests, not equal total application '
            'resource usage. The Spark driver runs outside YARN with a 1024 MiB heap. '
            'Control processes differ between engines. Delaying reducers affects Hadoop '
            'performance, and the number of input partitions can differ. YARN vcores do '
            'not pin processes to physical CPU cores.', '',
            '## Results', '',
            '| Input, MiB | Hadoop median [min–max], s | Spark median [min–max], s | Hadoop/Spark |',
            '|---:|---:|---:|---:|']
    for size in sizes:
        h, s = groups[size, 'hadoop'], groups[size, 'spark']
        text.append(f'| {size} | {median(h):.3f} [{min(h):.3f}–{max(h):.3f}] | '
                    f'{median(s):.3f} [{min(s):.3f}–{max(s):.3f}] | {median(h)/median(s):.3f}× |')
    text += ['', 'Speedup is the ratio of median times, not the mean of per-run ratios. '
             'Error bars show the observed min–max range, not a confidence interval. '
             'Black points show individual measurements.', '', '## Correctness', '',
             'During the experiment, complete word-count dictionaries were checked for equality '
             'across all runs at each input size. Both engines use the same tokenization: '
             'lowercase Unicode letters and digits. Additional consistency checks:', '',
             '| Input, MiB | Unique words | Total occurrences |', '|---:|---:|---:|']
    for size in sizes:
        row = next(r for r in rows if int(r['requested_mib']) == size)
        text.append(f"| {size} | {int(row['unique_words']):,} | {int(row['total_words']):,} |")
    ratios = [median(groups[size, 'hadoop']) / median(groups[size, 'spark']) for size in sizes]
    largest = sizes[-1]
    h, sp = median(groups[largest, 'hadoop']), median(groups[largest, 'spark'])
    conclusion = ('Spark had a lower median time at every tested input size.' if min(ratios) > 1
                  else 'The table shows which engine had a lower median at each input size.')
    text += ['', '## Conclusions and presentation notes', '',
             f'{conclusion} The Hadoop/Spark median-time ratio ranged from '
             f'{min(ratios):.2f} to {max(ratios):.2f}. This is an adapted reproduction of '
             'the method rather than an exact reproduction of the paper\'s numerical results.', '',
             f'At {largest} MiB, Hadoop took {h:.3f} s and Spark took {sp:.3f} s. '
             f'The time ratio was {h/sp:.2f}; the relative change in Spark execution '
             f'time compared with Hadoop was {(sp/h-1)*100:.1f}%.', '',
             'These findings apply to the recorded versions, hardware, corpus, Python '
             'implementations, and configuration. Three repetitions do not demonstrate '
             'universal superiority. Peak RAM, CPU usage, and disk I/O were not measured. '
             'The difference cannot be attributed solely to in-memory computation: '
             'persist/cache were not used, and Streaming, serialization, shuffle, and '
             'process startup costs differ.', '', '## Next experiment', '',
             'K-Means will use identical document vectors and initial centers with fixed '
             'iteration counts of 1, 3, 5, and 10. Hadoop will launch one job per iteration; '
             'Spark will reuse explicitly persisted vectors. Feature preparation will be '
             'measured separately, while initial loading and persistence will be included '
             'in total algorithm time. The experiment is now complete; see [the K-Means report](kmeans.md).', '']
    (output / 'wordcount.md').write_text('\n'.join(text), encoding='utf-8')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('results', type=Path)
    parser.add_argument('--output', type=Path, default=ROOT / 'reports')
    args = parser.parse_args()
    try:
        manifest, rows, groups = load_results(args.results.resolve())
        args.output.mkdir(parents=True, exist_ok=True)
        plot(groups, sorted(manifest['sizes_mib']), args.output)
        report(args.results.resolve(), args.output.resolve(), manifest, rows, groups)
    except (ValueError, OSError, ImportError) as error:
        parser.exit(1, f'Error: {error}\nCharts require matplotlib from requirements-report.txt\n')
    print(f'Report and charts: {args.output.resolve()}')


if __name__ == '__main__':
    main()
