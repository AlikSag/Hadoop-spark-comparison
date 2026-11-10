# K-Means input preparation

This is feature preparation, not a K-Means benchmark result.

- Source: `data/wikipedia_articles.jsonl` (1000 English Wikipedia articles).
- Retained documents: 1000.
- Excluded zero vectors: 0.
- Features per vector: 1000.
- Nonzero feature values: 243248.
- Local preparation time: 2.690 s.
- Implementation: scikit-learn 1.9.1 `TfidfVectorizer`.

## Transformation

Only the article text becomes a feature vector. The original ID links each vector
to its title and URL in a separate document table. IDs, titles, and URLs are not
numerical features.

The shared tokenizer lowercases text and splits it into Unicode letter/digit
sequences. English stop words are removed. Terms occurring in fewer than two
documents or more than 90% of documents are excluded. At most 1000 features are
selected by corpus frequency. Feature indices are shared across all documents.

Term frequency is `1 + ln(count)`. Smoothed inverse document frequency is
`ln((1 + N) / (1 + df)) + 1`, where `N` is the number of source documents and
`df` is the number containing the term. Each row is L2-normalized to unit length.
L2 normalization reduces the effect of document length; it does not remove all
length effects.

## Files

- `data/kmeans/vectors.jsonl`: one sparse vector per retained article.
- `data/kmeans/documents.jsonl`: article IDs, titles, and URLs.
- `data/kmeans/vocabulary.json`: ordered terms, IDF weights, and stop words.
- `data/kmeans/excluded.json`: IDs of excluded zero-vector documents, if any.
- `data/kmeans/metadata.json`: settings, versions, source and output hashes, timing.
- `data/kmeans/_SUCCESS`: completed preparation marker.

A sparse record contains `id`, `size`, `indices`, and `values`. The indices identify
nonzero positions; values are their TF-IDF weights. Unlisted positions are zero.
Hadoop and Spark will read this exact same prepared file.

## Scope and limitations

The local preparer loads the selected corpus into RAM. It is suitable for this
1000-document sample, not a claim of scalable distributed feature preparation.
The sample is a prefix in source order, not a random or representative selection.
There are no class labels or clustering-quality ground truth.

Preparation time is recorded separately and excluded from future K-Means engine
comparisons. Shared numerical rules and five initial centers have now been added.
Both distributed implementations and the iterative experiment are now complete; see [the K-Means report](kmeans.md). No K-Means speed comparison can be concluded here.

## Reproduce

```bash
python -m pip install -r requirements-prepare.txt
python scripts/prepare_vectors.py --output data/kmeans-repeat
```

Settings are in `config/tfidf.json`. Existing output directories are not overwritten.
An output directory without `_SUCCESS` is incomplete. Use a new output directory
for another preparation run.

Reference: [TfidfVectorizer documentation](https://scikit-learn.org/stable/modules/generated/sklearn.feature_extraction.text.TfidfVectorizer.html).

## Initial centers

`data/kmeans/initial_centers.json` contains five distinct document vectors,
selected with seed 42. Both engines will use these exact centers. Selection is
uniform over distinct nonzero vectors, not K-Means++ initialization.
The selected source IDs are 2175, 1235, 1036, 2391, and 1560.
Initial assignment of all 1000 documents was checked locally. This is a
correctness check, not a Hadoop/Spark K-Means benchmark.
