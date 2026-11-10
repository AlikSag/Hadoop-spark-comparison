"""Shared numerical rules for Hadoop and Spark Lloyd K-Means."""
import math
import json


def validate_vector(vector, expected_size=None):
    """Validate a sparse document vector once, when it is read."""
    if not isinstance(vector, dict):
        raise ValueError('A vector must be a JSON object')
    size = vector.get('size')
    if type(size) is not int or size < 1 or (expected_size is not None and size != expected_size):
        raise ValueError('Invalid or inconsistent vector dimension')
    if not isinstance(vector.get('id'), str) or not vector['id']:
        raise ValueError('A vector must have a nonempty string ID')
    indices, values = vector.get('indices'), vector.get('values')
    if not isinstance(indices, list) or not isinstance(values, list) or len(indices) != len(values):
        raise ValueError('indices and values must be equally sized lists')
    previous = -1
    for index, value in zip(indices, values):
        if type(index) is not int or not previous < index < size:
            raise ValueError('Sparse indices must be unique, sorted, and within the dimension')
        if type(value) not in (int, float) or not math.isfinite(value):
            raise ValueError('Vector values must be finite numbers')
        previous = index
    return vector


def load_centers(path):
    """Read a validated dense center matrix from a shared JSON file."""
    with open(path, encoding='utf-8') as stream:
        record = json.load(stream)
    if not isinstance(record, dict):
        raise ValueError('The centers file must contain a JSON object')
    size, count, centers = record.get('dimensions'), record.get('clusters'), record.get('centers')
    if type(size) is not int or size < 1 or type(count) is not int or count < 1:
        raise ValueError('Center dimensions and cluster count must be positive integers')
    if not isinstance(centers, list) or len(centers) != count:
        raise ValueError('The center matrix must match the cluster count')
    for center in centers:
        if not isinstance(center, list) or len(center) != size:
            raise ValueError('Every center must match the declared dimension')
        if not all(type(value) in (int, float) and math.isfinite(value) for value in center):
            raise ValueError('Center coordinates must be finite numbers')
    return centers


def dense_vector(vector):
    result = [0.0] * vector['size']
    for index, value in zip(vector['indices'], vector['values']):
        result[index] = float(value)
    return result


def squared_distance(vector, center, center_squared_norm=None):
    """Squared Euclidean distance from a sparse point to a dense center.

    ||x-c||² = ||x||² + ||c||² - 2<x,c>. Square roots do not change
    which center is nearest. Center norms can be reused for all documents.
    """
    if vector['size'] != len(center):
        raise ValueError('Vector and center dimensions differ')
    if center_squared_norm is None:
        center_squared_norm = math.fsum(value * value for value in center)
    point_norm = math.fsum(value * value for value in vector['values'])
    dot = math.fsum(value * center[index] for index, value in
                    zip(vector['indices'], vector['values']))
    # Round-off can produce a tiny negative result for identical vectors.
    return max(0.0, math.fsum((point_norm, center_squared_norm, -2.0 * dot)))


def nearest_center(vector, centers, center_squared_norms=None):
    """Return (cluster index, squared distance); exact ties use the lowest index."""
    if not centers:
        raise ValueError('At least one center is required')
    if center_squared_norms is None:
        center_squared_norms = [math.fsum(value * value for value in center) for center in centers]
    if len(center_squared_norms) != len(centers):
        raise ValueError('There must be one squared norm per center')
    best_index = 0
    best_distance = squared_distance(vector, centers[0], center_squared_norms[0])
    for index in range(1, len(centers)):
        distance = squared_distance(vector, centers[index], center_squared_norms[index])
        if distance < best_distance:
            best_index, best_distance = index, distance
    return best_index, best_distance


def update_centers(previous, sums, counts):
    """Take arithmetic means; an empty cluster keeps its previous center.

    Centers are not renormalized: this is ordinary Euclidean Lloyd K-Means,
    not spherical K-Means. Input documents may still be L2-normalized.
    """
    if len(previous) != len(sums) or len(previous) != len(counts):
        raise ValueError('Center sums and counts must cover every cluster')
    result = []
    for old, total, count in zip(previous, sums, counts):
        if type(count) is not int or count < 0 or len(total) != len(old):
            raise ValueError('Invalid cluster count or sum dimension')
        if not all(math.isfinite(value) for value in total):
            raise ValueError('Cluster sums must be finite')
        result.append([value / count for value in total] if count else list(old))
    return result


def merge_center_updates(previous, records):
    """Merge all reducer updates; omitted (empty) clusters retain old centers."""
    centers = [list(center) for center in previous]
    counts = [0] * len(previous)
    for record in records:
        cluster, count, center = record['cluster'], record['count'], record['center']
        if type(cluster) is not int or not 0 <= cluster < len(previous):
            raise ValueError('Reducer output contains an invalid cluster index')
        if counts[cluster]:
            raise ValueError('Duplicate cluster update across reducer outputs')
        if type(count) is not int or count < 1:
            raise ValueError('A reducer update must have a positive document count')
        if not isinstance(center, list) or len(center) != len(previous[cluster]):
            raise ValueError('Reducer center dimension differs from previous centers')
        if not all(type(value) in (int, float) and math.isfinite(value) for value in center):
            raise ValueError('Reducer center coordinates must be finite')
        centers[cluster], counts[cluster] = list(center), count
    return centers, counts


def partition_sums(vectors, centers):
    """Spark partition-local partial sums; assignments use the shared rules."""
    norms = [math.fsum(value * value for value in center) for center in centers]
    sums = {}
    counts = {}
    for vector in vectors:
        cluster, _ = nearest_center(vector, centers, norms)
        if cluster not in sums:
            sums[cluster] = [0.0] * len(centers[cluster])
            counts[cluster] = 0
        for index, value in zip(vector['indices'], vector['values']):
            sums[cluster][index] += value
        counts[cluster] += 1
    for cluster in sorted(sums):
        yield cluster, (sums[cluster], counts[cluster])


def combine_sums(left, right):
    """Add coordinate sums and counts; never average partial means."""
    if len(left[0]) != len(right[0]):
        raise ValueError('Partial sums have different dimensions')
    return [a + b for a, b in zip(left[0], right[0])], left[1] + right[1]
