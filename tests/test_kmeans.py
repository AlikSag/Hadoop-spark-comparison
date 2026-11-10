"""Check the shared K-Means geometry and reproducible initialization."""
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'common'))
sys.path.insert(0, str(ROOT / 'scripts'))
from kmeans import nearest_center, squared_distance, update_centers, validate_vector
from initialize_centers import choose_centers


def point(identifier, x, y):
    return {'id': identifier, 'size': 2, 'indices': [0, 1], 'values': [x, y]}


class KMeansTest(unittest.TestCase):
    def test_distance_and_tie(self):
        vector = point('a', 3, 4)
        self.assertEqual(squared_distance(vector, [0, 0]), 25)
        self.assertEqual(squared_distance(vector, [3, 4]), 0)
        self.assertEqual(nearest_center(point('b', 1, 0), [[0, 0], [2, 0]]), (0, 1))

    def test_one_lloyd_step_with_empty_cluster(self):
        centers = [[0, 0], [10, 10], [100, 100]]
        points = [point('a', 0, 2), point('b', 2, 0), point('c', 8, 10), point('d', 10, 8)]
        sums = [[0., 0.] for _ in centers]
        counts = [0] * len(centers)
        for vector in points:
            cluster, _ = nearest_center(vector, centers)
            counts[cluster] += 1
            for i, value in zip(vector['indices'], vector['values']):
                sums[cluster][i] += value
        self.assertEqual(update_centers(centers, sums, counts), [[1, 1], [9, 9], [100, 100]])
        self.assertEqual(centers, [[0, 0], [10, 10], [100, 100]])

    def test_seed_and_order_independence(self):
        vectors = [point(str(i), i, 1) for i in range(10)]
        first = choose_centers(vectors, 5, 42)
        self.assertEqual(first, choose_centers(list(reversed(vectors)), 5, 42))
        self.assertEqual(len(set(first['source_document_ids'])), 5)

    def test_identical_vectors_cannot_be_separate_initial_centers(self):
        with self.assertRaises(ValueError):
            choose_centers([point('a', 1, 0), point('b', 1, 0)], 2, 42)

    def test_invalid_sparse_coordinates_are_rejected(self):
        with self.assertRaises(ValueError):
            validate_vector({'id': 'a', 'size': 2, 'indices': [1, 0], 'values': [1, 1]})
        with self.assertRaises(ValueError):
            validate_vector(point('a', float('nan'), 1))
