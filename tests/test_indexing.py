"""Tests for DataFrame.__getitem__: column, row, slice, and mask selection."""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

from big_d.dataframe import DataFrame


def _sample():
    return DataFrame({"name": ["Гошо", "Пешо", "Иван"], "age": [30, 16, 45]})


class TestColumnAccess(unittest.TestCase):
    def test_single_column_returns_list(self):
        self.assertEqual(_sample()["age"], [30, 16, 45])

    def test_column_list_returns_subset_dataframe(self):
        result = _sample()[["age"]]
        self.assertIsInstance(result, DataFrame)
        self.assertEqual(result.shape, (3, 1))
        self.assertEqual(result["age"], [30, 16, 45])


class TestRowAccess(unittest.TestCase):
    def test_integer_returns_row_dict(self):
        self.assertEqual(_sample()[1], {"name": "Пешо", "age": 16})

    def test_slice_returns_dataframe(self):
        result = _sample()[0:2]
        self.assertEqual(result["name"], ["Гошо", "Пешо"])
        self.assertEqual(result["age"], [30, 16])


class TestBooleanMask(unittest.TestCase):
    def test_mask_keeps_only_true_rows(self):
        mask = [age > 18 for age in _sample()["age"]]
        result = _sample()[mask]
        self.assertEqual(result["name"], ["Гошо", "Иван"])
        self.assertEqual(result["age"], [30, 45])


class TestErrors(unittest.TestCase):
    def test_mixed_list_raises_type_error(self):
        with self.assertRaises(TypeError):
            _sample()[["age", True]]

    def test_unsupported_key_type_raises_type_error(self):
        with self.assertRaises(TypeError):
            _sample()[3.14]


if __name__ == "__main__":
    unittest.main()
