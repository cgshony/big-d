"""Tests for DataFrame.merge()."""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

from big_d.dataframe import DataFrame


def _users():
    return DataFrame({"user_id": [1, 2, 3], "name": ["Гошо", "Пешо", "Иван"]})


def _orders():
    return DataFrame({"user_id": [1, 1, 2], "item": ["book", "pen", "cup"]})


class TestInnerJoin(unittest.TestCase):
    def test_inner_join_matches_rows_and_drops_unmatched(self):
        result = _users().merge(_orders(), on="user_id", how="inner")
        rows = sorted(result.as_rows(), key=lambda row: (row["user_id"], row["item"]))
        self.assertEqual(
            rows,
            [
                {"user_id": 1, "name": "Гошо", "item": "book"},
                {"user_id": 1, "name": "Гошо", "item": "pen"},
                {"user_id": 2, "name": "Пешо", "item": "cup"},
            ],
        )


class TestLeftJoin(unittest.TestCase):
    def test_left_join_keeps_unmatched_left_rows_with_none(self):
        result = _users().merge(_orders(), on="user_id", how="left")
        rows = sorted(result.as_rows(), key=lambda row: (row["user_id"], row["item"] or ""))
        self.assertEqual(
            rows,
            [
                {"user_id": 1, "name": "Гошо", "item": "book"},
                {"user_id": 1, "name": "Гошо", "item": "pen"},
                {"user_id": 2, "name": "Пешо", "item": "cup"},
                {"user_id": 3, "name": "Иван", "item": None},
            ],
        )


class TestRightJoin(unittest.TestCase):
    def test_right_join_keeps_unmatched_right_rows_with_none(self):
        orders_with_extra = DataFrame({"user_id": [1, 4], "item": ["book", "kite"]})
        result = _users().merge(orders_with_extra, on="user_id", how="right")
        rows = sorted(result.as_rows(), key=lambda row: row["user_id"])
        self.assertEqual(
            rows,
            [
                {"user_id": 1, "name": "Гошо", "item": "book"},
                {"user_id": 4, "name": None, "item": "kite"},
            ],
        )


class TestOuterJoin(unittest.TestCase):
    def test_outer_join_keeps_everything(self):
        result = _users().merge(_orders(), on="user_id", how="outer")
        self.assertEqual(result.shape[0], 4)


class TestOverlappingColumns(unittest.TestCase):
    def test_shared_non_key_columns_get_suffixed(self):
        left = DataFrame({"id": [1], "value": ["a"]})
        right = DataFrame({"id": [1], "value": ["b"]})
        result = left.merge(right, on="id")
        row = result.as_rows()[0]
        self.assertEqual(row, {"id": 1, "value_x": "a", "value_y": "b"})


class TestErrors(unittest.TestCase):
    def test_unknown_how_raises(self):
        with self.assertRaises(ValueError):
            _users().merge(_orders(), on="user_id", how="cross")

    def test_missing_join_column_raises(self):
        with self.assertRaises(KeyError):
            _users().merge(_orders(), on="does_not_exist")


if __name__ == "__main__":
    unittest.main()
