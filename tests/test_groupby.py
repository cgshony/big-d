"""Tests for DataFrame.groupby() and DataFrame.value_counts()."""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

from big_d.dataframe import DataFrame


def _sample():
    return DataFrame(
        {
            "team": ["red", "blue", "red", "blue", "red"],
            "score": [10, 20, 30, 40, 50],
        },
    )


class TestGroups(unittest.TestCase):
    def test_groups_property_partitions_rows_by_key(self):
        groups = _sample().groupby("team").groups
        self.assertEqual(len(groups["red"]), 3)
        self.assertEqual(len(groups["blue"]), 2)


class TestBuiltinAggregations(unittest.TestCase):
    def test_sum(self):
        result = _sample().groupby("team").sum()
        pairs = dict(zip(result["team"], result["score"]))
        self.assertEqual(pairs, {"red": 90, "blue": 60})

    def test_mean(self):
        result = _sample().groupby("team").mean()
        pairs = dict(zip(result["team"], result["score"]))
        self.assertEqual(pairs, {"red": 30, "blue": 30})

    def test_min_and_max(self):
        min_result = _sample().groupby("team").min()
        max_result = _sample().groupby("team").max()
        self.assertEqual(dict(zip(min_result["team"], min_result["score"])), {"red": 10, "blue": 20})
        self.assertEqual(dict(zip(max_result["team"], max_result["score"])), {"red": 50, "blue": 40})

    def test_count(self):
        result = _sample().groupby("team").count()
        self.assertEqual(dict(zip(result["team"], result["count"])), {"red": 3, "blue": 2})


class TestAgg(unittest.TestCase):
    def test_agg_with_named_aggregation(self):
        result = _sample().groupby("team").agg({"score": "sum"})
        self.assertEqual(dict(zip(result["team"], result["score"])), {"red": 90, "blue": 60})

    def test_agg_with_callable(self):
        result = _sample().groupby("team").agg({"score": lambda values: max(values) - min(values)})
        self.assertEqual(dict(zip(result["team"], result["score"])), {"red": 40, "blue": 20})


class TestValueCounts(unittest.TestCase):
    def test_value_counts_orders_most_common_first(self):
        df = DataFrame({"team": ["red", "blue", "red", "red"]})
        result = df.value_counts("team")
        self.assertEqual(result["team"], ["red", "blue"])
        self.assertEqual(result["count"], [3, 1])


if __name__ == "__main__":
    unittest.main()
