"""Tests for None/missing-value handling on DataFrame."""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

from big_d.dataframe import DataFrame
from big_d.exceptions import SchemaError
from big_d.schema import Schema


class TestIsValidColumnWithNulls(unittest.TestCase):
    def test_none_alongside_consistent_type_is_valid(self):
        self.assertTrue(DataFrame.is_valid_column(None, [1, None, 3]))

    def test_all_none_is_valid(self):
        self.assertTrue(DataFrame.is_valid_column(None, [None, None]))

    def test_none_does_not_hide_inconsistent_type(self):
        with self.assertRaises(TypeError):
            DataFrame.is_valid_column(None, [1, None, "three"])


class TestIsna(unittest.TestCase):
    def test_isna_marks_none_values(self):
        df = DataFrame({"age": [30, None, 16]})
        mask = df.isna()
        self.assertEqual(mask["age"], [False, True, False])


class TestDropna(unittest.TestCase):
    def test_dropna_removes_rows_with_any_none(self):
        df = DataFrame({"name": ["Гошо", "Пешо", "Иван"], "age": [30, None, 16]})
        result = df.dropna()
        self.assertEqual(result["name"], ["Гошо", "Иван"])
        self.assertEqual(result["age"], [30, 16])

    def test_dropna_keeps_frame_with_no_nulls_unchanged(self):
        df = DataFrame({"age": [30, 16]})
        result = df.dropna()
        self.assertEqual(result, df)


class TestFillna(unittest.TestCase):
    def test_fillna_scalar_applies_to_every_column(self):
        df = DataFrame({"age": [30, None], "height": [None, 196]})
        result = df.fillna(0)
        self.assertEqual(result["age"], [30, 0])
        self.assertEqual(result["height"], [0, 196])

    def test_fillna_dict_targets_specific_columns(self):
        df = DataFrame({"age": [30, None], "height": [None, 196]})
        result = df.fillna({"age": -1})
        self.assertEqual(result["age"], [30, -1])
        self.assertEqual(result["height"], [None, 196])


class TestSchemaWithNulls(unittest.TestCase):
    def test_schema_tolerates_leading_none(self):
        schema = Schema(age=int)
        schema.validate({"age": [None, 30, 16]})

    def test_schema_still_catches_wrong_type(self):
        schema = Schema(age=int)
        with self.assertRaises(SchemaError):
            schema.validate({"age": [None, "thirty"]})


if __name__ == "__main__":
    unittest.main()
