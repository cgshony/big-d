"""Tests for the dataframe module."""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

from big_d.dataframe import DataFrame, require_non_empty


class TestDataFrame(unittest.TestCase):
    def setUp(self):
        self.df = DataFrame({"name": ["Гошо", "Пешо"], "age": [30, 16]})

    def test_initialize_invalid_input(self):
        with self.assertRaises(ValueError):
            DataFrame([("Гошо", "Пешо"), ("age"), [30, 16]])

    def tearDown(self):
        del self.df

    def test_df(self):
        self.assertEqual(self.df.shape, (2,2))

    def test_shape(self):
        self.assertEqual

    def test_getitem(self):
        self.assertEqual(self.df["age"], [30,16])

    def test_setitem(self):
        height = [175, "по-висок от Стан"]
        self.df["height"] = height
        self.assertEqual(self.df.shape, (2,3))
        self.assertEqual(self.df["height"], height)

    @unittest.skip
    def test_inconsistent_column_len(self):
        """When initializing DF with inconsistentcolumns length, an Error must be raised!"""
        with self.assertRaises(ValueError):
            DataFrame({"name": ["Гошо", "Пешо"], "age": [16]})

    def test_incompatible_data_types(self):
        """When initializing DF with inconsistent data types, an Error must be raised!"""
        with self.assertRaises(TypeError):
            DataFrame({"name": [22, "Пешо"], "age": [30, 16]})

    def test_setitem_invalid(self):
        height = [175, 215]
        with self.assertRaises(TypeError) as err:
            self.df["height"] = height
        self.assertEqual(str(err.exception), "Inconsistent column type")

    def test_print(self):
        """str(DataFrame) should represent the DataFrame with it's size and contents."""
        df = DataFrame({"name": ["Гошо", "Пешо"], "age": [30, 16], "height": [175, 196]})
        expected_string = """DataFrame (2x3)
+------+-----+--------+
| name | age | height |
+------+-----+--------+
| Гошо |  30 |  175   |
| Пешо |  16 |  196   |
+------+-----+--------+"""
        self.assertEqual(str(df), expected_string)

    def test_from_rows(self):
        """DataFrame.from_rows should create a new DataFrame object from a list of rows."""
        rows = [
            {"name": "Гошо", "age": 30, "height": 175},
            {"name": "Пешо", "age": 16, "height": 196},
        ]
        df = DataFrame.from_rows(rows)


class TestValidators(unittest, TestCase):

    def test_requite_non_empty(self):
        dummy_func = lambda *args, **kwards: None
        decorated = require_non_empty(dummy_func)
        decorated(DataFrame({"number":[1, 2, 3, 4, 7]}), column_number="number")
        self.assertEqual(result, None)

    def test_requite_non_empty(self):
        dummy_func = lambda *args, **kwards: None
        decorated = require_non_empty(dummy_func)
        with self.assertRaises(TypeError) as err:
            result = decorated(DataFrame({}), column_number="number")
        self.assertEqual(str(err.exception), "Empty DF is not allowed")


if __name__ == "__main__":
    unittest.main()
