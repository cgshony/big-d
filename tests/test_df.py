"""Tests for the dataframe module."""

import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

from dataframe import DataFrame, require_non_empty


class TestDataFrame(unittest.TestCase):
    def setUp(self):
        self.df = DataFrame({"name": ["Гошо", "Пешо"], "age": [30, 16]})

    def test_initialize_invalid_input(self):
        with self.assertRaises(ValueError):
            DataFrame([("Гошо", "Пешо"), ("age"), [30, 16]])

    def tearDown(self):
        del self.df

    def test_df(self):
        self.assertEqual(self.df.shape, (2, 2))

    def test_shape(self):
        self.assertEqual

    def test_getitem(self):
        self.assertEqual(self.df["age"], [30, 16])

    def test_setitem(self):
        height = [175, "по-висок от Стан"]
        self.df["height"] = height
        self.assertEqual(self.df.shape, (2, 3))
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
        DataFrame.from_rows(rows)

    def test_from_csv(self):
        """DataFrame.from_csv should create a new DataFrame object from the contents of a *.csv file."""
        rows = [
            {"name": "Гошо", "age": 30, "height": 175},
            {"name": "Пешо", "age": 16, "height": 196},
        ]
        with patch("dataframe.Path.open"), patch("dataframe.csv.DictReader", return_value=rows):
            df = DataFrame.from_csv("Some_path.csv")
        self.assertIsInstance(df, DataFrame)

    def test_map_column(self):
        """..."""
        start_df = DataFrame({"numbers": list(range(10))})
        expected_df = DataFrame({"numbers": list(range(0, 19, 2))})
        result_df = start_df.map_column(column_name="numbers", func=lambda x: x * 2)
        self.assertEqual(expected_df, result_df)

    @unittest.skip
    def test_map_column_parallel(self):
        """..."""
        start_df = DataFrame({"numbers": list(range(10))})
        expected_df = DataFrame({"numbers": list(range(0, 19, 2))})
        result_df = start_df.map_column_parallel(column_name="numbers", func=lambda x: x * 2, max_workers=4)
        self.assertEqual(expected_df, result_df)

    def test_iterable(self):
        """Add."""
        expected = [(x, x**2) for x in range(10)]
        df = DataFrame(
            {
                "id": list(range(10)),
                "number": [x**2 for x in range(10)],
            },
        )
        for index, row in enumerate(df):
            self.assertEqual(row, {"id": expected[index][0], "number": expected[index][1]})

    def test_is_valid_column_good_weather(self):
        """Validating a column of consistent types."""
        column = [172, 173, 174]
        self.asserTrue(self.df.is_valid_column(column))

    def test_is_valid_column_bad_weather(self):
        """Validating a column of inconsistent types resulting in TypeError."""
        column = [172, 173, "another_type"]
        with self.assertRaises(TypeError) as err:
            self.df.is_valid_column(column)
        self.assertEqual(str(err.exception), "Inconsistent column types.")

    def test_filter_gw(self):
        """Test filtering in good weather."""

        def filter_fun(row):
            return row["d_size"] > 16

        to_be_filtered = DataFrame({"name": ["Гошо", "Пешо"], "d_size": [22, 10]})
        filtered = DataFrame({"name": ["Гошо"], "d_size": [22]})
        potential_mates = to_be_filtered.filter(filter_fun)
        self.assertEqual(potential_mates, filtered)

    def test_filter_called(self):
        """Test filtering uses its argument."""
        filter_fun = mock.Mock()
        to_be_filtered = DataFrame({"name": ["баба Илийца"], "has": ["кол"]})
        to_be_filtered.filter(filter_fun)
        self.assertEqual(filter_fun.mock_calls, [mock.call({"name": "баба Илийца", "has": "кол"})])

    def test_filter_bw(self):
        """Test filtering with uncallable object."""
        uncallable = "your ex"
        to_be_filtered = DataFrame({"name": ["Гошо", "Пешо"], "age": [30, 16]})
        with self.assertRaises(TypeError):
            to_be_filtered.filter(uncallable)


class TestValidators(unittest, TestCase):
    def test_requite_non_empty(self):
        def dummy_func(*args, **kwards):
            return None

        decorated = require_non_empty(dummy_func)
        decorated(DataFrame({"number": [1, 2, 3, 4, 7]}), column_number="number")
        self.assertEqual(result, None)

    def test_requite_non_empty(self):
        def dummy_func(*args, **kwards):
            return None

        decorated = require_non_empty(dummy_func)
        with self.assertRaises(TypeError) as err:
            decorated(DataFrame({}), column_number="number")
        self.assertEqual(str(err.exception), "Empty DF is not allowed")


if __name__ == "__main__":
    unittest.main()
