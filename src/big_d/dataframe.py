"""A module with a basic DataFrame implementaiton from scratch. WIP."""

import csv
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from io import StringIO
from pathlib import Path

import requests
from prettytable import PrettyTable

from big_d import config

RAND_MIN = 0
RAND_MAX = 10000


class DataFrame:
    """TODO:"""

    def __init__(self, column_content, schema=None):
        """Validate input data and initialize the Data frame."""
        self._validate_input(column_content)
        for column in column_content.values():
            self.is_valid_column(column)
        super().__setattr__("column_content", column_content)
        super().__setattr__("schema", schema)
        if schema:
            schema.validate(self.column_content)

    def _validate_input(self, column_content):
        if not isinstance(column_content, dict):
            raise ValueError("Input must be a dictionary.")

    def is_valid_column(self, column):
        """Verify if a column has members of the same type.

        `None` is treated as a null/missing marker and is allowed
        alongside any type.
        """
        non_null = [item for item in column if item is not None]
        if not non_null:
            return True
        item_type = type(non_null[0])
        for item in non_null[1:]:
            if not isinstance(item, item_type):
                raise TypeError("Inconsistent column type.")
        return True

    @classmethod
    def from_rows(cls, rows):
        """Create a DataFrame object from some input lines."""
        column_content = defaultdict(list)
        for row in rows:
            for column, value in row.items():
                column_content[column].append(value)
        if not column_content:
            return None
        return cls(dict(column_content))

    @classmethod
    def from_csv(cls, path):
        """Create a DataFrame obj, based in the contents of a .csv file."""
        if path.startswith("http"):
            response = requests.get(path, timeout=5)
            reader = csv.DictReader(StringIO(str(response.content, encoding="utf-8")))
            rows = list(reader)
            return None
        with Path(path).open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        return cls.from_rows(rows)

    @classmethod
    def from_csv_bulk(cls, paths, max_workers):
        """Create DataFrame objects, based on the contents of multiple *.csv files."""
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            return tuple(executor.map(cls.from_csv, paths))

    @property
    def shape(self):
        """Dimensions of the DataFrame (rows, columns)."""
        assert self
        len_rows = max(len(row) for row in self.column_content.values())
        len_cols = len(self.column_content)
        return len_rows, len_cols

    def filter(self, func):
        """Filter the DataFrame..."""
        rows = filter(func, self.as_rows())
        return self.from_rows(list(rows))  # Immutable

    def map_column(self, column_name, func):
        """Apply a function to a column and return a new DataFrame.

        Examples:
        =========

        urls = [
        "https://www.timestored.com/data/sample/chickweight.csv",
        "https://www.timestored.com/data/sample/dowjones.csv",
        "https://www.timestored.com/data/sample/healthexp.csv",
        "https://www.timestored.com/data/sample/iris.csv",
        "https://www.timestored.com/data/sample/iso10383_mic.csv",
        "https://www.timestored.com/data/sample/sunspots.csv",
        "https://www.timestored.com/data/sample/taxis.csv",
        "https://www.timestored.com/data/sample/titanic.csv",
        ]
        df = DataFrame.from_csv(urls[5])

        """
        if column_name not in self.column_content:
            raise KeyError(f"Column '{column_name}' not found.")
        column_content = dict(self.column_content)
        column_content[column_name] = [func(item) for item in column_content[column_name]]
        return DataFrame(column_content)

    def map_column_parallel(self, column_name, func, max_workers):
        """TODO:"""
        column = self[column_name]
        chunk_size = len(column) // max_workers
        chunks = [column[index : index + chunk_size] for index in range(0, len(column), chunk_size)]

        def _apply_to_chunk(args):
            func, chunk = args
            return [func(item) for item in chunk]

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            processed_chunks = tuple(executor.map(_apply_to_chunk, [(func, chunk) for chunk in chunks]))
        # TODO: implement better solution than _apply_to_chunk

        new_column = [item for chunk in processed_chunks for item in chunk]
        new_columns = dict(self.column_content)
        new_columns[column_name] = new_column
        return DataFrame(new_columns, schema=self.schema)

    def __eq__(self, other):
        """Compare dataframe objects."""
        return self.column_content == other.column_content

    def __hash__(self):
        """Get dataframe hash."""
        return hash(str(self))

    def __getitem__(self, key):
        """Select a column, a subset of columns, or a subset of rows.

        - `df["col"]` -> that column, as a list.
        - `df[["col_a", "col_b"]]` -> a new DataFrame with just those columns.
        - `df[3]` -> the row at that position, as a dict.
        - `df[1:5]` -> a new DataFrame with those rows.
        - `df[[True, False, ...]]` -> a new DataFrame kept where the mask is True.
        """
        if isinstance(key, str):
            return self.column_content[key]
        if isinstance(key, slice):
            return self.from_rows(self.as_rows()[key])
        if isinstance(key, int):
            return self.as_rows()[key]
        if isinstance(key, (list, tuple)):
            if key and all(isinstance(item, str) for item in key):
                return DataFrame({column: self.column_content[column] for column in key}, schema=self.schema)
            if key and all(isinstance(item, bool) for item in key):
                rows = [row for row, keep in zip(self.as_rows(), key, strict=True) if keep]
                return self.from_rows(rows)
            raise TypeError("List indices must be all column names (str) or all booleans (a mask).")
        raise TypeError(f"Unsupported index type: {type(key).__name__}")

    def __setitem__(self, name, value):
        """Enable setting a new column."""
        self.is_valid_column(value)
        self.column_content[name] = value

    def __getattr__(self, name):
        """Get column by attribute access."""
        return self.column_content[name]

    def __setattr__(self, name, value):
        """Set column via attribute access."""
        if name != "column definitions" and name in self.column_content:
            self.is_valid_column(value)
            self.column_content[name] = value
        else:
            super().__setattr__(name, value)

    def __str__(self):
        """Represent DataFrame as a string with it's size and contents."""
        table = PrettyTable()
        table.field_names = self.column_content.keys()
        rows = list(zip(*self.column_content.values()))
        max_rows = config.MAX_ROWS
        if len(rows) <= max_rows:
            table.add_rows(rows)
        else:
            half = max_rows // 2
            table.add_rows(rows[:half])
            table.add_row(["..."] * len(table.field_names))
            table.add_rows(rows[-half:])
        num_rows, num_cols = self.shape
        return f"DataFrame ({num_rows}x{num_cols})\n{table!s}"

    def __bool__(self):
        return bool(self.column_content)

    def __iter__(self):
        self.index = 0
        return iter(self.as_rows())

    def as_rows(self):
        """Transform df as an i."""
        rows = []
        for col, row in self.column_content.items():
            for index, item in enumerate(row):
                if len(rows) <= index:
                    rows.append({})
                rows[index][col] = item
        return rows

    def isna(self):
        """Return a same-shaped DataFrame of booleans marking `None` values."""
        return DataFrame({column: [item is None for item in values] for column, values in self.column_content.items()})

    def dropna(self):
        """Return a new DataFrame with every row that contains a `None` value removed."""
        rows = [row for row in self.as_rows() if not any(value is None for value in row.values())]
        return self.from_rows(rows)

    def _validate_join_keys(self, other, keys):
        missing_left = [key for key in keys if key not in self.column_content]
        missing_right = [key for key in keys if key not in other.column_content]
        if missing_left or missing_right:
            raise KeyError(f"Join column(s) missing: left={missing_left}, right={missing_right}")

    def _join_left_rows(self, key_of, combine, right_by_key, how):
        """Match every left row against `right_by_key`, applying `how`'s unmatched-row policy."""
        result_rows = []
        matched_keys = set()
        for left_row in self.as_rows():
            key = key_of(left_row)
            matches = right_by_key.get(key, [])
            if matches:
                matched_keys.add(key)
                result_rows.extend(combine(left_row, right_row) for right_row in matches)
            elif how in ("left", "outer"):
                result_rows.append(combine(left_row, None))
        return result_rows, matched_keys

    def merge(self, other, on, how="inner", suffixes=("_x", "_y")):
        """Join with another DataFrame on one or more shared columns, via a hash join.

        `on` is a column name, or a list of column names, present in
        both frames. `how` is one of "inner", "left", "right", "outer".
        Non-key columns present in both frames are disambiguated with
        `suffixes`.
        """
        if how not in {"inner", "left", "right", "outer"}:
            raise ValueError(f"Unsupported join type: {how!r}")
        keys = [on] if isinstance(on, str) else list(on)
        self._validate_join_keys(other, keys)

        left_columns = [column for column in self.column_content if column not in keys]
        right_columns = [column for column in other.column_content if column not in keys]
        shared = set(left_columns) & set(right_columns)

        def key_of(row):
            return tuple(row[key] for key in keys)

        def combine(left_row, right_row):
            merged = {key: (left_row or right_row)[key] for key in keys}
            for column in left_columns:
                name = f"{column}{suffixes[0]}" if column in shared else column
                merged[name] = left_row[column] if left_row is not None else None
            for column in right_columns:
                name = f"{column}{suffixes[1]}" if column in shared else column
                merged[name] = right_row[column] if right_row is not None else None
            return merged

        right_rows = other.as_rows()
        right_by_key = defaultdict(list)
        for row in right_rows:
            right_by_key[key_of(row)].append(row)

        result_rows, matched_keys = self._join_left_rows(key_of, combine, right_by_key, how)
        if how in ("right", "outer"):
            result_rows.extend(
                combine(None, right_row) for right_row in right_rows if key_of(right_row) not in matched_keys
            )
        return self.from_rows(result_rows)

    def groupby(self, by):
        """Group rows by the values of column `by`, ready for `.agg(...)` or a built-in aggregation."""
        from big_d.groupby import GroupBy  # local import avoids a circular import

        return GroupBy(self, by)

    def value_counts(self, column_name):
        """Count occurrences of each distinct value in a column, most common first."""
        counts = Counter(self.column_content[column_name]).most_common()
        return DataFrame({column_name: [value for value, _ in counts], "count": [count for _, count in counts]})

    def fillna(self, value):
        """Return a new DataFrame with `None` values replaced.

        `value` is either a single value applied to every column, or a
        dict mapping column name to the value used for that column.
        """
        column_content = {}
        for column, values in self.column_content.items():
            fill = value.get(column, None) if isinstance(value, dict) else value
            column_content[column] = [fill if item is None else item for item in values]
        return DataFrame(column_content, schema=self.schema)


class LazyFrame(DataFrame):
    """A delayed evaluation of DataFrame."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._transformations = []  # queue to store func chaining

    def filter(self, *args, **kwargs):
        self._transformations.append(("filter", (args, kwargs)))
        return self

    def map_column(self, *args, **kwargs):
        self._transformations.append(("map_column", (args, kwargs)))
        return self

    def collect(self):
        df = DataFrame(self.column_content)
        for transform_func, arguments in self._transformations:
            args, kwargs = arguments
            df = getattr(df, transform_func)(*args, **kwargs)
        return df

    def __str__(self):
        """Represent DataFrame as a string with it's size and contents."""
        table = PrettyTable()
        table.field_names = self.column_content.keys()
        table.add_rows(row for row in zip(*self.column_content.values()))
        return f"LazyFrame ({self.shape[0]}x{self.shape[1]})"


if __name__ == "__main__":
    path_to_csv = Path(__file__).resolve().parents[2] / "customers-100.csv"
    dfs = DataFrame.from_csv_bulk([str(path_to_csv)] * 4, max_workers=4)

    with config.config(max_rows=4):
        print(dfs[0])
