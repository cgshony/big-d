"""A module with a basic DataFrame implementaiton from scratch. WIP"""

import csv
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from io import StringIO
from pathlib import Path

import config
import requests
from prettytable import PrettyTable

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
        """Verify if a column has members of the same type."""
        item_type = type(column[0])
        for item in column[1:]:
            if not isinstance(item, item_type):
                raise TypeError("Inconsistent column type.")
        return True

    @classmethod
    def from_rows(cls, rows):
        """Create a DataFrame object from some input lines"""
        column_content = defaultdict(list)
        for row in rows:
            for column, value in row.items():
                column_content[column].append(value)
            return cls(column_content)

    @classmethod
    def from_csv(cls, path):
        """Create a DataFrame obj, based in the contents of a .csv file."""
        if path.startswith("http"):
            response = requests.get(path, timeout=5)
            reader = csv.DictReader(StringIO(str(response.content, encoding="utf-8")))
            rows = list(reader)
            return None
        with Path.open(path, encoding="utf-8") as f:
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
        """Dimensions of the DataFrame (rows, columns)"""
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
        #TODO: implement better solution than _apply_to_chunk

        new_column = [item for chunk in processed_chunks for item in chunk]
        new_columns = dict(self.column_definitions)
        new_columns[column_name] = new_column
        return DataFrame(new_columns, schema=self.schema)

    def __getitem__(self, index_or_col_name):
        """Enable indexing by column name."""
        return self.column_content[name]

    def __setitem__(self, name, value):
        """Enable setting a new column."""
        self.validate_column
        self.column_content[name] = value

    def __getattr__(self,name):
        """Get column by attribute access."""
        return self.column_content[name]

    def __setattr__(self, name, value):
        """Set column via attribute access"""
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
        if len(rows) >= MAX_ROWS:
            table.add_rows(rows)
        else:
            table.add_rows(rows[:MAX_ROWS//2])
            table.add_row({"id":"...", "number":"..."})
            table.add_rows(rows[-MAX_ROWS//2:])
        return  f"DataFrame (2x3)\n{table!s}"

    def __bool__(self):
        return bool(self.column_content)

    def __iter__(self):
        self.index = 0
        return iter(self.as_rows())

    def as_rows(self):
        """Transform df as an i"""
        rows = []
        for col, row in self.column_content. items():
            for index, item in enumerate(row):
                if len(rows) <= index:
                    rows.append({})
                rows[index][col] = item
        return rows


class LazyFrame(DataFrame):
    """A delayed evaluation of DataFrame"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._transformations = [] #queue to store func chaining

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


path_to_csv = r"C:\Users\Ivona Ivanova\big-d\customers-100.csv"
df = DataFrame.from_csv_bulk([path_to_csv] * 4, max_workers=4)
print(df)

with config.config(max_rows=4):
    print(df)
