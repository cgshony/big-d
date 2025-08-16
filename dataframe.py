"""A module with a basic DataFrame implementaiton from scratch. WIP"""

from prettytable import PrettyTable
from collections import defaultdict
from datetime import datetime


class DataFrame:
    """TODO:"""

    def __init__(self, column_content):
        """Validate input data and initialize the Data frame."""
        for column in column_content.values():
            self.validate_column(column)
        self._validate_input(column_content)
        self.column_content = column_content

    @classmethod
    def from_rows(cls, rows):
        """Create a DataFrame object from """
        column_content = defaultdict(list)
        for row in rows:
            for column, value in row.items():
                column_content[column].append(value)
            return cls(column_content)

    def is_valid_column(self,column):
            """Verify if column elements are consistent data types"""
            item_type = type(column[0])
            for item in column[1:]:
                if not isinstance(item, item_type):
                    raise TypeError('Inconsistent column type')
                return True

    def _validate_input(self, column_content):
        if not isinstance(column_content, dict):
            raise ValueError('Cannot instantiate. Please use a dictionary.')

    @property
    def shape(self):
        """Dimensions of the DataFrame (rows, columns)"""
        assert self
        len_rows = max(len(row) for row in self.column_content.values())
        len_cols = len(self.column_content)
        return len_rows, len_cols
    
    
    def __getitem__(self, name):
        """Enable indexing by column name."""
        return self.column_content[name]
    
    def __setitem__(self, name, value):
        """Enable setting a new column."""
        self.validate_column
        self.column_content[name] = value

    def __str__(self):
        """Represent DataFrame as a string with it's size and contents."""
        table = PrettyTable()
        table.field_names = self.column_definitions.keys()
        table.add_rows(row for row in zip(*self.column_definitions.values()))        
        return  f"DataFrame (2x3)\n{table!s}"
    
    def __bool__(self):
        return bool(self.column_content)
    
    def as_rows(self):
        """transform df as an i"""
        rows = []
        for col, row in self.column_content. items():
            for index, item in enumerate(row):
                if len(rows) <= index:
                    rows.append({})
                row[index][col] = item


df = DataFrame({"date": [1, 2, 3, 4]})


    # def map_column(self, column_name, func):
    #     column_content = dict(self.column_content.items)
    #     column = self.column_content[column_name]
    #     column = [func(item) for item in column]
    #     column_content[column_name] = column
    #     return DataFrame(column_content)


def validate_column_types(**column_restrictions):
    """Decorate a function to check if columns
    with a certain name have a specific content type."""

    def decorator(func):
        def decorated(df, **kwargs):
            if not isinstance(df, DataFrame):
                raise TypeError(f"Object {df} is not a DataFrame.")
            for column, column_type in column_restrictions.items():
                if not isinstance(df[column][0], column_type):
                    raise TypeError(f"Type for column {column}, must be {column_type}")
            return func(df, **kwargs)
        return decorated
    return decorator

@validate_column_types(date=datetime)
def extract_time_interval(df):
    """Extract the interval from the earliest to the latest
    date in a DataFrame."""
    dates = sorted(df["date"])
    return dates[-1] - dates[0]

def require_non_empty(func):
    """Decorate a function to require a DataFrame with size > (0, 0)."""
    def decorated(df, **kwargs):
        if not isinstance(df, DataFrame):
            raise TypeError(f"Object {df} is not a DataFrame.")
        if not df:
            raise TypeError(f"Empty DataFrame not allowed for {func.__name__}.")
        return func(df, **kwargs)

@require_non_empty
def avg(df, /, column_name):
    """Find the average value for a specific column."""
    column = df[column_name]
    return sum(column) / len(column)
