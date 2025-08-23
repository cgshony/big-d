"""A module with a basic DataFrame implementaiton from scratch. WIP"""

from prettytable import PrettyTable
from collections import defaultdict
from datetime import datetime


class DataFrame:
    """TODO:"""

    def __init__(self, column_content, schema=None):
        """Validate input data and initialize the Data frame."""
        for column in column_content.values():
            self.is_valid_column(column)
        self._validate_input(column_content)
        self.column_content = column_content
        self.schema.validate(self.column_content)

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
    
    def filter(self, func):
        """Filter the DataFrame..."""
        rows = filter(func, self.as_rows())
        return self.from_rows(list(rows))  # Immutable
    
    def map_column(self, column_name, func):
        """Apply a function to a column and return a new DataFrame."""
        if column_name not in self.column_content:
            raise KeyError(f"Column '{column_name}' not found.")
        column_content = dict(self.column_content)
        column_content[column_name] = [func(item) for item in column_content[column_name]]
        return DataFrame(column_content)

    def __getitem__(self, name):
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
        if name != 'column definitions' and name in self.column_content:
            self.is_valid_column(value)
            self.column_content[name] = value
        else:
            super().__setattr__(name, value)


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
                rows[index][col] = item
        return rows


df = DataFrame({"date": [1, 2, 3, 4]})

def validate_column_types(**column_restrictions):
    """Decorate a function to check if columns
    with a certain name have a specific content type."""

    def decorator(func):
        def decorated(df, **kwargs):
            if not isinstance(df, DataFrame):
                raise TypeEsrror(f"Object {df} is not a DataFrame.")
            for column, column_type in column_restrictions.items():
                if not isinstance(df[column][0], column_type):
                    raise TypeError(f"Type for column {column}, must be {column_type}")
            return func(df, **kwargs)
        return decorated
    return decorator

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

# lf = LazyFrame({"name": ["Гошо", "Тошо", "Пешо"], "age": [25, 35, 30], "height": [180, 140, 220]})
# lf = (lf.filter(lambda row: row["age"] >= 30)
#     .map_column("height", lambda x: x + 10)
#     .filter(lambda row: row["height"] >= 230))

# print(lf.collect())

class SchemaError(Exception):
    """Raised when a schema validation error occurs"""

class Schema:
    """Schema for validating data types for DataFrames"""
    def __init__(self, **kwargs):
        self._schema = kwargs

    def validate(self, column_contents):
         """Validate column definitions against schema expectations"""
         for column, column_type in self._schema.items():
             if column not in column_contents:
                 raise SchemaError(f"Column {column} must be present according to the schema.")
             if not isinstance(column_contents[column][0], column_type):
                 raise SchemaError(f"Typo for column{column} must be {column_type} according to the schema.")
    
df = DataFrame({"name": ["Гошо", "Тошо", "Пешо"], "age": [25, 35, 30], "height": [180, 140, 220]},
               schema=Schema(name=str, age=int, height=int))
print(df)

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
