from datetime import datetime

from big_d.decorators import column, dataframe


@column.validate_column_types(date=datetime)
def extract_time_interval(df):
    """Extract the interval from the earliest to the latest
    date in a DataFrame.
    """
    dates = sorted(df["date"])
    return dates[-1] - dates[0]


@dataframe.require_non_empty
def avg(df, /, column_name):
    """Find the average value for a specific column."""
    column = df[column_name]
    return sum(column) / len(column)
