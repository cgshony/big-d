"""Validators for DataFrame-based operations"""

from big_d.dataframe import DataFrame


def require_non_empty(func):
    """Decorate a function to require a DataFrame with size > (0, 0)."""

    def decorated(df, **kwargs):
        if not isinstance(df, DataFrame):
            raise TypeError(f"Object {df} is not a DataFrame.")
        if not df:
            raise TypeError(f"Empty DataFrame not allowed for {func.__name__}.")
        return func(df, **kwargs)
    return decorated
