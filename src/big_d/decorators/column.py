"""Validators for column-based operations."""

from big_d.dataframe import DataFrame


def validate_column_types(**column_restrictions):
    """Decorate a function to check if columns
    with a certain name have a specific content type.
    """

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
