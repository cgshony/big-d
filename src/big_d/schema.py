"""Schemas."""

from big_d.exceptions import SchemaError


class Schema:
    """Schema for validating data types for DataFrames."""

    def __init__(self, **kwargs):
        self._schema = kwargs

    def __str__(self):
        """Represent with keyword arguments included."""
        kwargs = ", ".join(f"{key}={value.__name__}" for key, value in self._schema.items())
        return f"{self.__class__.__name__}({kwargs})"

    def validate(self, column_contents):
        """Validate column definitions against schema expectations."""
        for column, column_type in self._schema.items():
            if column not in column_contents:
                raise SchemaError(f"Column {column} must be present according to the schema.")
            values = [value for value in column_contents[column] if value is not None]
            if values and not isinstance(values[0], column_type):
                raise SchemaError(f"Type for column {column} must be {column_type} according to the schema.")
