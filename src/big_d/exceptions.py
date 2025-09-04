"""Exceptions for Dataframe."""


class DataframeError(Exception):
    """General DataFrame Exception."""


class SchemaError(Exception):
    """Raised when a schema validation error occurs."""

    def __init__(self, msg, schema=None):
        self.schema = schema
        super().__init__(msg)
