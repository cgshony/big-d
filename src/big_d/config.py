"""Configuration."""

MAX_ROWS = 10000

class _Config:
    def __init__(self, max_rows):
        self.max_rows = max_rows

    def __enter__(self):
        global MAX_ROWS
        self._max_rows_former = MAX_ROWS
        MAX_ROWS = self.max_rows

    def __exit__(self, exc_type, exc_val, exc_tb):
        global MAX_ROWS
        MAX_ROWS = self._max_rows_former


config = _Config
