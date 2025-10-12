# Minimal DataFrame from Scratch

A basic in-memory DataFrame implementation in Python, created from scratch without using `pandas`. This is a work-in-progress project designed to mimic a subset of `pandas`-like functionality for tabular data handling and transformations.

## Features

- Create a `DataFrame` from:
  - Dictionaries
  - CSV files (local or remote)
  - Lists of dictionaries (rows)
- Data validation by column type
- Functional transformations:
  - `filter()`
  - `map_column()`
  - `map_column_parallel()` (using multiprocessing)
- Parallel CSV loading via `ThreadPoolExecutor`
- Lazy evaluation with `LazyFrame`
- Pretty printed tables using `PrettyTable`
- Schema validation (optional)
