# Minimal DataFrame from Scratch

A basic in-memory DataFrame implementation in Python, created from scratch without using `pandas`. This is a work-in-progress project designed to mimic a subset of `pandas`-like functionality for tabular data handling and transformations.

## Features

- Create a `DataFrame` from:
  - Dictionaries
  - CSV files (local or remote)
  - Lists of dictionaries (rows)
- Data validation by column type
- Indexing: `df["col"]`, `df[["col_a", "col_b"]]`, `df[3]`, `df[1:5]`, `df[boolean_mask]`
- Functional transformations:
  - `filter()`
  - `map_column()`
  - `map_column_parallel()` (using multiprocessing)
- Missing-value handling: `isna()`, `dropna()`, `fillna(value)` (using `None` as the null marker)
- Aggregation: `groupby(column).agg({...})`, plus `.sum()`/`.mean()`/`.min()`/`.max()`/`.count()`, and `value_counts()`
- Joins: `merge(other, on=..., how="inner"|"left"|"right"|"outer")` (hash join)
- Parallel CSV loading via `ThreadPoolExecutor`
- Lazy evaluation with `LazyFrame`
- Pretty printed tables using `PrettyTable`
- Schema validation (optional, tolerates `None`)

## Development

```bash
pip install -e ".[dev]"
pytest tests/
ruff check .
pdoc big_d -o docs   # generate local API docs
```

CI runs ruff and pytest on every push (`.github/workflows/ci.yml`); pushes to `main` build and publish pdoc-generated docs to GitHub Pages (`.github/workflows/docs.yml`).
