"""Grouping and aggregation support for DataFrame.groupby()."""

from collections import defaultdict

_AGGREGATIONS = {
    "sum": sum,
    "mean": lambda values: sum(values) / len(values),
    "min": min,
    "max": max,
    "count": len,
}


class GroupBy:
    """The result of `DataFrame.groupby(column)`, ready for aggregation."""

    def __init__(self, dataframe, by):
        """Group `dataframe`'s rows by the distinct values of column `by`."""
        self._dataframe = dataframe
        self._by = by
        self._groups = self._build_groups()

    def _build_groups(self):
        groups = defaultdict(list)
        for row in self._dataframe.as_rows():
            groups[row[self._by]].append(row)
        return groups

    @property
    def groups(self):
        """Mapping of group key to the list of row dicts in that group."""
        return dict(self._groups)

    def _data_columns(self):
        return [column for column in self._dataframe.column_content if column != self._by]

    def agg(self, spec):
        """Aggregate grouped columns.

        `spec` maps column name to an aggregation, each either one of
        "sum"/"mean"/"min"/"max"/"count" or a callable taking a list of
        values and returning a single value. Returns one row per group.
        """
        from big_d.dataframe import DataFrame  # local import avoids a circular import

        result_columns = defaultdict(list)
        for key in sorted(self._groups, key=str):
            rows = self._groups[key]
            result_columns[self._by].append(key)
            for column, aggregation in spec.items():
                func = _AGGREGATIONS[aggregation] if isinstance(aggregation, str) else aggregation
                result_columns[column].append(func([row[column] for row in rows]))
        return DataFrame(dict(result_columns))

    def _apply(self, name):
        return self.agg(dict.fromkeys(self._data_columns(), name))

    def sum(self):
        """Sum every non-group column within each group."""
        return self._apply("sum")

    def mean(self):
        """Average every non-group column within each group."""
        return self._apply("mean")

    def min(self):
        """Minimum of every non-group column within each group."""
        return self._apply("min")

    def max(self):
        """Maximum of every non-group column within each group."""
        return self._apply("max")

    def count(self):
        """Row count within each group."""
        from big_d.dataframe import DataFrame

        return DataFrame(
            {
                self._by: list(self._groups.keys()),
                "count": [len(rows) for rows in self._groups.values()],
            },
        )
