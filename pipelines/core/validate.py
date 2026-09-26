from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import polars as pl


class ValidationError(Exception):
    def __init__(self, failures: list[str]):
        self.failures = failures
        super().__init__("validation failed:\n" + "\n".join(f"  - {f}" for f in failures))


def compare_with_previous(
    tables: Mapping[str, pl.DataFrame],
    previous: Mapping[str, Any] | None,
    *,
    tolerance: float,
    year_columns: Mapping[str, str],
) -> None:
    """No table or column removed; row counts >= previous x (1 - tolerance); max year does
    not go backwards. Skipped on the first run (previous is None)."""
    if previous is None:
        return
    failures: list[str] = []
    prev_tables: Mapping[str, Any] = previous.get("tables", {})
    prev_columns: Mapping[str, Mapping[str, str]] = previous.get("column_map", {})

    if removed := sorted(set(prev_tables) - set(tables)):
        failures.append(f"table(s) removed since previous release: {removed}")
    for name, df in tables.items():
        if name in prev_columns:
            if gone := sorted(set(prev_columns[name].values()) - set(df.columns)):
                failures.append(f"{name}: column(s) removed since previous release: {gone}")
        prev = prev_tables.get(name)
        if not prev:
            continue
        floor = prev["rows"] * (1 - tolerance)
        if df.height < floor:
            failures.append(
                f"{name}: rows shrank from {prev['rows']} to {df.height} (floor {floor:.0f})"
            )
        col = year_columns.get(name)
        prev_max = prev.get("max_year")
        if col and prev_max is not None:
            cur_max = df[col].max()
            if cur_max is None or cur_max < prev_max:
                failures.append(f"{name}: max {col} went from {prev_max} to {cur_max}")
    if failures:
        raise ValidationError(failures)
