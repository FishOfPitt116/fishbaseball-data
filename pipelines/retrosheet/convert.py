"""Converting one season's zip to its final tables: cast with Retrosheet's dtypes/date
format/null markers (`pipelines.core.convert.read_tables`, generalized for this), then stamp
`season` onto every table that doesn't carry it natively (all but `game_info`).
"""

from __future__ import annotations

import re
from pathlib import Path

import polars as pl

from pipelines.core.convert import dedupe_most_complete, read_tables
from pipelines.retrosheet.schema import (
    CSV_DTYPES,
    CSV_PRIMARY_KEYS,
    NATIVE_SEASON_TABLES,
    PRIMARY_KEYS,
    SEASON_COLUMN,
)

# Each CSV inside a season zip is named e.g. "1927batting.csv"; strip the leading year so it
# matches `pipelines.retrosheet.tables.TABLES`'s plain "batting.csv" keys.
_YEAR_PREFIX = re.compile(r"^\d{4}")


def read_season(zip_path: Path, season: str) -> dict[str, pl.DataFrame]:
    # Deferred: `pipelines.retrosheet.__init__` imports this module to build `RETROSHEET`
    # itself (for `PartitionConfig.convert`), so importing it back at module load time would
    # deadlock; by the time this function actually runs, that module is fully initialized.
    from pipelines.retrosheet import RETROSHEET

    tables = read_tables(
        zip_path,
        RETROSHEET,
        CSV_DTYPES,
        CSV_PRIMARY_KEYS,
        normalize_member_name=lambda name: _YEAR_PREFIX.sub("", name),
        date_format="%Y%m%d",
        null_markers=frozenset({"", "?"}),
        # Confirmed real: 1899's plays.csv lacks 16 columns every other season has.
        allow_missing_columns=True,
    )
    year = int(season)
    stamped = {
        name: (
            df
            if name in NATIVE_SEASON_TABLES
            else df.with_columns(pl.lit(year).cast(pl.Int16).alias(SEASON_COLUMN))
        )
        for name, df in tables.items()
    }
    # Confirmed real: Retrosheet's Negro Leagues-era data has 206 duplicate-key row pairs
    # (1899, 1933-1945), apparently merged from two disagreeing historical sources.
    return {name: dedupe_most_complete(df, PRIMARY_KEYS[name]) for name, df in stamped.items()}
