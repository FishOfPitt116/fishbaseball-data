"""Converting one season's zip to its final tables: cast with Retrosheet's dtypes/date
format/null markers (`pipelines.core.convert.read_tables`, generalized for this), then stamp
`season` onto every table that doesn't carry it natively (all but `game_info`).
"""

from __future__ import annotations

import re
from pathlib import Path

import polars as pl

from pipelines.core.convert import read_tables
from pipelines.retrosheet import RETROSHEET
from pipelines.retrosheet.schema import (
    CSV_DTYPES,
    NATIVE_SEASON_TABLES,
    PRIMARY_KEYS,
    SEASON_COLUMN,
)

# Each CSV inside a season zip is named e.g. "1927batting.csv"; strip the leading year so it
# matches `pipelines.retrosheet.tables.TABLES`'s plain "batting.csv" keys.
_YEAR_PREFIX = re.compile(r"^\d{4}")


def read_season(zip_path: Path, season: str) -> dict[str, pl.DataFrame]:
    tables = read_tables(
        zip_path,
        RETROSHEET,
        CSV_DTYPES,
        PRIMARY_KEYS,
        normalize_member_name=lambda name: _YEAR_PREFIX.sub("", name),
        date_format="%Y%m%d",
        null_markers=frozenset({"", "?"}),
    )
    year = int(season)
    return {
        name: (
            df
            if name in NATIVE_SEASON_TABLES
            else df.with_columns(pl.lit(year).cast(pl.Int16).alias(SEASON_COLUMN))
        )
        for name, df in tables.items()
    }
