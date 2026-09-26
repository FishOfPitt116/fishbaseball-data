from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True)
class SourceConfig:
    name: str  # "lahman"
    page_url: str
    version_pattern: re.Pattern[str]
    download_urls: tuple[str, ...]  # tried in order
    tables: Mapping[str, str]  # "Batting.csv" -> "batting"
    columns: Mapping[str, Mapping[str, str]]  # table -> {"playerID": "player_id", ...}
    license: str
    attribution: str
    row_shrink_tolerance: float = 0.005


@dataclass(frozen=True)
class SourceSchema:
    """What a source's tables must look like: owned by the source, used by the generic stages."""

    version: int
    dtypes: Mapping[str, Mapping[str, pl.DataType]]
    primary_keys: Mapping[str, Sequence[str]]
    year_columns: Mapping[str, str]
    validate: Callable[..., list[str]]  # (tables, *, full_dataset: bool) -> failures
