from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import polars as pl


@dataclass(frozen=True)
class PartitionConfig:
    """How a source's per-partition (e.g. per-season) zips are discovered and fetched.

    A partition is a zip containing CSVs for one slice of the data (Retrosheet: one season),
    downloaded, converted and hashed independently, the same way Lahman's single zip is —
    `pipelines.core.convert`/`download` already operate on one zip at a time and need no
    changes to run per partition. What's new lives in the per-partition orchestration
    (detect, versioning, manifest, publish), not in these low-level primitives.
    """

    discover: Callable[[Any], list[str]]  # session -> sorted partition keys, e.g. year strings
    url: Callable[[str], str]  # partition key -> zip download URL
    partitioned_tables: frozenset[str]  # this source's tables that are split per partition
    convert: Callable[[Any, str], Mapping[str, pl.DataFrame]]  # zip path, key -> that key's tables


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
    partitions: PartitionConfig | None = None  # None: a single zip, like Lahman
    extra_notice: str = ""  # one source-specific sentence appended to NOTICE.md


@dataclass(frozen=True)
class SourceSchema:
    """What a source's tables must look like: owned by the source, used by the generic stages."""

    version: int
    dtypes: Mapping[str, Mapping[str, pl.DataType]]
    primary_keys: Mapping[str, Sequence[str]]
    year_columns: Mapping[str, str]
    validate: Callable[..., list[str]]  # (tables, *, full_dataset: bool) -> failures
