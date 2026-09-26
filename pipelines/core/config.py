from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass


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
