"""Season discovery: which seasons Retrosheet currently publishes, and each one's zip URL.

A season disappearing from the downloads page would mean losing access to historical data
without anyone noticing; callers (build/validate) should treat that as a failure, not a
silent drop — this module only reports what's there now.
"""

from __future__ import annotations

import re
from typing import Any

DOWNLOADS_PAGE = "https://www.retrosheet.org/downloads/csvdownloads.html"
SEASON_ZIP = re.compile(r"downloads/(\d{4})/\1csvs\.zip")
USER_AGENT = "fishbaseball-data/0.1.0 (+https://github.com/FishOfPitt116/fishbaseball-data)"


class SeasonDiscoveryError(Exception):
    pass


def discover_seasons(session: Any) -> list[str]:
    """Every season year Retrosheet currently lists a CSV zip for, sorted ascending."""
    resp = session.get(DOWNLOADS_PAGE, timeout=30, headers={"User-Agent": USER_AGENT})
    resp.raise_for_status()
    years = sorted(set(SEASON_ZIP.findall(resp.text)))
    if not years:
        raise SeasonDiscoveryError(f"no season links found on {DOWNLOADS_PAGE}")
    return years


def season_zip_url(year: str) -> str:
    return f"https://www.retrosheet.org/downloads/{year}/{year}csvs.zip"
