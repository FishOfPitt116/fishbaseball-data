from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

from pipelines.core.config import SourceConfig
from pipelines.core.download import download

USER_AGENT = "fishbaseball-data/0.1.0 (+https://github.com/FishOfPitt116/fishbaseball-data)"
UPDATE_NOTE = re.compile(r"([A-Z][^<>.]*?\bupdated\b[^<>]*? on [A-Z][a-z]+ \d{1,2}, \d{4})")


class UpstreamFormatError(Exception):
    pass


@dataclass(frozen=True)
class PageInfo:
    version: str
    released: str  # ISO date
    update_note: str | None


def parse_page(html: str, pattern: re.Pattern[str]) -> PageInfo:
    m = pattern.search(html)
    if not m:
        raise UpstreamFormatError(f"version pattern {pattern.pattern!r} not found on page")
    version, released_text = m.group(1), m.group(2)
    try:
        released = datetime.strptime(released_text, "%B %d, %Y").date().isoformat()
    except ValueError as e:
        raise UpstreamFormatError(f"unparseable release date {released_text!r}") from e
    note = UPDATE_NOTE.search(html)
    return PageInfo(version, released, note.group(1).strip() if note else None)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def fetch_latest(url: str, session: Any) -> dict[str, Any] | None:
    """Our own `latest.json`; None when there is no previous release (404)."""
    resp = session.get(url, timeout=30, headers={"User-Agent": USER_AGENT})
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()


def detect(
    config: SourceConfig,
    *,
    out_dir: Path,
    latest_url: str,
    session: Any = None,
    source_url: str | None = None,
    force: bool = False,
) -> dict[str, Any]:
    session = session or requests.Session()
    out_dir.mkdir(parents=True, exist_ok=True)
    page = session.get(config.page_url, timeout=30, headers={"User-Agent": USER_AGENT})
    page.raise_for_status()
    info = parse_page(page.text, config.version_pattern)

    zip_path = download(
        config.download_urls,
        out_dir / f"{config.name}.zip",
        source_url=source_url,
        session=session,
        user_agent=USER_AGENT,
    )
    digest = sha256_file(zip_path)

    seen = ((fetch_latest(latest_url, session)) or {}).get("upstream_seen")
    if force:
        changed, reason = True, "forced"
    elif seen is None:
        changed, reason = True, "new_version"
    elif seen.get("sha256") == digest:
        changed, reason = False, "none"
    elif seen.get("version") != info.version:
        changed, reason = True, "new_version"
    else:
        changed, reason = True, "files_changed"

    result: dict[str, Any] = {
        "changed": changed,
        "reason": reason,
        "sabr_version": info.version,
        "released": info.released,
        "update_note": info.update_note,
        "upstream_sha256": digest,
        "zip_path": str(zip_path),
    }
    (out_dir / "detect.json").write_text(json.dumps(result, indent=2) + "\n")
    return result
