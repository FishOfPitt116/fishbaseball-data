"""Per-partition (e.g. per-season) change detection, reusing the same low-level download and
hash primitives Lahman's single-zip flow uses, just once per partition instead of once total.

Retrosheet serves real ETags with working conditional requests (verified against the live
site), so an unchanged partition costs one HEAD request, not a full download.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pipelines.core.detect import sha256_file
from pipelines.core.download import download

HTTP_NOT_MODIFIED = 304
DEFAULT_TIMEOUT = 30.0


@dataclass(frozen=True)
class PartitionCheck:
    key: str
    changed: bool
    etag: str | None  # the upstream ETag now, if the server sent one


def check_partition(
    url: str,
    key: str,
    *,
    session: Any,
    previous_etag: str | None,
    user_agent: str,
    timeout: float = DEFAULT_TIMEOUT,
) -> PartitionCheck:
    """A cheap HEAD-based check: unchanged if the server confirms our previous ETag is still
    current (304), or if it returns 200 with that same ETag (some servers don't honor
    conditional headers on HEAD). Anything else — including never having seen this partition
    before — is reported as changed, since there's no proof it's the same without a download.
    """
    headers = {"User-Agent": user_agent}
    if previous_etag:
        headers["If-None-Match"] = previous_etag
    resp = session.head(url, headers=headers, timeout=timeout, allow_redirects=True)
    if resp.status_code == HTTP_NOT_MODIFIED:
        return PartitionCheck(key=key, changed=False, etag=previous_etag)
    resp.raise_for_status()
    etag = resp.headers.get("ETag")
    if previous_etag and etag == previous_etag:
        return PartitionCheck(key=key, changed=False, etag=etag)
    return PartitionCheck(key=key, changed=True, etag=etag)


def download_partition(
    url: str, key: str, *, out_dir: Path, session: Any, user_agent: str
) -> tuple[Path, str]:
    """Download a partition's zip and return `(path, sha256)`."""
    zip_path = download((url,), out_dir / f"{key}.zip", session=session, user_agent=user_agent)
    return zip_path, sha256_file(zip_path)


@dataclass(frozen=True)
class PartitionDecision:
    release: bool
    reason: str  # first_release | content_changed | content_unchanged | forced
    changed_seasons: list[str]  # which partition keys actually need rebuilding this run


def decide_partitions(
    checks: dict[str, PartitionCheck], *, first_release: bool, force: bool = False
) -> PartitionDecision:
    """Whether to cut a new release, for a source whose change detection is per-partition
    rather than one upstream version string. Unlike Lahman's `decide()`, there's no upstream
    regression to guard against here: a partition either changed or it didn't."""
    changed = sorted(key for key, c in checks.items() if c.changed)
    if first_release:
        return PartitionDecision(True, "first_release", changed)
    if changed:
        return PartitionDecision(True, "content_changed", changed)
    if force:
        return PartitionDecision(True, "forced", changed)
    return PartitionDecision(False, "content_unchanged", changed)


def check_all_partitions(
    config: Any,
    *,
    session: Any,
    previous_upstream_seen: dict[str, Any] | None,
    user_agent: str,
    only: list[str] | None = None,
    delay: float = 0.0,
    progress: Callable[[str], None] | None = None,
) -> dict[str, PartitionCheck]:
    """Every discoverable partition's check result, keyed by partition key. `only` restricts
    to specific keys (e.g. a manual rebuild of particular seasons). `delay` paces requests
    (a sleep between each pair, none before the first) so a source with many partitions
    (Retrosheet: ~129 seasons) doesn't fire them all at a small, volunteer-run site at once.
    `progress`, if given, is called once per key — a long silent stretch checking ~129
    partitions sequentially has been observed getting a CI step cancelled by the platform."""
    assert config.partitions is not None
    keys = config.partitions.discover(session)
    if only is not None:
        unknown = sorted(set(only) - set(keys))
        if unknown:
            raise ValueError(f"unknown partition(s): {unknown}")
        keys = [k for k in keys if k in only]
    seen = previous_upstream_seen or {}
    results: dict[str, PartitionCheck] = {}
    for i, key in enumerate(keys):
        if i and delay:
            time.sleep(delay)
        results[key] = check_partition(
            config.partitions.url(key),
            key,
            session=session,
            previous_etag=(seen.get(key) or {}).get("etag"),
            user_agent=user_agent,
        )
        if progress:
            progress(f"checked {key}: {'changed' if results[key].changed else 'unchanged'}")
    return results
