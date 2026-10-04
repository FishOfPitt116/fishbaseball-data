"""Per-partition (e.g. per-season) change detection, reusing the same low-level download and
hash primitives Lahman's single-zip flow uses, just once per partition instead of once total.

Retrosheet serves real ETags with working conditional requests (verified against the live
site), so an unchanged partition costs one HEAD request, not a full download.
"""

from __future__ import annotations

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


def check_all_partitions(
    config: Any,
    *,
    session: Any,
    previous_upstream_seen: dict[str, Any] | None,
    user_agent: str,
    only: list[str] | None = None,
) -> dict[str, PartitionCheck]:
    """Every discoverable partition's check result, keyed by partition key. `only` restricts
    to specific keys (e.g. a manual rebuild of particular seasons)."""
    assert config.partitions is not None
    keys = config.partitions.discover(session)
    if only is not None:
        unknown = sorted(set(only) - set(keys))
        if unknown:
            raise ValueError(f"unknown partition(s): {unknown}")
        keys = [k for k in keys if k in only]
    seen = previous_upstream_seen or {}
    return {
        key: check_partition(
            config.partitions.url(key),
            key,
            session=session,
            previous_etag=(seen.get(key) or {}).get("etag"),
            user_agent=user_agent,
        )
        for key in keys
    }
