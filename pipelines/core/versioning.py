from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date
from typing import Any


class UpstreamRegressionError(Exception):
    pass


@dataclass(frozen=True)
class Decision:
    release: bool
    reason: str  # first_release | new_version | content_changed | content_unchanged | forced


def _version_key(v: str) -> tuple[int, str]:
    return (int(v), "") if v.isdigit() else (-1, v)


def decide(
    previous: Mapping[str, Any] | None,
    sabr_version: str,
    content_hashes: Mapping[str, str],
    *,
    force: bool = False,
) -> Decision:
    """Whether to cut a new release. Same upstream version with identical table content
    (e.g. a BOM-only change) is not a release, unless `force` asks for one anyway (e.g. to
    carry a manifest-only change, such as a new field, out to a release). `force` never
    overrides the regression check: publishing older upstream data is always a mistake."""
    if previous is None:
        return Decision(True, "first_release")
    prev_version = str(previous["version"])
    if _version_key(sabr_version) < _version_key(prev_version):
        raise UpstreamRegressionError(
            f"upstream version {sabr_version} is older than published {prev_version}"
        )
    if _version_key(sabr_version) > _version_key(prev_version):
        return Decision(True, "new_version")
    prev_hashes = {t: e["content_sha256"] for t, e in previous["tables"].items()}
    if dict(content_hashes) != prev_hashes:
        return Decision(True, "content_changed")
    if force:
        return Decision(True, "forced")
    return Decision(False, "content_unchanged")


def next_tag(source: str, on: date, existing: Iterable[str]) -> str:
    """`<source>-YYYY-MM-DD`, with `-2`, `-3`... if that date is already taken."""
    taken = set(existing)
    base = f"{source}-{on.isoformat()}"
    if base not in taken:
        return base
    n = 2
    while f"{base}-{n}" in taken:
        n += 1
    return f"{base}-{n}"
