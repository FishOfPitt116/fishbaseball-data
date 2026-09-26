from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl

from pipelines.core.config import SourceConfig
from pipelines.core.convert import content_hash


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def release_url(repo: str, tag: str, filename: str) -> str:
    return f"https://github.com/{repo}/releases/download/{tag}/{filename}"


def compute_changes(
    previous: Mapping[str, Any] | None, new_tables: Mapping[str, Mapping[str, Any]]
) -> dict[str, Any]:
    if previous is None:
        return {"previous": None, "tables_added": [], "tables_changed": [], "row_deltas": {}}
    prev_tables = previous["tables"]
    added = sorted(set(new_tables) - set(prev_tables))
    changed = sorted(
        t
        for t in set(new_tables) & set(prev_tables)
        if new_tables[t]["content_sha256"] != prev_tables[t]["content_sha256"]
    )
    deltas = {
        t: new_tables[t]["rows"] - prev_tables[t]["rows"]
        for t in sorted(set(new_tables) & set(prev_tables))
        if new_tables[t]["rows"] != prev_tables[t]["rows"]
    }
    return {
        "previous": previous["tag"],
        "tables_added": added,
        "tables_changed": changed,
        "row_deltas": deltas,
    }


def build_manifest(
    config: SourceConfig,
    *,
    tag: str,
    repo: str,
    upstream: Mapping[str, Any],
    tables: Mapping[str, pl.DataFrame],
    parquet_dir: Path,
    primary_keys: Mapping[str, Sequence[str]],
    year_columns: Mapping[str, str],
    schema_version: int,
    pipeline_version: str,
    built_at: datetime,
    previous: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    entries: dict[str, dict[str, Any]] = {}
    for name in sorted(tables):
        df, file = tables[name], f"{name}.parquet"
        path = parquet_dir / file
        year_col = year_columns.get(name)
        entries[name] = {
            "file": file,
            "url": release_url(repo, tag, file),
            "sha256": _sha256(path),
            "content_sha256": content_hash(df, primary_keys[name]),
            "rows": df.height,
            "bytes": path.stat().st_size,
            "min_year": df[year_col].min() if year_col else None,
            "max_year": df[year_col].max() if year_col else None,
        }
    return {
        "source": config.name,
        "tag": tag,
        "version": upstream["version"],
        "schema_version": schema_version,
        "built_at": built_at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "pipeline_version": pipeline_version,
        "upstream": dict(upstream),
        "license": config.license,
        "attribution": config.attribution,
        "column_map": {t: dict(config.columns[t]) for t in sorted(tables)},
        "changes": compute_changes(previous, entries),
        "tables": entries,
    }
