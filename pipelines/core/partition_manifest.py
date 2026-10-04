"""Manifest building for sources that mix plain single-file tables (built and republished in
full every release, exactly like Lahman's) with partitioned tables — one Parquet file per
partition (Retrosheet: per season), where an unchanged partition's manifest entry keeps
pointing at whichever older release still holds that file, rather than being re-uploaded.

`build_partitioned_manifest` reuses `build_manifest` unchanged for the single-file tables
(so Lahman's own manifest building is untouched), and adds the partitioned-table entries and
their `changes` tracking alongside.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from pipelines.core.convert import content_hash, write_parquet
from pipelines.core.manifest import build_manifest, release_url


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _build_partition_entry(
    table: str,
    season: str,
    df: pl.DataFrame,
    *,
    repo: str,
    tag: str,
    primary_key: Sequence[str],
    parquet_dir: Path,
) -> dict[str, Any]:
    file = f"{table}_{season}.parquet"
    path = parquet_dir / file
    write_parquet(df, path)
    return {
        "file": file,
        "url": release_url(repo, tag, file),
        "sha256": _sha256(path),
        "content_sha256": content_hash(df, primary_key),
        "rows": df.height,
        "bytes": path.stat().st_size,
    }


def _build_partitioned_table_entry(
    table: str,
    *,
    fresh: Mapping[str, pl.DataFrame],
    previous_entry: Mapping[str, Any] | None,
    repo: str,
    tag: str,
    primary_key: Sequence[str],
    parquet_dir: Path,
    dtypes: Mapping[str, pl.DataType],
    season_column: str,
) -> dict[str, Any]:
    partitions: dict[str, Any] = {}
    for season, entry in (previous_entry or {}).get("partitions", {}).items():
        if season not in fresh:
            partitions[season] = dict(entry)  # untouched: still points at its own old release
    for season, df in fresh.items():
        partitions[season] = _build_partition_entry(
            table, season, df, repo=repo, tag=tag, primary_key=primary_key, parquet_dir=parquet_dir
        )
    seasons = sorted(int(s) for s in partitions)
    return {
        "partitioned": True,
        "columns": {c: str(t) for c, t in dtypes.items()},
        "primary_key": list(primary_key),
        "season_column": season_column,
        "partitions": partitions,
        "rows": sum(p["rows"] for p in partitions.values()),
        "min_year": seasons[0] if seasons else None,
        "max_year": seasons[-1] if seasons else None,
    }


def _compute_partitioned_changes(
    previous: Mapping[str, Any] | None,
    new_tables: Mapping[str, Mapping[str, Any]],
    partitioned_fresh: Mapping[str, Mapping[str, pl.DataFrame]],
) -> dict[str, Any]:
    if previous is None:
        return {
            "previous": None,
            "tables_added": [],
            "tables_changed": [],
            "partitions_changed": {},
            "row_deltas": {},
        }
    prev_tables = previous["tables"]
    added = sorted(set(new_tables) - set(prev_tables))
    changed = sorted(
        t
        for t in set(new_tables) & set(prev_tables)
        if not new_tables[t].get("partitioned")
        and new_tables[t]["content_sha256"] != prev_tables[t].get("content_sha256")
    )
    partitions_changed = {t: sorted(seasons) for t, seasons in partitioned_fresh.items() if seasons}

    row_deltas: dict[str, Any] = {}
    for t in sorted(set(new_tables) & set(prev_tables)):
        if new_tables[t].get("partitioned"):
            prev_partitions = prev_tables[t].get("partitions", {})
            deltas = {}
            for season in partitioned_fresh.get(t, {}):
                new_rows = new_tables[t]["partitions"][season]["rows"]
                prev_rows = prev_partitions.get(season, {}).get("rows", 0)
                if new_rows != prev_rows:
                    deltas[season] = new_rows - prev_rows
            if deltas:
                row_deltas[t] = deltas
        elif new_tables[t]["rows"] != prev_tables[t]["rows"]:
            row_deltas[t] = new_tables[t]["rows"] - prev_tables[t]["rows"]
    return {
        "previous": previous["tag"],
        "tables_added": added,
        "tables_changed": changed,
        "partitions_changed": partitions_changed,
        "row_deltas": row_deltas,
    }


def build_partitioned_manifest(
    config: Any,
    *,
    tag: str,
    repo: str,
    upstream: Mapping[str, Any],
    unpartitioned_tables: Mapping[str, pl.DataFrame],
    partitioned_tables_fresh: Mapping[str, Mapping[str, pl.DataFrame]],
    parquet_dir: Path,
    primary_keys: Mapping[str, Sequence[str]],
    season_columns: Mapping[str, str],
    dtypes: Mapping[str, Mapping[str, pl.DataType]],
    schema_version: int,
    pipeline_version: str,
    built_at: datetime,
    previous: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    for name, df in unpartitioned_tables.items():
        write_parquet(df, parquet_dir / f"{name}.parquet")
    manifest = build_manifest(
        config,
        tag=tag,
        repo=repo,
        upstream=upstream,
        tables=unpartitioned_tables,
        parquet_dir=parquet_dir,
        primary_keys=primary_keys,
        year_columns=season_columns,
        dtypes=dtypes,
        schema_version=schema_version,
        pipeline_version=pipeline_version,
        built_at=built_at,
        previous=previous,
    )
    for table, fresh in partitioned_tables_fresh.items():
        prev_entry = (previous or {}).get("tables", {}).get(table)
        manifest["tables"][table] = _build_partitioned_table_entry(
            table,
            fresh=fresh,
            previous_entry=prev_entry,
            repo=repo,
            tag=tag,
            primary_key=primary_keys[table],
            parquet_dir=parquet_dir,
            dtypes=dtypes[table],
            season_column=season_columns[table],
        )
    manifest["column_map"] = {
        t: dict(config.columns[t])
        for t in sorted(set(unpartitioned_tables) | set(partitioned_tables_fresh))
    }
    manifest["changes"] = _compute_partitioned_changes(
        previous, manifest["tables"], partitioned_tables_fresh
    )
    return manifest
