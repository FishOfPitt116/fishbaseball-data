"""Stage orchestration: detect -> build -> publish, with outputs in `out_dir` per stage."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from pipelines.core.config import SourceConfig, SourceSchema
from pipelines.core.convert import content_hash, read_tables, write_parquet
from pipelines.core.detect import USER_AGENT, detect, fetch_latest
from pipelines.core.http import new_session
from pipelines.core.manifest import build_manifest, release_url
from pipelines.core.publish import (
    ReleaseClient,
    make_notice,
    pointer_tag,
    publish,
    record_upstream_seen,
)
from pipelines.core.validate import ValidationError, compare_with_previous
from pipelines.core.versioning import decide, next_tag


class StageError(Exception):
    def __init__(self, stage: str, message: str):
        self.stage = stage
        super().__init__(f"[{stage}] {message}")


def _fetch_previous_manifest(
    repo: str, previous_latest: dict[str, Any] | None, session: Any
) -> dict[str, Any] | None:
    if not previous_latest or not previous_latest.get("latest"):
        return None
    url = release_url(repo, previous_latest["latest"], "manifest.json")
    resp = session.get(url, timeout=30, headers={"User-Agent": USER_AGENT})
    resp.raise_for_status()
    return resp.json()


def _write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, indent=2, sort_keys=False) + "\n")


def _latest_url(config: SourceConfig, repo: str) -> str:
    return release_url(repo, pointer_tag(config.name), "latest.json")


def stage_detect(
    config: SourceConfig,
    *,
    out_dir: Path,
    repo: str,
    session: Any,
    source_url: str | None,
    force: bool,
) -> dict[str, Any]:
    try:
        return detect(
            config, out_dir=out_dir, latest_url=_latest_url(config, repo), session=session,
            source_url=source_url, force=force,
        )  # fmt: skip
    except Exception as e:
        raise StageError("detect", str(e)) from e


def stage_build(
    config: SourceConfig,
    schema: SourceSchema,
    *,
    out_dir: Path,
    repo: str,
    session: Any,
    client: ReleaseClient | None,
    found: dict[str, Any],
    now: datetime,
    pipeline_version: str,
    full_dataset: bool,
    force: bool = False,
) -> dict[str, Any]:
    """Convert, validate, write Parquet + manifest. Returns build.json's content."""
    try:
        previous_latest = fetch_latest(_latest_url(config, repo), session)
        previous = _fetch_previous_manifest(repo, previous_latest, session)
        tables = read_tables(Path(found["zip_path"]), config, schema.dtypes, schema.primary_keys)
        failures = schema.validate(tables, full_dataset=full_dataset)
        if failures:
            raise ValidationError(failures)
        compare_with_previous(
            tables,
            previous,
            tolerance=config.row_shrink_tolerance,
            year_columns=schema.year_columns,
        )
        for name, df in tables.items():
            write_parquet(df, out_dir / "tables" / f"{name}.parquet")
        hashes = {n: content_hash(df, schema.primary_keys[n]) for n, df in tables.items()}
        decision = decide(previous, found["sabr_version"], hashes, force=force)
        existing = client.existing_tags() if client is not None else []
        tag = next_tag(config.name, now.date(), existing)
        upstream = {
            "version": found["sabr_version"],
            "released": found["released"],
            "page_url": config.page_url,
            "sha256": found["upstream_sha256"],
            "file": f"{config.name}_{found['sabr_version']}_csv.zip",
        }
        manifest = build_manifest(
            config, tag=tag, repo=repo, upstream=upstream, tables=tables,
            parquet_dir=out_dir / "tables", primary_keys=schema.primary_keys,
            year_columns=schema.year_columns, dtypes=schema.dtypes, schema_version=schema.version,
            pipeline_version=pipeline_version, built_at=now, previous=previous,
        )  # fmt: skip
        _write_json(out_dir / "manifest.json", manifest)
        (out_dir / "NOTICE.md").write_text(make_notice(config, manifest))
        build = {"release": decision.release, "reason": decision.reason, "tag": tag}
        _write_json(out_dir / "build.json", build)
        return build
    except Exception as e:
        raise StageError("build", str(e)) from e


def stage_publish(
    config: SourceConfig,
    *,
    out_dir: Path,
    repo: str,
    session: Any,
    client: ReleaseClient | None,
    found: dict[str, Any],
    build: dict[str, Any],
    dry_run: bool,
    now: datetime,
) -> dict[str, Any]:
    try:
        previous_latest = fetch_latest(_latest_url(config, repo), session)
        if not build["release"]:
            seen = {"version": found["sabr_version"], "sha256": found["upstream_sha256"]}
            if not dry_run and client is not None and previous_latest is not None:
                record_upstream_seen(client, config.name, previous_latest, seen, now, out_dir)
            return {"status": "content_unchanged", "reason": build["reason"]}
        manifest = json.loads((out_dir / "manifest.json").read_text())
        plan = publish(
            client, config=config, manifest=manifest, build_dir=out_dir,
            zip_path=Path(found["zip_path"]), previous_latest=previous_latest,
            dry_run=dry_run, now=now,
        )  # fmt: skip
    except Exception as e:
        raise StageError("publish", str(e)) from e
    return {
        "status": "published" if plan["published"] else "dry_run",
        "reason": build["reason"],
        "tag": build["tag"],
        "assets": plan["assets"],
    }


def run_all(
    config: SourceConfig,
    schema: SourceSchema,
    *,
    out_dir: Path,
    repo: str,
    client: ReleaseClient | None,
    session: Any = None,
    source_url: str | None = None,
    force: bool = False,
    dry_run: bool = False,
    now: datetime,
    pipeline_version: str,
    full_dataset: bool = True,
) -> dict[str, Any]:
    session = session or new_session()
    out_dir.mkdir(parents=True, exist_ok=True)
    found = stage_detect(
        config, out_dir=out_dir, repo=repo, session=session, source_url=source_url, force=force
    )
    if not found["changed"]:
        return {"status": "no_change", "reason": found["reason"]}
    build = stage_build(
        config, schema, out_dir=out_dir, repo=repo, session=session, client=client, found=found,
        now=now, pipeline_version=pipeline_version, full_dataset=full_dataset, force=force,
    )  # fmt: skip
    return stage_publish(
        config, out_dir=out_dir, repo=repo, session=session, client=client, found=found,
        build=build, dry_run=dry_run, now=now,
    )  # fmt: skip
