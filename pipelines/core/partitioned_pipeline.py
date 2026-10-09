"""Stage orchestration for a partitioned source (Retrosheet: per-season zips), reusing the
same `fetch_previous_manifest`/`write_json`/`latest_url`/`StageError` helpers Lahman's
`pipelines.core.pipeline` stages use. `pipelines.core.pipeline.stage_detect/build/publish`
delegate here whenever `config.partitions is not None`; this module never runs for Lahman.

Unlike Lahman's single zip, there's no one upstream version string to detect against: each
run only downloads and converts the seasons whose ETag actually changed (`decide_partitions`),
and the two un-partitioned tables (`game_info`/`all_players`, always republished whole) are
assembled by carrying forward the previous release's rows for every season that *didn't*
change and unioning in the freshly converted rows for the ones that did — not by
re-downloading and re-converting every season on every run.
"""

from __future__ import annotations

import io
import json
import shutil
import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from pipelines.core.config import SourceConfig, SourceSchema
from pipelines.core.detect import USER_AGENT, fetch_latest
from pipelines.core.partition_manifest import build_partitioned_manifest
from pipelines.core.partitions import check_all_partitions, decide_partitions, download_partition
from pipelines.core.pipeline import StageError, fetch_previous_manifest, latest_url, write_json
from pipelines.core.publish import ReleaseClient, make_notice, publish, record_upstream_seen
from pipelines.core.validate import ValidationError
from pipelines.core.versioning import next_tag

DEFAULT_PARTITION_DELAY = 0.5  # seconds between requests to a small, volunteer-run site


def stage_detect(
    config: SourceConfig,
    *,
    out_dir: Path,
    repo: str,
    session: Any,
    force: bool = False,
    only: list[str] | None = None,
    delay: float = DEFAULT_PARTITION_DELAY,
    progress: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """`progress`, if given, is called repeatedly through both the ETag-check pass and the
    download pass — confirmed real: a CI step that produced no output for ~19 minutes while
    checking/downloading ~129 seasons sequentially was cancelled by the platform."""
    assert config.partitions is not None
    try:
        previous_latest = fetch_latest(latest_url(config, repo), session)
        checks = check_all_partitions(
            config,
            session=session,
            previous_upstream_seen=(previous_latest or {}).get("upstream_seen"),
            user_agent=USER_AGENT,
            only=only,
            delay=delay,
            progress=progress,
        )
        decision = decide_partitions(checks, first_release=previous_latest is None, force=force)
        zip_paths: dict[str, str] = {}
        if decision.release:
            for i, season in enumerate(decision.changed_seasons):
                if i and delay:
                    time.sleep(delay)
                path, _ = download_partition(
                    config.partitions.url(season), season, out_dir=out_dir / "downloads",
                    session=session, user_agent=USER_AGENT,
                )  # fmt: skip
                zip_paths[season] = str(path)
                if progress:
                    progress(f"downloaded {season}")
        result = {
            "changed": decision.release,
            "reason": decision.reason,
            "changed_seasons": decision.changed_seasons,
            "zip_paths": zip_paths,
            "upstream_seen": {key: {"etag": c.etag} for key, c in checks.items()},
        }
        write_json(out_dir / "detect.json", result)
        return result
    except Exception as e:
        raise StageError("detect", str(e)) from e


def _fetch_parquet(url: str, session: Any) -> pl.DataFrame:
    resp = session.get(url, timeout=60, headers={"User-Agent": USER_AGENT})
    resp.raise_for_status()
    return pl.read_parquet(io.BytesIO(resp.content))


def _assemble_unpartitioned(
    name: str,
    fresh_seasons: dict[str, pl.DataFrame],
    *,
    previous: dict[str, Any] | None,
    changed_seasons: list[str],
    season_column: str,
    session: Any,
) -> pl.DataFrame:
    """The complete, current table: every season carried forward from the previous release's
    own file except the ones rebuilt this run, unioned with those seasons' fresh rows. On a
    first release there's nothing to carry forward, so every season must be fresh."""
    fresh = pl.concat(list(fresh_seasons.values())) if fresh_seasons else None
    if previous is None:
        assert fresh is not None, f"{name}: first release but no fresh rows were converted"
        return fresh
    old = _fetch_parquet(previous["tables"][name]["url"], session)
    changed = {int(s) for s in changed_seasons}
    carried = old.filter(~pl.col(season_column).is_in(list(changed)))
    return pl.concat([carried, fresh]) if fresh is not None else carried


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
    progress: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Convert only the changed seasons, validate just that slice (a self-consistent subset
    of a season or few — golden checks that assume the full history is skipped unless this
    *is* the full history, i.e. a first release), assemble the un-partitioned tables in full,
    and write Parquet + manifest. Returns build.json's content."""
    assert config.partitions is not None
    try:
        # `publish()` treats "exists on disk in build_dir/tables" as "written this run, so
        # upload it"; a season's file left over from an earlier build in this same `out_dir`
        # (e.g. a local rerun that reused it) would otherwise be mistaken for fresh and
        # re-uploaded even though it was never touched this run.
        shutil.rmtree(out_dir / "tables", ignore_errors=True)
        previous_latest = fetch_latest(latest_url(config, repo), session)
        previous = fetch_previous_manifest(repo, previous_latest, session)
        changed_seasons: list[str] = found["changed_seasons"]

        fresh_by_table: dict[str, dict[str, pl.DataFrame]] = {}
        for season in changed_seasons:
            zip_path = Path(found["zip_paths"][season])
            for name, df in config.partitions.convert(zip_path, season).items():
                fresh_by_table.setdefault(name, {})[season] = df
            if progress:
                progress(f"converted {season}")

        # A forced release with no actually-changed seasons (e.g. to carry a pipeline-version
        # bump out) converts nothing fresh; there's nothing new to validate — every carried-
        # forward row was already validated when it was first built.
        if changed_seasons:
            season_scoped = {
                name: pl.concat(list(d.values())) for name, d in fresh_by_table.items()
            }
            failures = schema.validate(season_scoped, full_dataset=previous is None)
            if failures:
                raise ValidationError(failures)

        partitioned_names = config.partitions.partitioned_tables
        partitioned_fresh = {t: fresh_by_table.get(t, {}) for t in partitioned_names}
        unpartitioned_tables = {
            name: _assemble_unpartitioned(
                name,
                fresh_by_table.get(name, {}),
                previous=previous,
                changed_seasons=changed_seasons,
                season_column=schema.year_columns[name],
                session=session,
            )  # fmt: skip
            for name in sorted(set(fresh_by_table) - partitioned_names)
        }

        existing = client.existing_tags() if client is not None else []
        tag = next_tag(config.name, now.date(), existing)
        upstream = {"version": now.date().isoformat(), "page_url": config.page_url}
        if progress:
            progress(f"writing manifest for {len(fresh_by_table)} table(s)")
        manifest = build_partitioned_manifest(
            config, tag=tag, repo=repo, upstream=upstream,
            unpartitioned_tables=unpartitioned_tables, partitioned_tables_fresh=partitioned_fresh,
            parquet_dir=out_dir / "tables", primary_keys=schema.primary_keys,
            season_columns=schema.year_columns, dtypes=schema.dtypes, schema_version=schema.version,
            pipeline_version=pipeline_version, built_at=now, previous=previous,
        )  # fmt: skip
        write_json(out_dir / "manifest.json", manifest)
        (out_dir / "NOTICE.md").write_text(make_notice(config, manifest, extra=config.extra_notice))
        build = {"release": True, "reason": found["reason"], "tag": tag}
        write_json(out_dir / "build.json", build)
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
    progress: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    try:
        previous_latest = fetch_latest(latest_url(config, repo), session)
        if not build["release"]:
            seen = found["upstream_seen"]
            if not dry_run and client is not None and previous_latest is not None:
                record_upstream_seen(client, config.name, previous_latest, seen, now, out_dir)
            return {"status": "content_unchanged", "reason": build["reason"]}
        manifest = json.loads((out_dir / "manifest.json").read_text())
        if progress:
            progress(f"publishing {build['tag']} ({len(manifest['tables'])} table(s))")
        plan = publish(
            client, config=config, manifest=manifest, build_dir=out_dir, zip_path=None,
            previous_latest=previous_latest, dry_run=dry_run, now=now,
            upstream_seen=found["upstream_seen"],
        )  # fmt: skip
    except Exception as e:
        raise StageError("publish", str(e)) from e
    return {
        "status": "published" if plan["published"] else "dry_run",
        "reason": build["reason"],
        "tag": build["tag"],
        "assets": plan["assets"],
    }
