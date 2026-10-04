"""`publish()` generalized to a manifest with partitioned tables: only freshly-written
partition files (and every single-file table) get staged/uploaded; a carried-forward
partition's file was never written this run, so it's never re-uploaded."""

import re
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from pipelines.core.config import SourceConfig
from pipelines.core.partition_manifest import build_partitioned_manifest
from pipelines.core.publish import make_notice, publish
from tests.unit.test_publish import FakeClient

REPO = "FishOfPitt116/fishbaseball-data"
NOW = datetime(2026, 10, 5, 12, 0, 0, tzinfo=timezone.utc)

CONFIG = SourceConfig(
    name="retrosheet",
    page_url="https://www.retrosheet.org/",
    version_pattern=re.compile(r"$never^"),
    download_urls=(),
    tables={},
    columns={"plays": {"gid": "gid", "pn": "pn"}, "game_info": {"gid": "gid", "season": "season"}},
    license="x",
    attribution="y",
)


UPSTREAM = {"version": "2026-08-09", "page_url": "https://www.retrosheet.org/"}
DTYPES = {
    "plays": {"gid": pl.Utf8, "pn": pl.Int16},
    "game_info": {"gid": pl.Utf8, "season": pl.Int16},
}
PRIMARY_KEYS = {"plays": ["gid", "pn"], "game_info": ["gid"]}
SEASON_COLUMNS = {"plays": "season", "game_info": "season"}


def plays_df(season: int, rows: int = 2) -> pl.DataFrame:
    return pl.DataFrame({"gid": [f"G{season}{i}" for i in range(rows)], "pn": list(range(rows))})


def game_info_df(seasons: list[int]) -> pl.DataFrame:
    return pl.DataFrame({"gid": [f"G{s}" for s in seasons], "season": seasons})


def manifest_for(tag, fresh, unpartitioned, parquet_dir, previous=None):
    return build_partitioned_manifest(
        CONFIG,
        tag=tag,
        repo=REPO,
        upstream=UPSTREAM,
        unpartitioned_tables=unpartitioned,
        partitioned_tables_fresh=fresh,
        parquet_dir=parquet_dir,
        primary_keys=PRIMARY_KEYS,
        season_columns=SEASON_COLUMNS,
        dtypes=DTYPES,
        schema_version=1,
        pipeline_version="0.1.0",
        built_at=NOW,
        previous=previous,
    )


@pytest.fixture
def first_release(tmp_path: Path):
    parquet_dir = tmp_path / "first" / "tables"
    parquet_dir.mkdir(parents=True)
    manifest = manifest_for(
        "retrosheet-2026-01-01",
        {"plays": {"2023": plays_df(2023), "2024": plays_df(2024)}},
        {"game_info": game_info_df([2023, 2024])},
        parquet_dir,
    )
    (tmp_path / "first" / "manifest.json").write_text(
        "{}"
    )  # placeholder, not used for first release
    (tmp_path / "first" / "NOTICE.md").write_text(make_notice(CONFIG, manifest))
    return manifest, parquet_dir, tmp_path / "first"


def test_dry_run_assets_exclude_nothing_on_a_first_release(first_release):
    manifest, parquet_dir, build_dir = first_release
    plan = publish(
        None, config=CONFIG, manifest=manifest, build_dir=build_dir, zip_path=None,
        previous_latest=None, dry_run=True, now=NOW,
    )  # fmt: skip
    assert "plays_2023.parquet" in plan["assets"]
    assert "plays_2024.parquet" in plan["assets"]
    assert "game_info.parquet" in plan["assets"]
    assert "manifest.json" in plan["assets"] and "NOTICE.md" in plan["assets"]
    assert not any(a.endswith(".zip") for a in plan["assets"])  # no zip_path given


def test_second_release_only_uploads_the_changed_partition(tmp_path: Path, first_release):
    first_manifest, _, _ = first_release
    second_dir = tmp_path / "second"
    parquet_dir = second_dir / "tables"
    parquet_dir.mkdir(parents=True)
    manifest = manifest_for(
        "retrosheet-2026-02-01",
        {"plays": {"2024": plays_df(2024, rows=5)}},  # only 2024 revised
        {"game_info": game_info_df([2023, 2024])},
        parquet_dir,
        previous=first_manifest,
    )
    (second_dir / "manifest.json").write_text("{}")
    (second_dir / "NOTICE.md").write_text(make_notice(CONFIG, manifest))

    client = FakeClient()
    plan = publish(
        client, config=CONFIG, manifest=manifest, build_dir=second_dir, zip_path=None,
        previous_latest={"latest": "retrosheet-2026-01-01", "releases": []}, dry_run=False, now=NOW,
        upstream_seen={"2024": {"etag": "\"x\""}},
    )  # fmt: skip
    assert plan["published"] is True
    uploaded = {name for (tag, name) in client.assets if tag == "retrosheet-2026-02-01"}
    assert "plays_2024.parquet" in uploaded
    assert "plays_2023.parquet" not in uploaded  # carried forward, never re-uploaded
    assert "game_info.parquet" in uploaded  # single-file table: always re-uploaded


def test_verification_checks_a_freshly_uploaded_file_not_a_carried_forward_one(
    tmp_path: Path, first_release
):
    first_manifest, _, _ = first_release
    second_dir = tmp_path / "second"
    parquet_dir = second_dir / "tables"
    parquet_dir.mkdir(parents=True)
    manifest = manifest_for(
        "retrosheet-2026-02-01",
        {"plays": {"2024": plays_df(2024, rows=5)}},
        {"game_info": game_info_df([2023, 2024])},
        parquet_dir,
        previous=first_manifest,
    )
    (second_dir / "manifest.json").write_text("{}")
    (second_dir / "NOTICE.md").write_text(make_notice(CONFIG, manifest))

    # `_verify` samples the first fresh file; with both an unpartitioned table and a changed
    # partition present, that's game_info.parquet (it's always listed first — see
    # `build_manifest`, which `build_partitioned_manifest` calls for the unpartitioned tables
    # before adding partitioned ones).
    client = FakeClient(corrupt="game_info.parquet")
    with pytest.raises(Exception, match="game_info.parquet"):
        publish(
            client, config=CONFIG, manifest=manifest, build_dir=second_dir, zip_path=None,
            previous_latest={"latest": "retrosheet-2026-01-01", "releases": []},
            dry_run=False, now=NOW, upstream_seen={"2024": {"etag": '"x"'}},
        )  # fmt: skip


def test_no_upstream_zip_is_attached_when_zip_path_is_none(first_release):
    manifest, parquet_dir, build_dir = first_release
    client = FakeClient()
    publish(
        client, config=CONFIG, manifest=manifest, build_dir=build_dir, zip_path=None,
        previous_latest=None, dry_run=False, now=NOW, upstream_seen={},
    )  # fmt: skip
    uploaded_names = {name for (_, name) in client.assets}
    assert not any(n.endswith(".zip") for n in uploaded_names)
