"""Manifest building for sources that mix partitioned tables (one Parquet file per season,
unchanged seasons reused from an older release) with plain single-file tables."""

from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from pipelines.core.manifest import release_url
from pipelines.core.partition_manifest import build_partitioned_manifest

REPO = "FishOfPitt116/fishbaseball-data"
TAG = "retrosheet-2026-10-05"
UPSTREAM = {"version": "2026-08-09", "page_url": "https://www.retrosheet.org/"}


class Config:
    name = "retrosheet"
    license = "x"
    attribution = "y"
    columns = {
        "plays": {"gid": "gid", "pn": "pn"},
        "game_info": {"gid": "gid", "season": "season"},
    }


def plays_df(season: int, rows: int = 2) -> pl.DataFrame:
    return pl.DataFrame({"gid": [f"G{season}{i}" for i in range(rows)], "pn": list(range(rows))})


def game_info_df(seasons: list[int]) -> pl.DataFrame:
    return pl.DataFrame({"gid": [f"G{s}" for s in seasons], "season": seasons})


DTYPES = {
    "plays": {"gid": pl.Utf8, "pn": pl.Int16},
    "game_info": {"gid": pl.Utf8, "season": pl.Int16},
}
PRIMARY_KEYS = {"plays": ["gid", "pn"], "game_info": ["gid"]}
SEASON_COLUMNS = {"plays": "season", "game_info": "season"}


@pytest.fixture
def parquet_dir(tmp_path: Path) -> Path:
    d = tmp_path / "tables"
    d.mkdir()
    return d


def build(
    *,
    tag=TAG,
    fresh_partitions,
    unpartitioned,
    parquet_dir,
    previous=None,
):
    return build_partitioned_manifest(
        Config(),
        tag=tag,
        repo=REPO,
        upstream=UPSTREAM,
        unpartitioned_tables=unpartitioned,
        partitioned_tables_fresh=fresh_partitions,
        parquet_dir=parquet_dir,
        primary_keys=PRIMARY_KEYS,
        season_columns=SEASON_COLUMNS,
        dtypes=DTYPES,
        schema_version=1,
        pipeline_version="0.1.0",
        built_at=datetime(2026, 10, 5, 12, 0, 0, tzinfo=timezone.utc),
        previous=previous,
    )


def test_first_release_all_partitions_fresh(parquet_dir):
    m = build(
        fresh_partitions={"plays": {"2024": plays_df(2024), "2025": plays_df(2025)}},
        unpartitioned={"game_info": game_info_df([2024, 2025])},
        parquet_dir=parquet_dir,
    )
    plays = m["tables"]["plays"]
    assert plays["partitioned"] is True
    assert set(plays["partitions"]) == {"2024", "2025"}
    assert plays["partitions"]["2024"]["url"] == release_url(REPO, TAG, "plays_2024.parquet")
    assert plays["partitions"]["2025"]["url"] == release_url(REPO, TAG, "plays_2025.parquet")
    assert plays["rows"] == 4 and plays["min_year"] == 2024 and plays["max_year"] == 2025
    assert plays["primary_key"] == ["gid", "pn"] and plays["season_column"] == "season"

    game_info = m["tables"]["game_info"]
    assert "partitioned" not in game_info or game_info.get("partitioned") is False
    assert game_info["rows"] == 2
    assert (parquet_dir / "plays_2024.parquet").exists()
    assert (parquet_dir / "game_info.parquet").exists()


def test_unchanged_partitions_are_carried_forward_not_rewritten(parquet_dir):
    first = build(
        fresh_partitions={"plays": {"2024": plays_df(2024)}},
        unpartitioned={"game_info": game_info_df([2024])},
        parquet_dir=parquet_dir,
        tag="retrosheet-2026-01-01",
    )
    second_dir = parquet_dir.parent / "tables2"
    second_dir.mkdir()
    second = build(
        fresh_partitions={"plays": {}},  # nothing changed this run
        unpartitioned={"game_info": game_info_df([2024])},
        parquet_dir=second_dir,
        previous=first,
    )
    plays = second["tables"]["plays"]
    assert plays["partitions"]["2024"]["url"] == release_url(
        REPO, "retrosheet-2026-01-01", "plays_2024.parquet"
    )
    assert not (second_dir / "plays_2024.parquet").exists()  # not re-downloaded or re-written


def test_mixed_fresh_and_carried_forward_partitions(parquet_dir):
    first = build(
        fresh_partitions={"plays": {"2023": plays_df(2023), "2024": plays_df(2024)}},
        unpartitioned={"game_info": game_info_df([2023, 2024])},
        parquet_dir=parquet_dir,
        tag="retrosheet-2026-01-01",
    )
    second_dir = parquet_dir.parent / "tables2"
    second_dir.mkdir()
    second = build(
        fresh_partitions={"plays": {"2024": plays_df(2024, rows=5)}},  # only 2024 revised
        unpartitioned={"game_info": game_info_df([2023, 2024])},
        parquet_dir=second_dir,
        previous=first,
    )
    plays = second["tables"]["plays"]
    assert plays["partitions"]["2023"]["url"] == release_url(
        REPO, "retrosheet-2026-01-01", "plays_2023.parquet"
    )
    assert plays["partitions"]["2024"]["url"] == release_url(REPO, TAG, "plays_2024.parquet")
    assert plays["partitions"]["2024"]["rows"] == 5
    assert not (second_dir / "plays_2023.parquet").exists()
    assert (second_dir / "plays_2024.parquet").exists()


def test_changes_tracks_partitions_changed_and_row_deltas(parquet_dir):
    first = build(
        fresh_partitions={"plays": {"2023": plays_df(2023), "2024": plays_df(2024)}},
        unpartitioned={"game_info": game_info_df([2023, 2024])},
        parquet_dir=parquet_dir,
        tag="retrosheet-2026-01-01",
    )
    second_dir = parquet_dir.parent / "tables2"
    second_dir.mkdir()
    second = build(
        fresh_partitions={"plays": {"2024": plays_df(2024, rows=5)}},
        unpartitioned={"game_info": game_info_df([2023, 2024])},
        parquet_dir=second_dir,
        previous=first,
    )
    changes = second["changes"]
    assert changes["previous"] == "retrosheet-2026-01-01"
    assert changes["partitions_changed"] == {"plays": ["2024"]}
    assert changes["row_deltas"]["plays"] == {"2024": 3}  # 5 - 2


def test_changes_flat_table_row_delta(parquet_dir):
    first = build(
        fresh_partitions={"plays": {"2024": plays_df(2024)}},
        unpartitioned={"game_info": game_info_df([2024])},
        parquet_dir=parquet_dir,
        tag="retrosheet-2026-01-01",
    )
    second_dir = parquet_dir.parent / "tables2"
    second_dir.mkdir()
    second = build(
        fresh_partitions={"plays": {}},
        unpartitioned={"game_info": game_info_df([2024, 2024])},  # +1 row
        parquet_dir=second_dir,
        previous=first,
    )
    assert second["changes"]["row_deltas"]["game_info"] == 1


def test_changes_new_partition_counts_as_added_rows(parquet_dir):
    first = build(
        fresh_partitions={"plays": {"2024": plays_df(2024)}},
        unpartitioned={"game_info": game_info_df([2024])},
        parquet_dir=parquet_dir,
        tag="retrosheet-2026-01-01",
    )
    second_dir = parquet_dir.parent / "tables2"
    second_dir.mkdir()
    second = build(
        fresh_partitions={"plays": {"2025": plays_df(2025)}},  # brand new season
        unpartitioned={"game_info": game_info_df([2024, 2025])},
        parquet_dir=second_dir,
        previous=first,
    )
    assert second["changes"]["partitions_changed"] == {"plays": ["2025"]}
    assert second["changes"]["row_deltas"]["plays"] == {"2025": 2}


def test_first_release_has_no_changes(parquet_dir):
    m = build(
        fresh_partitions={"plays": {"2024": plays_df(2024)}},
        unpartitioned={"game_info": game_info_df([2024])},
        parquet_dir=parquet_dir,
    )
    assert m["changes"] == {
        "previous": None, "tables_added": [], "tables_changed": [],
        "partitions_changed": {}, "row_deltas": {},
    }  # fmt: skip


def test_content_sha256_present_per_partition_for_fallback_detection(parquet_dir):
    m = build(
        fresh_partitions={"plays": {"2024": plays_df(2024)}},
        unpartitioned={"game_info": game_info_df([2024])},
        parquet_dir=parquet_dir,
    )
    entry = m["tables"]["plays"]["partitions"]["2024"]
    assert len(entry["sha256"]) == 64 and len(entry["content_sha256"]) == 64
    assert entry["bytes"] > 0
