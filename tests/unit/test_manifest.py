import json
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from pipelines.core.convert import content_hash, read_tables, write_parquet
from pipelines.core.manifest import build_manifest, compute_changes
from pipelines.lahman import LAHMAN
from pipelines.lahman.schema import DTYPES, PRIMARY_KEYS, SCHEMA_VERSION, YEAR_COLUMNS

REPO = "FishOfPitt116/fishbaseball-data"
TAG = "lahman-2026-10-02"
UPSTREAM = {
    "version": "2025",
    "released": "2026-01-02",
    "page_url": LAHMAN.page_url,
    "sha256": "f" * 64,
    "file": "lahman_2025_csv.zip",
}


@pytest.fixture
def built(tmp_path: Path, small_zip: Path):
    tables = read_tables(small_zip, LAHMAN, DTYPES, PRIMARY_KEYS)
    for name, df in tables.items():
        write_parquet(df, tmp_path / f"{name}.parquet")
    return tables, tmp_path


def make(built, previous=None, tag=TAG):
    tables, out = built
    return build_manifest(
        LAHMAN,
        tag=tag,
        repo=REPO,
        upstream=UPSTREAM,
        tables=tables,
        parquet_dir=out,
        primary_keys=PRIMARY_KEYS,
        year_columns=YEAR_COLUMNS,
        dtypes=DTYPES,
        schema_version=SCHEMA_VERSION,
        pipeline_version="0.1.0",
        built_at=datetime(2026, 10, 2, 13, 20, 11, tzinfo=timezone.utc),
        previous=previous,
    )


def test_top_level_shape(built):
    m = make(built)
    assert m["source"] == "lahman" and m["tag"] == TAG and m["version"] == "2025"
    assert m["schema_version"] == 1 and m["pipeline_version"] == "0.1.0"
    assert m["built_at"] == "2026-10-02T13:20:11Z"
    assert m["license"] == "CC BY-SA 3.0" and m["attribution"] == LAHMAN.attribution
    assert m["upstream"] == UPSTREAM
    assert "fishbaseball_ref" not in m
    json.dumps(m)  # serializable


def test_column_map_is_the_committed_map(built):
    m = make(built)
    assert m["column_map"]["batting"]["2B"] == "doubles"
    assert set(m["column_map"]) == set(LAHMAN.columns)


def test_table_entries(built):
    tables, out = built
    m = make(built)
    e = m["tables"]["batting"]
    assert e["file"] == "batting.parquet"
    assert e["url"] == f"https://github.com/{REPO}/releases/download/{TAG}/batting.parquet"
    assert e["rows"] == tables["batting"].height
    assert e["bytes"] == (out / "batting.parquet").stat().st_size
    assert len(e["sha256"]) == 64 and e["content_sha256"] == content_hash(
        tables["batting"], PRIMARY_KEYS["batting"]
    )
    assert (e["min_year"], e["max_year"]) == (1914, 2025)  # Ruth debut; latest-season sample
    assert m["tables"]["people"]["min_year"] is None


def test_urls_and_tag_are_consistent(built):
    m = make(built)
    for t in m["tables"].values():
        assert f"/download/{m['tag']}/{t['file']}" in t["url"]


def test_first_release_changes(built):
    m = make(built)
    assert m["changes"] == {
        "previous": None,
        "tables_added": [],
        "tables_changed": [],
        "row_deltas": {},
    }


def test_changes_against_previous(built):
    first = make(built, tag="lahman-2026-01-05")
    tables, out = built
    tables = dict(tables)
    tables["batting"] = pl.concat([tables["batting"], tables["batting"].head(2)])  # +2 rows
    changed_first = {**first, "tables": {k: dict(v) for k, v in first["tables"].items()}}
    del changed_first["tables"]["salaries"]
    m = build_manifest(
        LAHMAN,
        tag=TAG,
        repo=REPO,
        upstream=UPSTREAM,
        tables=tables,
        parquet_dir=out,
        primary_keys=PRIMARY_KEYS,
        year_columns=YEAR_COLUMNS,
        dtypes=DTYPES,
        schema_version=1,
        pipeline_version="0.1.0",
        built_at=datetime(2026, 10, 2, tzinfo=timezone.utc),
        previous=changed_first,
    )
    ch = m["changes"]
    assert ch["previous"] == "lahman-2026-01-05"
    assert ch["tables_added"] == ["salaries"]
    assert ch["tables_changed"] == ["batting"]
    assert ch["row_deltas"] == {"batting": 2}


def test_compute_changes_row_deltas_only_for_nonzero():
    prev = {
        "tag": "t0",
        "tables": {
            "a": {"content_sha256": "1", "rows": 10},
            "b": {"content_sha256": "2", "rows": 5},
        },
    }
    new = {"a": {"content_sha256": "1", "rows": 10}, "b": {"content_sha256": "3", "rows": 4}}
    assert compute_changes(prev, new) == {
        "previous": "t0",
        "tables_added": [],
        "tables_changed": ["b"],
        "row_deltas": {"b": -1},
    }


def test_previous_manifest_fixture_is_valid_for_versioning(fixtures: Path):
    prev = json.loads((fixtures / "previous_manifest.json").read_text())
    assert prev["source"] == "lahman" and prev["version"] == "2025"
    assert set(prev["tables"]) == set(LAHMAN.tables.values())


def test_table_entries_carry_columns_primary_key_and_season_column(built):
    m = make(built)
    batting = m["tables"]["batting"]
    assert batting["primary_key"] == PRIMARY_KEYS["batting"]
    assert batting["season_column"] == "year_id"
    assert batting["columns"]["player_id"] == "String"
    assert batting["columns"]["year_id"] == "Int32"
    assert batting["columns"]["hr"] == "Int32"
    assert set(batting["columns"]) == set(DTYPES["batting"])
    people = m["tables"]["people"]
    assert people["season_column"] is None
    assert people["primary_key"] == ["player_id"]


def test_columns_match_dtypes_for_every_table(built):
    m = make(built)
    for name, entry in m["tables"].items():
        assert entry["columns"] == {c: str(t) for c, t in DTYPES[name].items()}
