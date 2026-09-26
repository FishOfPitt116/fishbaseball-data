from pathlib import Path

import polars as pl
import pytest

from pipelines.core.convert import read_tables
from pipelines.core.validate import ValidationError, compare_with_previous
from pipelines.lahman import LAHMAN
from pipelines.lahman.schema import DTYPES, PRIMARY_KEYS, YEAR_COLUMNS, validate


@pytest.fixture
def tables(small_zip: Path) -> dict[str, pl.DataFrame]:
    return read_tables(small_zip, LAHMAN, DTYPES, PRIMARY_KEYS)


def failures(tables) -> list[str]:
    return validate(tables, full_dataset=False)


def test_fixture_passes(tables):
    assert failures(tables) == []


def test_duplicate_primary_key_fails(tables):
    tables["batting"] = pl.concat([tables["batting"], tables["batting"].head(1)])
    assert any("batting" in f and "duplicate" in f for f in failures(tables))


def test_hits_greater_than_at_bats_fails(tables):
    b = tables["batting"]
    tables["batting"] = b.with_columns(
        pl.when(pl.col("player_id") == "ruthba01")
        .then(pl.col("ab") + 1000)
        .otherwise(pl.col("ab"))
        .alias("h")
    )
    assert any("h > ab" in f for f in failures(tables))


def test_orphan_player_id_fails(tables):
    p = tables["people"]
    tables["people"] = p.filter(pl.col("player_id") != "ruthba01")
    assert any("orphan" in f and "ruthba01" in f for f in failures(tables))


def test_bad_month_fails(tables):
    p = tables["people"]
    tables["people"] = p.with_columns(pl.lit(13, pl.Int32).alias("birth_month"))
    assert any("birth_month" in f for f in failures(tables))


def test_wrong_dtype_fails(tables):
    b = tables["batting"]
    tables["batting"] = b.with_columns(pl.col("hr").cast(pl.Int64))
    assert any("batting.hr" in f and "dtype" in f for f in failures(tables))


def test_dropped_column_fails(tables):
    tables["batting"] = tables["batting"].drop("gidp")
    assert any("batting" in f and "gidp" in f for f in failures(tables))


def test_missing_table_fails(tables):
    del tables["salaries"]
    assert any("salaries" in f and "missing" in f for f in failures(tables))


def test_golden_value_ruth_1927_fails_when_wrong(tables):
    b = tables["batting"]
    tables["batting"] = b.with_columns(
        pl.when((pl.col("player_id") == "ruthba01") & (pl.col("year_id") == 1927))
        .then(59)
        .otherwise(pl.col("hr"))
        .cast(pl.Int32)
        .alias("hr")
    )
    assert any("ruthba01" in f and "60" in f for f in failures(tables))


def test_latest_season_mismatch_fails(tables):
    t = tables["teams"]
    tables["teams"] = t.filter(pl.col("year_id") < 2025)
    assert any("latest season" in f for f in failures(tables))


def test_full_dataset_requires_a_full_league(tables):
    assert any("teams" in f and "latest season" in f for f in validate(tables, full_dataset=True))


# ---- comparison with the previous release ----


def prev_manifest(tables) -> dict:
    return {
        "column_map": {t: {c: c for c in df.columns} for t, df in tables.items()},
        "tables": {
            t: {
                "rows": df.height,
                "max_year": df[YEAR_COLUMNS[t]].max() if t in YEAR_COLUMNS else None,
            }
            for t, df in tables.items()
        },
    }


def test_compare_passes_when_unchanged(tables):
    compare_with_previous(tables, prev_manifest(tables), tolerance=0.005, year_columns=YEAR_COLUMNS)


def test_compare_first_run_is_skipped(tables):
    compare_with_previous(tables, None, tolerance=0.005, year_columns=YEAR_COLUMNS)


def test_compare_fails_on_removed_table(tables):
    prev = prev_manifest(tables)
    prev["tables"]["extra"] = {"rows": 1, "max_year": None}
    prev["column_map"]["extra"] = {"a": "a"}
    with pytest.raises(ValidationError, match="removed.*extra"):
        compare_with_previous(tables, prev, tolerance=0.005, year_columns=YEAR_COLUMNS)


def test_compare_fails_on_removed_column(tables):
    prev = prev_manifest(tables)
    tables["batting"] = tables["batting"].drop("gidp")
    with pytest.raises(ValidationError, match="batting.*gidp"):
        compare_with_previous(tables, prev, tolerance=0.005, year_columns=YEAR_COLUMNS)


def test_compare_fails_on_shrunk_rows(tables):
    prev = prev_manifest(tables)
    prev["tables"]["batting"]["rows"] = tables["batting"].height * 2
    with pytest.raises(ValidationError, match="batting.*rows"):
        compare_with_previous(tables, prev, tolerance=0.005, year_columns=YEAR_COLUMNS)


def test_compare_allows_shrink_within_tolerance(tables):
    prev = prev_manifest(tables)
    prev["tables"]["batting"]["rows"] = tables["batting"].height + 3
    compare_with_previous(tables, prev, tolerance=0.05, year_columns=YEAR_COLUMNS)
    with pytest.raises(ValidationError, match="batting.*rows"):
        compare_with_previous(tables, prev, tolerance=0.005, year_columns=YEAR_COLUMNS)


def test_compare_fails_when_max_year_regresses(tables):
    prev = prev_manifest(tables)
    prev["tables"]["batting"]["max_year"] = 2026
    with pytest.raises(ValidationError, match="batting.*max"):
        compare_with_previous(tables, prev, tolerance=0.005, year_columns=YEAR_COLUMNS)


def _with_row(tables, **overrides):
    b = tables["batting"]
    row = b.head(1).with_columns(
        [pl.lit(v).cast(b.schema[k]).alias(k) for k, v in overrides.items()]
    )
    tables["batting"] = pl.concat([b, row])
    return tables


def test_known_upstream_error_row_is_allowed(tables):
    # Lahman 2025 has this row with hr > h; it is an upstream data error we tolerate by name.
    _with_row(
        tables, player_id="tayloci99", year_id=1912, stint=1, ab=7, h=0, hr=1, doubles=0, triples=0
    )
    tables["people"] = pl.concat(
        [
            tables["people"],
            tables["people"].head(1).with_columns(pl.lit("tayloci99").alias("player_id")),
        ]
    )
    assert failures(tables) == []


def test_same_error_on_another_row_is_still_caught(tables):
    _with_row(
        tables, player_id="ruthba01", year_id=1900, stint=1, ab=7, h=0, hr=1, doubles=0, triples=0
    )
    assert any("hr > h" in f for f in failures(tables))
