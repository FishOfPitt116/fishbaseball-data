from pathlib import Path

import polars as pl
import pytest

from pipelines.core.convert import (
    CastError,
    ConvertError,
    InventoryError,
    content_hash,
    dedupe_most_complete,
    read_tables,
    write_parquet,
)
from pipelines.lahman import LAHMAN
from pipelines.lahman.schema import DTYPES, PRIMARY_KEYS
from tests.conftest import rewrite_zip


def load(zip_path: Path):
    return read_tables(zip_path, LAHMAN, DTYPES, PRIMARY_KEYS)


def test_reads_every_table_with_snake_case_columns(small_zip: Path):
    tables = load(small_zip)
    assert set(tables) == set(LAHMAN.tables.values())
    assert tables["batting"].columns[:3] == ["player_id", "year_id", "stint"]
    assert "doubles" in tables["batting"].columns


def test_bom_is_stripped(fixtures: Path):
    tables = load(fixtures / "lahman_bom.zip")
    for name, df in tables.items():
        assert all(not c.startswith("﻿") for c in df.columns), name


def test_bom_only_change_gives_identical_content(fixtures: Path):
    a, b = load(fixtures / "lahman_small.zip"), load(fixtures / "lahman_bom.zip")
    assert {t: content_hash(df, PRIMARY_KEYS[t]) for t, df in a.items()} == {
        t: content_hash(df, PRIMARY_KEYS[t]) for t, df in b.items()
    }


def test_dtypes_applied(small_zip: Path):
    t = load(small_zip)
    assert t["batting"].schema["hr"] == pl.Int32
    assert t["batting"].schema["player_id"] == pl.Utf8
    assert t["people"].schema["debut"] == pl.Date
    assert t["pitching"].schema["era"] == pl.Float64
    assert t["hall_of_fame"].schema["inducted"] == pl.Utf8


def test_golden_value_ruth_1927(small_zip: Path):
    b = load(small_zip)["batting"]
    row = b.filter((pl.col("player_id") == "ruthba01") & (pl.col("year_id") == 1927))
    assert row["hr"].to_list() == [60]


def test_empty_values_become_null(small_zip: Path):
    b = load(small_zip)["batting"]
    assert b["ibb"].null_count() > 0  # early seasons have no IBB


def test_output_sorted_by_primary_key(small_zip: Path):
    b = load(small_zip)["batting"]
    assert b.equals(b.sort(PRIMARY_KEYS["batting"]))


def test_missing_table_fails(fixtures: Path):
    with pytest.raises(InventoryError, match="missing.*Salaries.csv"):
        load(fixtures / "lahman_missing_table.zip")


def test_unknown_csv_fails_with_guidance(fixtures: Path):
    with pytest.raises(InventoryError, match="new upstream table.*Extra.csv.*tables.py"):
        load(fixtures / "lahman_extra_table.zip")


def test_unmapped_column_fails(tmp_path: Path, small_zip: Path):
    def fn(name, text):
        if name != "Batting.csv":
            return text
        head, *rows = text.splitlines()
        return "\n".join([head + ",newCol"] + [r + ",x" for r in rows]) + "\n"

    z = rewrite_zip(small_zip, tmp_path / "z.zip", fn)
    with pytest.raises(ConvertError, match="batting.*newCol"):
        load(z)


def test_missing_column_fails(tmp_path: Path, small_zip: Path):
    def fn(name, text):
        if name != "Batting.csv":
            return text
        rows = [r.rsplit(",", 1)[0] for r in text.splitlines()]  # drop GIDP
        return "\n".join(rows) + "\n"

    z = rewrite_zip(small_zip, tmp_path / "z.zip", fn)
    with pytest.raises(ConvertError, match="batting.*GIDP"):
        load(z)


def test_missing_column_is_null_filled_when_allowed(tmp_path: Path, small_zip: Path):
    # Real upstream data can drop a column for just one partition (Retrosheet's 1899 plays.csv
    # lacks 16 columns every other season has) without it being an error worth blocking a whole
    # release over; `allow_missing_columns` opts into filling it null instead of raising.
    def fn(name, text):
        if name != "Batting.csv":
            return text
        rows = [r.rsplit(",", 1)[0] for r in text.splitlines()]  # drop GIDP
        return "\n".join(rows) + "\n"

    z = rewrite_zip(small_zip, tmp_path / "z.zip", fn)
    tables = read_tables(z, LAHMAN, DTYPES, PRIMARY_KEYS, allow_missing_columns=True)
    assert tables["batting"]["gidp"].dtype == pl.Int32
    assert tables["batting"]["gidp"].null_count() == tables["batting"].height


def test_unmapped_column_still_fails_when_missing_is_allowed(tmp_path: Path, small_zip: Path):
    # Leniency only ever applies to a column the schema expects but the CSV lacks; a column
    # the CSV has that the schema doesn't know about is still unambiguously worth stopping for.
    def fn(name, text):
        if name != "Batting.csv":
            return text
        head, *rows = text.splitlines()
        return "\n".join([head + ",newCol"] + [r + ",x" for r in rows]) + "\n"

    z = rewrite_zip(small_zip, tmp_path / "z.zip", fn)
    with pytest.raises(ConvertError, match="batting.*newCol"):
        read_tables(z, LAHMAN, DTYPES, PRIMARY_KEYS, allow_missing_columns=True)


def test_stray_whitespace_around_a_value_does_not_fail_the_cast(tmp_path: Path, small_zip: Path):
    # Confirmed real: Retrosheet's 1976 game_info has a windspeed value of "17 " (trailing
    # space) for one game — clearly just incidental CSV formatting, not a bad value.
    def fn(name, text):
        return (
            text.replace(
                "ruthba01,1927,1,NYA,AL,151,540,158,192,29,8,60,",
                "ruthba01,1927,1,NYA,AL,151,540,158,192,29,8, 60 ,",
            )
            if name == "Batting.csv"
            else text
        )

    z = rewrite_zip(small_zip, tmp_path / "z.zip", fn)
    tables = load(z)
    row = tables["batting"].filter(
        (pl.col("player_id") == "ruthba01") & (pl.col("year_id") == 1927)
    )
    assert row["hr"].to_list() == [60]


def test_bad_cast_reports_table_column_and_value(tmp_path: Path, small_zip: Path):
    def fn(name, text):
        return (
            text.replace(
                "ruthba01,1927,1,NYA,AL,151,540,158,192,29,8,60",
                "ruthba01,1927,1,NYA,AL,151,540,158,192,29,8,sixty",
            )
            if name == "Batting.csv"
            else text
        )

    z = rewrite_zip(small_zip, tmp_path / "z.zip", fn)
    with pytest.raises(CastError) as e:
        load(z)
    msg = str(e.value)
    assert "batting" in msg and "hr" in msg and "sixty" in msg


def test_parquet_is_deterministic_and_round_trips(tmp_path: Path, small_zip: Path):
    b = load(small_zip)["batting"]
    write_parquet(b, tmp_path / "a.parquet")
    write_parquet(b, tmp_path / "b.parquet")
    assert (tmp_path / "a.parquet").read_bytes() == (tmp_path / "b.parquet").read_bytes()
    assert pl.read_parquet(tmp_path / "a.parquet").equals(b)


def test_content_hash_changes_with_content(small_zip: Path):
    b = load(small_zip)["batting"]
    changed = b.with_columns(
        pl.when(pl.col("player_id") == "ruthba01")
        .then(pl.col("hr") + 1)
        .otherwise(pl.col("hr"))
        .alias("hr")
    )
    assert content_hash(b, PRIMARY_KEYS["batting"]) != content_hash(
        changed, PRIMARY_KEYS["batting"]
    )


def test_content_hash_ignores_row_order(small_zip: Path):
    b = load(small_zip)["batting"]
    assert content_hash(b, PRIMARY_KEYS["batting"]) == content_hash(
        b.reverse(), PRIMARY_KEYS["batting"]
    )


def test_dedupe_keeps_the_row_with_fewer_nulls():
    # Confirmed real: Retrosheet's Negro Leagues-era data occasionally has two rows for the
    # same key, apparently merged from two slightly-disagreeing historical sources — one
    # sparser than the other (e.g. blank b_pa/b_ab vs. real values for the same play).
    df = pl.DataFrame(
        {
            "gid": ["G1", "G1", "G2"],
            "id": ["p1", "p1", "p2"],
            "pa": [None, 1, 1],
            "ab": [None, 1, 1],
        }
    )
    out = dedupe_most_complete(df, ["gid", "id"])
    assert out.height == 2
    assert out.filter(pl.col("gid") == "G1")["pa"].to_list() == [1]


def test_dedupe_is_a_no_op_without_duplicate_keys():
    df = pl.DataFrame({"gid": ["G1", "G2"], "id": ["p1", "p2"], "pa": [None, 1]})
    out = dedupe_most_complete(df, ["gid", "id"])
    assert out.sort("gid").equals(df.sort("gid"))


def test_dedupe_breaks_an_exact_tie_deterministically():
    df = pl.DataFrame({"gid": ["G1", "G1"], "id": ["p1", "p1"], "pa": [1, 1]})
    out = dedupe_most_complete(df, ["gid", "id"])
    assert out.height == 1 and out["pa"].to_list() == [1]
