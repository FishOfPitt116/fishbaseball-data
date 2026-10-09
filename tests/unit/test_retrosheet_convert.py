from pathlib import Path

import polars as pl

from pipelines.retrosheet.convert import read_season
from pipelines.retrosheet.schema import NATIVE_SEASON_TABLES
from tests.conftest import rewrite_zip


def test_stamps_season_onto_every_table_but_the_native_one(retrosheet_season_zips):
    tables = read_season(retrosheet_season_zips[1927], "1927")
    for df in tables.values():
        assert df["season"].unique().to_list() == [1927]
        assert df.schema["season"] == pl.Int16
    assert "game_info" in tables and "game_info" in NATIVE_SEASON_TABLES


def test_member_names_are_normalized_past_the_year_prefix(retrosheet_season_zips):
    tables = read_season(retrosheet_season_zips[1956], "1956")
    assert set(tables) == {
        "game_info", "all_players", "batting", "pitching", "fielding", "team_stats", "plays",
    }  # fmt: skip


def test_a_season_missing_a_column_is_null_filled_not_rejected(
    tmp_path: Path, retrosheet_season_zips
):
    # Confirmed real: Retrosheet's actual 1899 plays.csv lacks 16 columns every other season
    # (1897-2025) has. read_season must tolerate that rather than failing the whole build.
    import csv
    import io

    def drop_balls(name: str, text: str) -> str:
        if name != "1927plays.csv":
            return text
        reader = csv.reader(io.StringIO(text))
        rows = list(reader)
        i = rows[0].index("balls")
        out = io.StringIO()
        csv.writer(out).writerows([r[:i] + r[i + 1 :] for r in rows])
        return out.getvalue()

    z = rewrite_zip(retrosheet_season_zips[1927], tmp_path / "missing_col.zip", drop_balls)
    tables = read_season(z, "1927")
    assert "balls" in tables["plays"].columns
    assert tables["plays"]["balls"].dtype == pl.Int8
    assert tables["plays"]["balls"].null_count() == tables["plays"].height


def test_a_season_with_a_duplicate_key_keeps_the_more_complete_row(
    tmp_path: Path, retrosheet_season_zips
):
    # Confirmed real: Retrosheet's Negro Leagues-era batting/plays/all_players data has 206
    # duplicate-key row pairs (1899, 1933-1945), apparently merged from two disagreeing
    # historical sources — one sparser than the other for the same key.
    import csv
    import io

    def duplicate_first_row_with_a_blank_stat(name: str, text: str) -> str:
        if name != "1927batting.csv":
            return text
        reader = csv.reader(io.StringIO(text))
        rows = list(reader)
        header, first = rows[0], list(rows[1])
        sparse = list(first)
        sparse[header.index("b_h")] = ""  # the complete original row has a real b_h value
        out = io.StringIO()
        csv.writer(out).writerows([header, first, sparse, *rows[2:]])
        return out.getvalue()

    z = rewrite_zip(
        retrosheet_season_zips[1927],
        tmp_path / "dup_key.zip",
        duplicate_first_row_with_a_blank_stat,
    )
    tables = read_season(z, "1927")
    batting = tables["batting"]
    matches = batting.filter(
        (pl.col("gid") == "BSN192704120")
        & (pl.col("id") == "statj101")
        & (pl.col("team") == "BRO")
        & (pl.col("stattype") == "value")
    )
    assert matches.height == 1
    assert matches["b_h"].null_count() == 0
