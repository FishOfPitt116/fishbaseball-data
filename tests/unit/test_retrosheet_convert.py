import polars as pl

from pipelines.retrosheet.convert import read_season
from pipelines.retrosheet.schema import NATIVE_SEASON_TABLES


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
