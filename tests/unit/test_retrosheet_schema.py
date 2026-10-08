import polars as pl
import pytest

from pipelines.retrosheet.schema import KNOWN_LOGIC_EXCEPTIONS, validate
from tests.conftest import read_retrosheet_seasons


@pytest.fixture(scope="module")
def tables(retrosheet_season_zips):
    # module-scoped: converting 3 seasons (notably ~25k `plays` rows) isn't free. Every test
    # that mutates this does `tables = dict(tables)` first (a shallow copy), so the shared
    # DataFrames themselves are never touched in place.
    return read_retrosheet_seasons(retrosheet_season_zips)


def failures(tables, **kw):
    return validate(tables, **kw)


def test_fixture_passes(tables):
    assert failures(tables, full_dataset=True) == []


def test_single_season_passes_without_golden_checks(retrosheet_season_zips):
    one = read_retrosheet_seasons(retrosheet_season_zips, years=(1927,))
    assert failures(one, full_dataset=False) == []


def test_single_season_with_full_dataset_fails_golden_checks(retrosheet_season_zips):
    # 1927-only has no Bonds (2001) or Larsen's game (1956); full_dataset=True still expects them.
    one = read_retrosheet_seasons(retrosheet_season_zips, years=(1927,))
    assert any("golden value" in f for f in failures(one, full_dataset=True))


def test_duplicate_primary_key_fails(tables):
    tables = dict(tables)
    tables["batting"] = pl.concat([tables["batting"], tables["batting"].head(1)])
    assert any("batting" in f and "duplicate" in f for f in failures(tables, full_dataset=True))


def test_orphan_gid_fails(tables):
    tables = dict(tables)
    game_info = tables["game_info"]
    tables["game_info"] = game_info.filter(pl.col("gid") != game_info["gid"][0])
    assert any("orphan gid" in f for f in failures(tables, full_dataset=True))


def test_outs_pre_out_of_range_fails(tables):
    tables = dict(tables)
    plays = tables["plays"]
    bad = plays.head(1).with_columns(pl.lit(3).cast(pl.Int8).alias("outs_pre"))
    tables["plays"] = pl.concat([plays, bad])
    assert any("outs_pre" in f for f in failures(tables, full_dataset=True))


def test_outs_post_out_of_range_fails(tables):
    tables = dict(tables)
    plays = tables["plays"]
    bad = plays.head(1).with_columns(pl.lit(4).cast(pl.Int8).alias("outs_post"))
    tables["plays"] = pl.concat([plays, bad])
    assert any("outs_post" in f for f in failures(tables, full_dataset=True))


def test_np_event_with_outs_pre_3_does_not_fail(tables):
    # Confirmed real (2017 ATL game, pn 60): Retrosheet's "NP" (no play) event — e.g. a
    # substitution logged at the moment the third out ends a half-inning — legitimately
    # carries outs_pre == outs_post == 3; it isn't a real play and shouldn't be range-checked.
    tables = dict(tables)
    plays = tables["plays"]
    np_row = plays.head(1).with_columns(
        pl.lit("NP").alias("event"),
        pl.lit(3).cast(pl.Int8).alias("outs_pre"),
        pl.lit(3).cast(pl.Int8).alias("outs_post"),
    )
    tables["plays"] = pl.concat([plays, np_row])
    assert not any("outs_pre" in f or "outs_post" in f for f in failures(tables, full_dataset=True))


def test_outs_post_less_than_outs_pre_fails(tables):
    tables = dict(tables)
    plays = tables["plays"]
    bad = plays.head(1).with_columns(
        pl.lit(2).cast(pl.Int8).alias("outs_pre"), pl.lit(0).cast(pl.Int8).alias("outs_post")
    )
    tables["plays"] = pl.concat([plays, bad])
    assert any("outs_post < outs_pre" in f for f in failures(tables, full_dataset=True))


def test_negative_lob_fails(tables):
    tables = dict(tables)
    team_stats = tables["team_stats"]
    bad = team_stats.head(1).with_columns(
        pl.lit("ZZZ000000000").alias("gid"), pl.lit(-5).cast(pl.Int8).alias("lob")
    )
    tables["team_stats"] = pl.concat([team_stats, bad])
    assert any("lob" in f for f in failures(tables, full_dataset=True))


def test_known_lob_exception_is_allowed():
    assert ("team_stats", "MEM194107041", "SNO") in KNOWN_LOGIC_EXCEPTIONS


def test_golden_ruth_1927_hr_fails_when_wrong(tables):
    tables = dict(tables)
    batting = tables["batting"]
    tables["batting"] = batting.with_columns(
        pl.when(
            (pl.col("id") == "ruthb101")
            & (pl.col("season") == 1927)
            & (pl.col("stattype") == "value")
            & (pl.col("gametype") == "regular")
        )
        .then(0)
        .otherwise(pl.col("b_hr"))
        .cast(pl.Int8)
        .alias("b_hr")
    )
    assert any("ruthb101" in f and "60" in f for f in failures(tables, full_dataset=True))


def test_golden_larsen_perfect_game_fails_when_wrong(tables):
    tables = dict(tables)
    batting = tables["batting"]
    tables["batting"] = batting.with_columns(
        pl.when((pl.col("gid") == "NYA195610080") & (pl.col("team") == "BRO"))
        .then(1)
        .otherwise(pl.col("b_h"))
        .cast(pl.Int8)
        .alias("b_h")
    )
    assert any("NYA195610080" in f for f in failures(tables, full_dataset=True))


def test_missing_table_fails(tables):
    tables = dict(tables)
    del tables["plays"]
    assert any("plays" in f and "missing" in f for f in failures(tables, full_dataset=True))


def test_coverage_missing_team_stats_for_a_season_fails(tables):
    tables = dict(tables)
    tables["team_stats"] = tables["team_stats"].filter(pl.col("season") != 1956)
    assert any("1956" in f for f in failures(tables, full_dataset=True))


def test_all_players_same_id_and_team_in_different_seasons_is_not_a_duplicate(tables):
    # Confirmed real: the same player+team recurs every season they play there (432 shared
    # (id, team) pairs between just two adjacent real seasons) — `season` must be part of
    # all_players' primary key, or every multi-season player looks like a PK violation.
    tables = dict(tables)
    all_players = tables["all_players"]
    one = all_players.head(1)
    other_season = one.with_columns((pl.col("season") + 1).alias("season"))
    tables["all_players"] = pl.concat([all_players, other_season])
    assert not any(
        "all_players" in f and "duplicate" in f for f in failures(tables, full_dataset=True)
    )


def test_all_players_true_duplicate_still_fails(tables):
    tables = dict(tables)
    all_players = tables["all_players"]
    tables["all_players"] = pl.concat([all_players, all_players.head(1)])
    assert any("all_players" in f and "duplicate" in f for f in failures(tables, full_dataset=True))
