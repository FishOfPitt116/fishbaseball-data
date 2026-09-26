"""Lahman dtypes, primary keys and (in `validate`) checks. Owned by this repo and published
in the manifest via `schema_version`.

Conventions: counts -> Int32; ids and codes -> Utf8; flags (Y/N) -> Utf8; debut, final_game and
span dates -> Date; fractional stats -> Float64. Values that fail to cast are failures, never nulls.

Keys, dtypes and year columns of AllstarFull, Appearances, the four Awards tables, FieldingOF and
FieldingOFsplit were written from earlier releases (SABR's Box listing hid them); confirm on the
first real run.
"""

from __future__ import annotations

import polars as pl

from pipelines.lahman.columns import COLUMNS

SCHEMA_VERSION = 1

_BATTING = "year_id g ab r h doubles triples hr rbi sb cs bb so ibb hbp sh sf gidp"
_PITCHING = "year_id w l g gs cg sho sv ip_outs h er hr bb so ibb wp hbp bk bfp gf r sh sf gidp"

# snake_case columns that are not Utf8, per table: (Int32 columns, Float64 columns, Date columns)
_NON_STRING: dict[str, tuple[str, str, str]] = {
    "allstar_full": ("year_id game_num gp", "", ""),
    "appearances": (
        "year_id g_all gs g_batting g_defense g_p g_c g_1b g_2b g_3b g_ss g_lf g_cf g_rf g_of"
        " g_dh g_ph g_pr",
        "",
        "",
    ),
    "awards_managers": ("year_id", "", ""),
    "awards_players": ("year_id", "", ""),
    "awards_share_managers": ("year_id", "points_won points_max votes_first", ""),
    "awards_share_players": ("year_id", "points_won points_max votes_first", ""),
    "batting": ("stint " + _BATTING, "", ""),
    "batting_post": (_BATTING, "", ""),
    "college_playing": ("year_id", "", ""),
    "fielding": ("year_id stint g gs inn_outs po a e dp pb wp sb cs zr", "", ""),
    "fielding_of": ("year_id stint g_lf g_cf g_rf", "", ""),
    "fielding_of_split": ("year_id stint g gs inn_outs po a e dp", "", ""),
    "fielding_post": ("year_id g gs inn_outs po a e dp tp pb sb cs", "", ""),
    "hall_of_fame": ("year_id ballots needed votes", "", ""),
    "home_games": ("year_key games openings attendance", "", "span_first span_last"),
    "managers": ("year_id inseason g w l rank", "", ""),
    "managers_half": ("year_id inseason half g w l rank", "", ""),
    "parks": ("id", "", ""),
    "people": (
        "birth_year birth_month birth_day death_year death_month death_day weight height",
        "",
        "debut final_game",
    ),
    "pitching": ("stint " + _PITCHING, "ba_opp era", ""),
    "pitching_post": (_PITCHING, "ba_opp era", ""),
    "salaries": ("year_id salary", "", ""),
    "schools": ("", "", ""),
    "series_post": ("year_id wins losses ties", "", ""),
    "teams": (
        "year_id rank g g_home w l r ab h doubles triples hr bb so sb cs hbp sf ra er cg sho sv"
        " ip_outs ha hra bba soa e dp attendance bpf ppf",
        "era fp",
        "",
    ),
    "teams_franchises": ("", "", ""),
    "teams_half": ("year_id half rank g w l", "", ""),
}


def _dtypes() -> dict[str, dict[str, pl.DataType]]:
    out: dict[str, dict[str, pl.DataType]] = {}
    for table, mapping in COLUMNS.items():
        ints, floats, dates = (set(s.split()) for s in _NON_STRING[table])
        cols: dict[str, pl.DataType] = {}
        for name in mapping.values():
            if name in ints:
                cols[name] = pl.Int32()
            elif name in floats:
                cols[name] = pl.Float64()
            elif name in dates:
                cols[name] = pl.Date()
            else:
                cols[name] = pl.Utf8()
        unknown = (ints | floats | dates) - set(cols)
        assert not unknown, (table, unknown)
        out[table] = cols
    return out


DTYPES: dict[str, dict[str, pl.DataType]] = _dtypes()

PRIMARY_KEYS: dict[str, list[str]] = {
    "allstar_full": ["player_id", "year_id", "game_num", "game_id", "team_id"],
    "appearances": ["year_id", "team_id", "player_id"],
    "awards_managers": ["year_id", "award_id", "lg_id", "player_id"],
    "awards_players": ["year_id", "award_id", "lg_id", "player_id"],
    "awards_share_managers": ["award_id", "year_id", "lg_id", "player_id"],
    "awards_share_players": ["award_id", "year_id", "lg_id", "player_id"],
    "batting": ["player_id", "year_id", "stint"],
    "batting_post": ["year_id", "round", "player_id"],
    "college_playing": ["player_id", "school_id", "year_id"],
    "fielding": ["player_id", "year_id", "stint", "pos"],
    "fielding_of": ["player_id", "year_id", "stint"],
    "fielding_of_split": ["player_id", "year_id", "stint", "pos"],
    "fielding_post": ["player_id", "year_id", "round", "pos"],
    "hall_of_fame": ["player_id", "year_id", "voted_by", "category"],
    "home_games": ["year_key", "league_key", "team_key", "park_key"],
    "managers": ["year_id", "team_id", "inseason"],
    "managers_half": ["player_id", "year_id", "team_id", "half"],
    "parks": ["id"],
    "people": ["player_id"],
    "pitching": ["player_id", "year_id", "stint"],
    "pitching_post": ["player_id", "year_id", "round"],
    "salaries": ["year_id", "team_id", "player_id"],
    "schools": ["school_id"],
    "series_post": ["year_id", "round"],
    "teams": ["year_id", "team_id"],
    "teams_franchises": ["franch_id"],
    "teams_half": ["year_id", "team_id", "half"],
}

# Column holding the season, for min_year/max_year in the manifest (absent: no season column).
YEAR_COLUMNS: dict[str, str] = {
    t: c for t in COLUMNS for c in ("year_id", "year_key") if c in COLUMNS[t].values()
}
