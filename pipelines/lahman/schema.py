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


# ---- validation -------------------------------------------------------------------------

GOLDEN_HR = {  # (player_id, year_id or None for career) -> home runs
    ("ruthba01", 1927): 60,
    ("bondsba01", 2001): 73,
    ("aaronha01", None): 755,
}
MIN_TEAMS_LATEST_SEASON = 30

# Known upstream data errors (report to lahmandb@sabr.org): (table, player_id, year_id).
# tayloci99 1912 WBS has 1 HR on 0 H in Lahman 2025.
KNOWN_LOGIC_EXCEPTIONS = {("batting", "tayloci99", 1912)}

# tables whose player_id must exist in `people`
_PLAYER_TABLES = (
    "allstar_full appearances awards_players awards_share_players batting batting_post "
    "college_playing fielding fielding_of fielding_of_split fielding_post hall_of_fame "
    "managers managers_half pitching pitching_post salaries"
).split()


def _orphans(child: pl.DataFrame, col: str, parent: pl.DataFrame, parent_col: str) -> list[str]:
    known = parent.select(pl.col(parent_col).alias(col)).drop_nulls().unique()
    present = child.select(col).drop_nulls().unique()
    return sorted(present.join(known, on=col, how="anti")[col].to_list())


def _structure(tables: dict[str, pl.DataFrame]) -> list[str]:
    out: list[str] = []
    for name in sorted(set(DTYPES) - set(tables)):
        out.append(f"{name}: table missing")
    for name in sorted(set(tables) - set(DTYPES)):
        out.append(f"{name}: unexpected table")
    for name, df in tables.items():
        if name not in DTYPES:
            continue
        for col, dtype in DTYPES[name].items():
            if col not in df.columns:
                out.append(f"{name}.{col}: column missing")
            elif df.schema[col] != dtype:
                out.append(f"{name}.{col}: dtype {df.schema[col]}, expected {dtype}")
        for col in sorted(set(df.columns) - set(DTYPES[name])):
            out.append(f"{name}.{col}: unexpected column")
    return out


def _keys(tables: dict[str, pl.DataFrame]) -> list[str]:
    out: list[str] = []
    for name, df in tables.items():
        key = PRIMARY_KEYS.get(name)
        if not key or not set(key) <= set(df.columns):
            continue
        dups = df.height - df.unique(subset=key).height
        if dups:
            out.append(f"{name}: {dups} duplicate row(s) on primary key {key}")
    return out


def _referential(tables: dict[str, pl.DataFrame]) -> list[str]:
    out: list[str] = []
    people = tables.get("people")
    if people is not None:
        for name in _PLAYER_TABLES:
            df = tables.get(name)
            if df is None or "player_id" not in df.columns:
                continue
            bad = _orphans(df, "player_id", people, "player_id")
            if bad:
                out.append(f"{name}: {len(bad)} orphan player_id(s) not in people, e.g. {bad[:5]}")
    teams, franchises = tables.get("teams"), tables.get("teams_franchises")
    if teams is not None and franchises is not None:
        bad = _orphans(teams, "franch_id", franchises, "franch_id")
        if bad:
            out.append(
                f"teams: {len(bad)} orphan franch_id(s) not in teams_franchises, e.g. {bad[:5]}"
            )
    return out


def _logic(tables: dict[str, pl.DataFrame]) -> list[str]:
    out: list[str] = []
    for name in ("batting", "batting_post"):
        df = tables.get(name)
        if df is None:
            continue
        for t, player, year in KNOWN_LOGIC_EXCEPTIONS:
            if t == name:
                df = df.filter(~((pl.col("player_id") == player) & (pl.col("year_id") == year)))
        if df.filter(pl.col("h") > pl.col("ab")).height:
            out.append(f"{name}: rows with h > ab")
        if df.filter(pl.col("hr") > pl.col("h")).height:
            out.append(f"{name}: rows with hr > h")
        extra = pl.col("doubles") + pl.col("triples") + pl.col("hr")
        if df.filter(extra > pl.col("h")).height:
            out.append(f"{name}: rows with extra-base hits > h")
    pitching = tables.get("pitching")
    if pitching is not None and pitching.filter(pl.col("ip_outs") < 0).height:
        out.append("pitching: negative ip_outs")
    people = tables.get("people")
    if people is not None:
        ranges = {
            "birth_month": (1, 12),
            "death_month": (1, 12),
            "birth_day": (1, 31),
            "death_day": (1, 31),
            "birth_year": (1500, 2100),
            "death_year": (1500, 2100),
        }
        for col, (lo, hi) in ranges.items():
            if people.filter((pl.col(col) < lo) | (pl.col(col) > hi)).height:
                out.append(f"people.{col}: values outside {lo}..{hi}")
    return out


def _latest_season(tables: dict[str, pl.DataFrame], full_dataset: bool) -> list[str]:
    out: list[str] = []
    seasons = {
        n: tables[n]["year_id"].max()
        for n in ("batting", "pitching", "teams", "fielding")
        if n in tables and "year_id" in tables[n].columns
    }
    if len(set(seasons.values())) > 1:
        out.append(f"latest season differs across tables: {seasons}")
    teams = tables.get("teams")
    if teams is not None and "year_id" in teams.columns:
        latest = teams["year_id"].max()
        if (
            full_dataset
            and teams.filter(pl.col("year_id") == latest).height < MIN_TEAMS_LATEST_SEASON
        ):
            out.append(
                f"teams: fewer than {MIN_TEAMS_LATEST_SEASON} teams in latest season {latest}"
            )
    return out


def _golden(tables: dict[str, pl.DataFrame]) -> list[str]:
    out: list[str] = []
    batting = tables.get("batting")
    if batting is None:
        return out
    for (player, year), expected in GOLDEN_HR.items():
        rows = batting.filter(pl.col("player_id") == player)
        if year is not None:
            rows = rows.filter(pl.col("year_id") == year)
        got = rows["hr"].sum()
        if got != expected:
            where = f"{year}" if year else "career"
            out.append(f"golden value: {player} {where} hr is {got}, expected {expected}")
    return out


def validate(tables: dict[str, pl.DataFrame], *, full_dataset: bool = True) -> list[str]:
    """Return every failure found (empty list means valid). `full_dataset=False` relaxes checks
    that only make sense on the complete database (e.g. a full league in the latest season)."""
    structure = _structure(tables)
    if structure:  # later checks assume the shape is right
        return structure
    return [
        *_keys(tables),
        *_referential(tables),
        *_logic(tables),
        *_latest_season(tables, full_dataset),
        *_golden(tables),
    ]
