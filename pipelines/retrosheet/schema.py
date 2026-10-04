"""Retrosheet dtypes, primary keys and (in `validate`) checks. Owned by this repo and
published in the manifest via `schema_version`.

Conventions, inferred programmatically from the real full-history data (1897-2025) rather
than guessed: 0/1 flags and small counts -> Int8; a handful of columns that need more
headroom (timeofgame, pn, all_players' per-position game counts, ids/dates) -> Int16/Int32;
`date` -> Date (format "%Y%m%d", not Lahman's hyphenated form). A small number of columns
that look numeric but carry real non-numeric content upstream (an estimate marker on
attendance, a qualitative temp reading, inning-by-inning line scores that use 'x' for an
inning that didn't happen) are kept Utf8 rather than forced into a lossy cast.

Every Retrosheet CSV is scoped to one season and single-character-sentinel "?" marks a
missing value in an otherwise-numeric column (normalized to null before casting, same as
any other blank). None of this repo's 27 Lahman tables are affected by anything here.
"""

from __future__ import annotations

import polars as pl

SCHEMA_VERSION = 1

# The partition (season) column every table ends up with. Only "game_info" carries it in
# the real per-season CSV already; the other six get it stamped on during conversion from
# the partition key itself (none of them has a usable per-row date/season field of their
# own that covers every row — `all_players` has neither `gid` nor `date` at all).
SEASON_COLUMN = "season"
NATIVE_SEASON_TABLES = frozenset({"game_info"})

_PL_DTYPES = {
    "Int8": pl.Int8,
    "Int16": pl.Int16,
    "Int32": pl.Int32,
    "Utf8": pl.Utf8,
    "Date": pl.Date,
}

# Column -> type name, per table, as the *final* published shape (after season-stamping).
_DTYPE_NAMES: dict[str, dict[str, str]] = {
    "game_info": {
        "gid": "Utf8",
        "visteam": "Utf8",
        "hometeam": "Utf8",
        "site": "Utf8",
        "date": "Date",
        "number": "Int8",
        "starttime": "Utf8",
        "daynight": "Utf8",
        "innings": "Int8",
        "tiebreaker": "Int8",
        "usedh": "Utf8",
        "htbf": "Utf8",
        "timeofgame": "Int16",
        "attendance": "Utf8",
        "fieldcond": "Utf8",
        "precip": "Utf8",
        "sky": "Utf8",
        "temp": "Utf8",
        "winddir": "Utf8",
        "windspeed": "Int8",
        "oscorer": "Utf8",
        "forfeit": "Utf8",
        "suspend": "Int32",
        "umphome": "Utf8",
        "ump1b": "Utf8",
        "ump2b": "Utf8",
        "ump3b": "Utf8",
        "umplf": "Utf8",
        "umprf": "Utf8",
        "wp": "Utf8",
        "lp": "Utf8",
        "save": "Utf8",
        "gametype": "Utf8",
        "vruns": "Int8",
        "hruns": "Int8",
        "wteam": "Utf8",
        "lteam": "Utf8",
        "line": "Utf8",
        "batteries": "Utf8",
        "lineups": "Utf8",
        "box": "Utf8",
        "pbp": "Utf8",
        "season": "Int16",
    },
    "all_players": {
        "id": "Utf8",
        "last": "Utf8",
        "first": "Utf8",
        "bat": "Utf8",
        "throw": "Utf8",
        "team": "Utf8",
        "g": "Int16",
        "g_p": "Int8",
        "g_sp": "Int8",
        "g_rp": "Int8",
        "g_c": "Int16",
        "g_1b": "Int16",
        "g_2b": "Int16",
        "g_3b": "Int16",
        "g_ss": "Int16",
        "g_lf": "Int16",
        "g_cf": "Int16",
        "g_rf": "Int16",
        "g_of": "Int16",
        "g_dh": "Int16",
        "g_ph": "Int8",
        "g_pr": "Int8",
        "first_g": "Int32",
        "last_g": "Int32",
        "season": "Int16",
    },
    "pitching": {
        "gid": "Utf8",
        "id": "Utf8",
        "team": "Utf8",
        "p_seq": "Int8",
        "stattype": "Utf8",
        "p_ipouts": "Int8",
        "p_noout": "Int8",
        "p_bfp": "Int8",
        "p_h": "Int8",
        "p_d": "Int8",
        "p_t": "Int8",
        "p_hr": "Int8",
        "p_r": "Int8",
        "p_er": "Int8",
        "p_w": "Int8",
        "p_iw": "Int8",
        "p_k": "Int8",
        "p_hbp": "Int8",
        "p_wp": "Int8",
        "p_bk": "Int8",
        "p_sh": "Int8",
        "p_sf": "Int8",
        "p_sb": "Int8",
        "p_cs": "Int8",
        "p_pb": "Int8",
        "wp": "Int8",
        "lp": "Int8",
        "save": "Int8",
        "p_gs": "Int8",
        "p_gf": "Int8",
        "p_cg": "Int8",
        "date": "Date",
        "number": "Int8",
        "site": "Utf8",
        "vishome": "Utf8",
        "opp": "Utf8",
        "win": "Int8",
        "loss": "Int8",
        "tie": "Int8",
        "gametype": "Utf8",
        "box": "Utf8",
        "pbp": "Utf8",
        "season": "Int16",
    },
    "batting": {
        "gid": "Utf8",
        "id": "Utf8",
        "team": "Utf8",
        "b_lp": "Int8",
        "b_seq": "Int8",
        "stattype": "Utf8",
        "b_pa": "Int8",
        "b_ab": "Int8",
        "b_r": "Int8",
        "b_h": "Int8",
        "b_d": "Int8",
        "b_t": "Int8",
        "b_hr": "Int8",
        "b_rbi": "Int8",
        "b_sh": "Int8",
        "b_sf": "Int8",
        "b_hbp": "Int8",
        "b_w": "Int8",
        "b_iw": "Int8",
        "b_k": "Int8",
        "b_sb": "Int8",
        "b_cs": "Int8",
        "b_gdp": "Int8",
        "b_xi": "Int8",
        "b_roe": "Int8",
        "dh": "Int8",
        "ph": "Int8",
        "pr": "Int8",
        "date": "Date",
        "number": "Int8",
        "site": "Utf8",
        "vishome": "Utf8",
        "opp": "Utf8",
        "win": "Int8",
        "loss": "Int8",
        "tie": "Int8",
        "gametype": "Utf8",
        "box": "Utf8",
        "pbp": "Utf8",
        "season": "Int16",
    },
    "fielding": {
        "gid": "Utf8",
        "id": "Utf8",
        "team": "Utf8",
        "d_seq": "Int8",
        "d_pos": "Int8",
        "stattype": "Utf8",
        "d_ifouts": "Int8",
        "d_po": "Int8",
        "d_a": "Int8",
        "d_e": "Int8",
        "d_dp": "Int8",
        "d_tp": "Int8",
        "d_pb": "Int8",
        "d_wp": "Int8",
        "d_sb": "Int8",
        "d_cs": "Int8",
        "d_gs": "Int8",
        "date": "Date",
        "number": "Int8",
        "site": "Utf8",
        "vishome": "Utf8",
        "opp": "Utf8",
        "win": "Int8",
        "loss": "Int8",
        "tie": "Int8",
        "gametype": "Utf8",
        "box": "Utf8",
        "pbp": "Utf8",
        "season": "Int16",
    },
    "team_stats": {
        "gid": "Utf8",
        "team": "Utf8",
        "inn1": "Utf8",
        "inn2": "Utf8",
        "inn3": "Utf8",
        "inn4": "Utf8",
        "inn5": "Utf8",
        "inn6": "Utf8",
        "inn7": "Utf8",
        "inn8": "Utf8",
        "inn9": "Utf8",
        "inn10": "Utf8",
        "inn11": "Utf8",
        "inn12": "Utf8",
        "inn13": "Utf8",
        "inn14": "Utf8",
        "inn15": "Utf8",
        "inn16": "Utf8",
        "inn17": "Utf8",
        "inn18": "Utf8",
        "inn19": "Utf8",
        "inn20": "Utf8",
        "inn21": "Utf8",
        "inn22": "Utf8",
        "inn23": "Utf8",
        "inn24": "Utf8",
        "inn25": "Utf8",
        "inn26": "Utf8",
        "inn27": "Utf8",
        "inn28": "Utf8",
        "lob": "Int8",
        "mgr": "Utf8",
        "stattype": "Utf8",
        "b_pa": "Int8",
        "b_ab": "Int8",
        "b_r": "Int8",
        "b_h": "Int8",
        "b_d": "Int8",
        "b_t": "Int8",
        "b_hr": "Int8",
        "b_rbi": "Int8",
        "b_sh": "Int8",
        "b_sf": "Int8",
        "b_hbp": "Int8",
        "b_w": "Int8",
        "b_iw": "Int8",
        "b_k": "Int8",
        "b_sb": "Int8",
        "b_cs": "Int8",
        "b_gdp": "Int8",
        "b_xi": "Int8",
        "b_roe": "Int8",
        "p_ipouts": "Int8",
        "p_noout": "Int8",
        "p_bfp": "Int8",
        "p_h": "Int8",
        "p_d": "Int8",
        "p_t": "Int8",
        "p_hr": "Int8",
        "p_r": "Int8",
        "p_er": "Int8",
        "p_w": "Int8",
        "p_iw": "Int8",
        "p_k": "Int8",
        "p_hbp": "Int8",
        "p_wp": "Int8",
        "p_bk": "Int8",
        "p_sh": "Int8",
        "p_sf": "Int8",
        "p_sb": "Int8",
        "p_cs": "Int8",
        "p_pb": "Int8",
        "d_po": "Int8",
        "d_a": "Int8",
        "d_e": "Int8",
        "d_dp": "Int8",
        "d_tp": "Int8",
        "d_pb": "Int8",
        "d_wp": "Int8",
        "d_sb": "Int8",
        "d_cs": "Int8",
        "start_l1": "Utf8",
        "start_l2": "Utf8",
        "start_l3": "Utf8",
        "start_l4": "Utf8",
        "start_l5": "Utf8",
        "start_l6": "Utf8",
        "start_l7": "Utf8",
        "start_l8": "Utf8",
        "start_l9": "Utf8",
        "start_f1": "Utf8",
        "start_f2": "Utf8",
        "start_f3": "Utf8",
        "start_f4": "Utf8",
        "start_f5": "Utf8",
        "start_f6": "Utf8",
        "start_f7": "Utf8",
        "start_f8": "Utf8",
        "start_f9": "Utf8",
        "start_f10": "Utf8",
        "date": "Date",
        "number": "Int8",
        "site": "Utf8",
        "vishome": "Utf8",
        "opp": "Utf8",
        "win": "Int8",
        "loss": "Int8",
        "tie": "Int8",
        "gametype": "Utf8",
        "box": "Utf8",
        "pbp": "Utf8",
        "season": "Int16",
    },
    "plays": {
        "gid": "Utf8",
        "event": "Utf8",
        "inning": "Int8",
        "top_bot": "Int8",
        "vis_home": "Int8",
        "site": "Utf8",
        "batteam": "Utf8",
        "pitteam": "Utf8",
        "score_v": "Int8",
        "score_h": "Int8",
        "batter": "Utf8",
        "pitcher": "Utf8",
        "lp": "Int8",
        "bat_f": "Int8",
        "bathand": "Utf8",
        "pithand": "Utf8",
        "balls": "Int8",
        "strikes": "Int8",
        "count": "Utf8",
        "pitches": "Utf8",
        "nump": "Int8",
        "pa": "Int8",
        "ab": "Int8",
        "single": "Int8",
        "double": "Int8",
        "triple": "Int8",
        "hr": "Int8",
        "sh": "Int8",
        "sf": "Int8",
        "hbp": "Int8",
        "walk": "Int8",
        "k": "Int8",
        "xi": "Int8",
        "roe": "Int8",
        "fc": "Int8",
        "othout": "Int8",
        "noout": "Int8",
        "oth": "Int8",
        "bip": "Int8",
        "bunt": "Int8",
        "ground": "Int8",
        "fly": "Int8",
        "line": "Int8",
        "iw": "Int8",
        "gdp": "Int8",
        "othdp": "Int8",
        "tp": "Int8",
        "fle": "Int8",
        "wp": "Int8",
        "pb": "Int8",
        "bk": "Int8",
        "oa": "Int8",
        "di": "Int8",
        "sb2": "Int8",
        "sb3": "Int8",
        "sbh": "Int8",
        "cs2": "Int8",
        "cs3": "Int8",
        "csh": "Int8",
        "pko1": "Int8",
        "pko2": "Int8",
        "pko3": "Int8",
        "k_safe": "Int8",
        "e1": "Int8",
        "e2": "Int8",
        "e3": "Int8",
        "e4": "Int8",
        "e5": "Int8",
        "e6": "Int8",
        "e7": "Int8",
        "e8": "Int8",
        "e9": "Int8",
        "outs_pre": "Int8",
        "outs_post": "Int8",
        "br1_pre": "Utf8",
        "br2_pre": "Utf8",
        "br3_pre": "Utf8",
        "br1_post": "Utf8",
        "br2_post": "Utf8",
        "br3_post": "Utf8",
        "lob_id1": "Utf8",
        "lob_id2": "Utf8",
        "lob_id3": "Utf8",
        "pr1_pre": "Utf8",
        "pr2_pre": "Utf8",
        "pr3_pre": "Utf8",
        "pr1_post": "Utf8",
        "pr2_post": "Utf8",
        "pr3_post": "Utf8",
        "run_b": "Utf8",
        "run1": "Utf8",
        "run2": "Utf8",
        "run3": "Utf8",
        "prun_b": "Utf8",
        "prun1": "Utf8",
        "prun2": "Utf8",
        "prun3": "Utf8",
        "ur_b": "Int8",
        "ur1": "Int8",
        "ur2": "Int8",
        "ur3": "Int8",
        "rbi_b": "Int8",
        "rbi1": "Int8",
        "rbi2": "Int8",
        "rbi3": "Int8",
        "runs": "Int8",
        "rbi": "Int8",
        "er": "Int8",
        "tur": "Int8",
        "l1": "Utf8",
        "l2": "Utf8",
        "l3": "Utf8",
        "l4": "Utf8",
        "l5": "Utf8",
        "l6": "Utf8",
        "l7": "Utf8",
        "l8": "Utf8",
        "l9": "Utf8",
        "lf1": "Int8",
        "lf2": "Int8",
        "lf3": "Int8",
        "lf4": "Int8",
        "lf5": "Int8",
        "lf6": "Int8",
        "lf7": "Int8",
        "lf8": "Int8",
        "lf9": "Int8",
        "f2": "Utf8",
        "f3": "Utf8",
        "f4": "Utf8",
        "f5": "Utf8",
        "f6": "Utf8",
        "f7": "Utf8",
        "f8": "Utf8",
        "f9": "Utf8",
        "po0": "Int8",
        "po1": "Int8",
        "po2": "Int8",
        "po3": "Int8",
        "po4": "Int8",
        "po5": "Int8",
        "po6": "Int8",
        "po7": "Int8",
        "po8": "Int8",
        "po9": "Int8",
        "a1": "Int8",
        "a2": "Int8",
        "a3": "Int8",
        "a4": "Int8",
        "a5": "Int8",
        "a6": "Int8",
        "a7": "Int8",
        "a8": "Int8",
        "a9": "Int8",
        "fseq": "Utf8",
        "batout1": "Int8",
        "batout2": "Int8",
        "batout3": "Int8",
        "brout_b": "Int8",
        "brout1": "Int8",
        "brout2": "Int8",
        "brout3": "Int8",
        "firstf": "Int8",
        "loc": "Utf8",
        "hittype": "Utf8",
        "dpopp": "Int8",
        "pivot": "Int8",
        "pn": "Int16",
        "umphome": "Utf8",
        "ump1b": "Utf8",
        "ump2b": "Utf8",
        "ump3b": "Utf8",
        "umplf": "Utf8",
        "umprf": "Utf8",
        "date": "Date",
        "gametype": "Utf8",
        "pbp": "Utf8",
        "season": "Int16",
    },
}

DTYPES: dict[str, dict[str, pl.DataType]] = {
    table: {col: _PL_DTYPES[name]() for col, name in cols.items()}
    for table, cols in _DTYPE_NAMES.items()
}

# What `read_tables()` should cast, before season-stamping: every table's real CSV columns
# (DTYPES minus the stamped `season` column, except for game_info, which has it natively).
CSV_DTYPES: dict[str, dict[str, pl.DataType]] = {
    table: {c: t for c, t in cols.items() if c != SEASON_COLUMN or table in NATIVE_SEASON_TABLES}
    for table, cols in DTYPES.items()
}

PRIMARY_KEYS: dict[str, list[str]] = {
    "game_info": ["gid"],
    "all_players": ["id", "team"],
    "batting": ["gid", "id", "team", "stattype"],
    "pitching": ["gid", "id", "team", "stattype"],
    "fielding": ["gid", "id", "team", "d_seq", "d_pos", "stattype"],
    "team_stats": ["gid", "team", "stattype"],
    "plays": ["gid", "pn"],
}

SEASON_COLUMNS: dict[str, str] = {table: SEASON_COLUMN for table in DTYPES}

# ---- validation -------------------------------------------------------------------------

GOLDEN_HR = {  # (table, player_id, season) -> regular-season home runs, stattype="value"
    ("batting", "ruthb101", 1927): 60,
    ("batting", "bondb001", 2001): 73,
}
# Larsen's perfect game, 1956 World Series Game 5: Brooklyn made 27 outs on 27 at-bats, 0 hits.
PERFECT_GAME = {"gid": "NYA195610080", "team": "BRO", "b_ab": 27, "b_h": 0}

# Known upstream data errors (report to Retrosheet): (table, gid, team) -> which check to skip.
# A 1941 Negro Leagues (Memphis/Seamheads-sourced) row has lob=-31, physically impossible.
KNOWN_LOGIC_EXCEPTIONS = {("team_stats", "MEM194107041", "SNO")}


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
    game_info = tables.get("game_info")
    if game_info is None:
        return out
    for name in ("plays", "team_stats", "batting", "pitching", "fielding"):
        df = tables.get(name)
        if df is None or "gid" not in df.columns:
            continue
        bad = _orphans(df, "gid", game_info, "gid")
        if bad:
            out.append(f"{name}: {len(bad)} orphan gid(s) not in game_info, e.g. {bad[:5]}")
    return out


def _logic(tables: dict[str, pl.DataFrame]) -> list[str]:
    out: list[str] = []
    plays = tables.get("plays")
    if plays is not None:
        if plays.filter(~pl.col("outs_pre").is_in([0, 1, 2])).height:
            out.append("plays: outs_pre outside {0,1,2}")
        if plays.filter(~pl.col("outs_post").is_in([0, 1, 2, 3])).height:
            out.append("plays: outs_post outside {0,1,2,3}")
        if plays.filter(pl.col("outs_post") < pl.col("outs_pre")).height:
            out.append("plays: a row with outs_post < outs_pre")
    team_stats = tables.get("team_stats")
    if team_stats is not None:
        exceptions = {gid for t, gid, _ in KNOWN_LOGIC_EXCEPTIONS if t == "team_stats"}
        checked = team_stats.filter(
            ~pl.col("gid").is_in(exceptions) if exceptions else pl.lit(True)
        )
        if checked.filter(pl.col("lob") < 0).height:
            out.append("team_stats: rows with lob < 0")
    return out


def _golden(tables: dict[str, pl.DataFrame]) -> list[str]:
    out: list[str] = []
    batting = tables.get("batting")
    if batting is not None:
        for (table, player, season), expected in GOLDEN_HR.items():
            if table != "batting":
                continue
            got = batting.filter(
                (pl.col("id") == player)
                & (pl.col("season") == season)
                & (pl.col("stattype") == "value")
                & (pl.col("gametype") == "regular")
            )["b_hr"].sum()
            if got != expected:
                out.append(
                    f"golden value: {player} {season} regular hr is {got}, expected {expected}"
                )
        pg = PERFECT_GAME
        row = batting.filter(
            (pl.col("gid") == pg["gid"])
            & (pl.col("team") == pg["team"])
            & (pl.col("stattype") == "value")
        )
        if row.height:
            ab, h = row["b_ab"].sum(), row["b_h"].sum()
            if (ab, h) != (pg["b_ab"], pg["b_h"]):
                out.append(
                    f"golden value: {pg['gid']} {pg['team']} ab/h is {ab}/{h}, expected "
                    f"{pg['b_ab']}/{pg['b_h']}"
                )
    return out


def _coverage(tables: dict[str, pl.DataFrame]) -> list[str]:
    """Every season with games also has team stats for them. Not checked: exact game counts
    per season (too season-dependent — lockouts and strikes legitimately shrink a season)."""
    out: list[str] = []
    game_info, team_stats = tables.get("game_info"), tables.get("team_stats")
    if game_info is not None and team_stats is not None:
        missing = sorted(set(game_info["season"].unique()) - set(team_stats["season"].unique()))
        if missing:
            out.append(f"team_stats: no rows for season(s) with games: {missing}")
    return out


def validate(tables: dict[str, pl.DataFrame], *, full_dataset: bool = True) -> list[str]:
    """Return every failure found (empty list means valid). `full_dataset=False` skips the
    golden-value checks, which assume specific historical seasons (Ruth's 1927, Bonds' 2001,
    Larsen's 1956 World Series game) are present — true for the full build, not for a single-
    or few-season partial one (e.g. a dry run against one season's zip)."""
    structure = _structure(tables)
    if structure:
        return structure
    return [
        *_keys(tables),
        *_referential(tables),
        *_logic(tables),
        *(_golden(tables) if full_dataset else []),
        *_coverage(tables),
    ]
