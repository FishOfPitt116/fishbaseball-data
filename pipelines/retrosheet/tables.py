"""Normalized CSV name (the per-season file, with its leading year stripped) -> table
name. Explicit on purpose: a new upstream table stops the pipeline until it's added here
deliberately, the same contract as Lahman's tables.py."""

TABLES: dict[str, str] = {
    "allplayers.csv": "all_players",
    "batting.csv": "batting",
    "fielding.csv": "fielding",
    "gameinfo.csv": "game_info",
    "pitching.csv": "pitching",
    "plays.csv": "plays",
    "teamstats.csv": "team_stats",
}
