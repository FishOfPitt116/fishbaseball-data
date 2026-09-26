import io
import zipfile
from pathlib import Path

import polars as pl

from pipelines.core.config import SourceConfig
from pipelines.lahman import LAHMAN
from pipelines.lahman.columns import COLUMNS
from pipelines.lahman.tables import TABLES

BOM = "﻿"


def _fixture_headers(zip_path: Path) -> dict[str, list[str]]:
    out = {}
    with zipfile.ZipFile(zip_path) as z:
        for name in z.namelist():
            text = z.read(name).decode("utf-8").lstrip(BOM)
            out[name] = pl.read_csv(io.StringIO(text), n_rows=0, infer_schema=False).columns
    return out


def test_lahman_config_is_a_source_config():
    assert isinstance(LAHMAN, SourceConfig)
    assert LAHMAN.name == "lahman"
    assert LAHMAN.license == "CC BY-SA 3.0"
    assert LAHMAN.page_url == "https://sabr.org/lahman-database/"


def test_version_pattern_matches_sabr_page(fixtures: Path):
    html = (fixtures / "sabr_page.html").read_text()
    m = LAHMAN.version_pattern.search(html)
    assert m is not None
    assert m.groups() == ("2025", "January 2, 2026")


def test_table_names_are_snake_case_and_unique():
    assert len(TABLES) == 27
    assert len(set(TABLES.values())) == len(TABLES)
    assert all(v == v.lower() and " " not in v for v in TABLES.values())
    assert TABLES["FieldingOFsplit.csv"] == "fielding_of_split"
    assert TABLES["BattingPost.csv"] == "batting_post"
    assert TABLES["AllstarFull.csv"] == "allstar_full"


def test_every_table_has_a_column_map():
    assert set(COLUMNS) == set(TABLES.values())


def test_every_fixture_column_has_exactly_one_mapping(small_zip: Path):
    headers = _fixture_headers(small_zip)
    assert set(headers) == set(TABLES)
    for csv_name, cols in headers.items():
        mapping = COLUMNS[TABLES[csv_name]]
        assert set(cols) == set(mapping), csv_name


def test_mapped_names_are_unique_snake_case_within_table():
    for table, mapping in COLUMNS.items():
        names = list(mapping.values())
        assert len(names) == len(set(names)), table
        for n in names:
            assert n == n.lower() and not n[0].isdigit(), (table, n)


def test_documented_conventions():
    b = COLUMNS["batting"]
    assert b["playerID"] == "player_id"
    assert b["2B"] == "doubles" and b["3B"] == "triples"
    assert b["GIDP"] == "gidp"
    p = COLUMNS["pitching"]
    assert p["IPouts"] == "ip_outs" and p["BAOpp"] == "ba_opp"
    assert COLUMNS["people"]["nameFirst"] == "name_first"
    assert COLUMNS["people"]["finalGame"] == "final_game"
    assert COLUMNS["teams"]["franchID"] == "franch_id"
    assert COLUMNS["teams"]["lgID"] == "lg_id"
