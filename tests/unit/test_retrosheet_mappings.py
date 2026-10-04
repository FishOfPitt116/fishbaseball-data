import zipfile
from pathlib import Path

from pipelines.retrosheet import RETROSHEET
from pipelines.retrosheet.columns import COLUMNS
from pipelines.retrosheet.schema import DTYPES, PRIMARY_KEYS, SEASON_COLUMNS
from pipelines.retrosheet.tables import TABLES


def _real_headers(zip_path: Path, year: int) -> dict[str, list[str]]:
    out = {}
    with zipfile.ZipFile(zip_path) as zf:
        for csv_name, table in TABLES.items():
            member = f"{year}{csv_name}"
            text = zf.read(member).decode("utf-8-sig")
            out[table] = text.splitlines()[0].split(",")
    return out


def test_retrosheet_config_is_a_source_config():
    assert RETROSHEET.name == "retrosheet"
    assert RETROSHEET.partitions is not None
    assert RETROSHEET.partitions.partitioned_tables == set(TABLES.values())


def test_table_names_are_snake_case_and_unique():
    assert len(TABLES) == 7
    assert len(set(TABLES.values())) == len(TABLES)
    assert all(v == v.lower() and " " not in v for v in TABLES.values())
    assert TABLES["gameinfo.csv"] == "game_info"
    assert TABLES["allplayers.csv"] == "all_players"
    assert TABLES["teamstats.csv"] == "team_stats"


def test_every_table_has_a_column_map_and_a_dtype_map():
    assert set(COLUMNS) == set(TABLES.values())
    assert set(DTYPES) == set(TABLES.values())


def test_every_real_fixture_column_has_exactly_one_mapping(retrosheet_season_zips):
    for year, zip_path in retrosheet_season_zips.items():
        headers = _real_headers(zip_path, year)
        for table, cols in headers.items():
            mapping = COLUMNS[table]
            assert set(cols) == set(mapping), (year, table)


def test_columns_is_identity_every_retrosheet_name_is_already_snake_case():
    for table, mapping in COLUMNS.items():
        for source, target in mapping.items():
            assert source == target, (table, source, target)
            assert target == target.lower() and not target[0].isdigit()


def test_dtypes_cover_every_mapped_column_plus_the_stamped_season_column():
    for table, mapping in COLUMNS.items():
        expected = set(mapping.values()) | {"season"}
        assert set(DTYPES[table]) == expected, table


def test_primary_keys_are_subsets_of_each_table_and_cover_every_table():
    assert set(PRIMARY_KEYS) == set(TABLES.values())
    for table, key in PRIMARY_KEYS.items():
        assert set(key) <= set(DTYPES[table]), table
        assert len(key) == len(set(key)), table  # no repeated column in a key


def test_season_column_is_uniformly_named():
    assert set(SEASON_COLUMNS.values()) == {"season"}
    assert set(SEASON_COLUMNS) == set(TABLES.values())
