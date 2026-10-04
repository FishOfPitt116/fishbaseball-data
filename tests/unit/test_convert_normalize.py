"""`read_tables`'s optional member-name normalizer, for sources whose per-partition zips
prefix every CSV with a key (Retrosheet: the season year, e.g. "2024batting.csv")."""

import re
import zipfile
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from pipelines.core.config import SourceConfig
from pipelines.core.convert import CastError, InventoryError, read_tables

CONFIG = SourceConfig(
    name="x",
    page_url="https://example.test",
    version_pattern=re.compile(r"v(\d+)"),
    download_urls=(),
    tables={"batting.csv": "batting", "people.csv": "people"},
    columns={
        "batting": {"id": "id", "hr": "hr"},
        "people": {"id": "id", "name": "name"},
    },
    license="x",
    attribution="x",
)
DTYPES = {
    "batting": {"id": pl.Utf8, "hr": pl.Int32},
    "people": {"id": pl.Utf8, "name": pl.Utf8},
}
PRIMARY_KEYS = {"batting": ["id"], "people": ["id"]}
STRIP_YEAR = re.compile(r"^\d{4}")


def normalize(name: str) -> str:
    return STRIP_YEAR.sub("", name)


def make_zip(path: Path, members: dict[str, str]) -> Path:
    with zipfile.ZipFile(path, "w") as zf:
        for name, content in members.items():
            zf.writestr(name, content)
    return path


def test_normalizes_year_prefixed_member_names(tmp_path: Path):
    z = make_zip(
        tmp_path / "2024.zip",
        {"2024batting.csv": "id,hr\nruthba01,60\n", "2024people.csv": "id,name\nruthba01,Ruth\n"},
    )
    tables = read_tables(z, CONFIG, DTYPES, PRIMARY_KEYS, normalize_member_name=normalize)
    assert tables["batting"]["hr"].to_list() == [60]
    assert tables["people"]["name"].to_list() == ["Ruth"]


def test_without_normalizer_year_prefixed_names_are_unknown(tmp_path: Path):
    z = make_zip(tmp_path / "2024.zip", {"2024batting.csv": "id,hr\nruthba01,60\n"})
    with pytest.raises(InventoryError):
        read_tables(z, CONFIG, DTYPES, PRIMARY_KEYS)


def test_normalizer_still_catches_a_genuinely_unknown_table(tmp_path: Path):
    z = make_zip(
        tmp_path / "2024.zip",
        {
            "2024batting.csv": "id,hr\nruthba01,60\n",
            "2024people.csv": "id,name\nruthba01,Ruth\n",
            "2024mystery.csv": "a,b\n1,2\n",
        },
    )
    with pytest.raises(InventoryError, match="mystery"):
        read_tables(z, CONFIG, DTYPES, PRIMARY_KEYS, normalize_member_name=normalize)


def test_normalizer_still_catches_a_missing_table(tmp_path: Path):
    z = make_zip(tmp_path / "2024.zip", {"2024batting.csv": "id,hr\nruthba01,60\n"})
    with pytest.raises(InventoryError, match="people"):
        read_tables(z, CONFIG, DTYPES, PRIMARY_KEYS, normalize_member_name=normalize)


def test_date_format_defaults_to_lahmans_hyphenated_form(tmp_path: Path):
    config = SourceConfig(
        name="x", page_url="x", version_pattern=re.compile(r"v(\d+)"), download_urls=(),
        tables={"g.csv": "g"}, columns={"g": {"date": "date"}}, license="x", attribution="x",
    )  # fmt: skip
    z = make_zip(tmp_path / "z.zip", {"g.csv": "date\n2024-03-20\n"})
    tables = read_tables(z, config, {"g": {"date": pl.Date()}}, {"g": ["date"]})
    assert tables["g"]["date"].to_list() == [date(2024, 3, 20)]


def test_date_format_can_be_overridden_for_retrosheets_yyyymmdd(tmp_path: Path):
    config = SourceConfig(
        name="x", page_url="x", version_pattern=re.compile(r"v(\d+)"), download_urls=(),
        tables={"g.csv": "g"}, columns={"g": {"date": "date"}}, license="x", attribution="x",
    )  # fmt: skip
    z = make_zip(tmp_path / "z.zip", {"g.csv": "date\n20240320\n"})
    tables = read_tables(
        z, config, {"g": {"date": pl.Date()}}, {"g": ["date"]}, date_format="%Y%m%d"
    )
    assert tables["g"]["date"].to_list() == [date(2024, 3, 20)]


def test_normalizer_collision_raises(tmp_path: Path):
    # two different raw names normalizing to the same table name
    z = make_zip(
        tmp_path / "2024.zip",
        {
            "2024batting.csv": "id,hr\nruthba01,60\n",
            "2025batting.csv": "id,hr\nruthba01,61\n",
            "2024people.csv": "id,name\nruthba01,Ruth\n",
        },
    )
    with pytest.raises(InventoryError, match="duplicate"):
        read_tables(z, CONFIG, DTYPES, PRIMARY_KEYS, normalize_member_name=normalize)


def test_extra_null_markers_are_treated_as_null(tmp_path: Path):
    # "balls" is a plain column, not the primary key, so sort order doesn't reshuffle it
    config = SourceConfig(
        name="x",
        page_url="x",
        version_pattern=re.compile(r"v(\d+)"),
        download_urls=(),
        tables={"g.csv": "g"},
        columns={"g": {"id": "id", "balls": "balls"}},
        license="x",
        attribution="x",
    )
    z = make_zip(tmp_path / "z.zip", {"g.csv": "id,balls\n1,3\n2,?\n3,1\n"})
    tables = read_tables(
        z,
        config,
        {"g": {"id": pl.Utf8(), "balls": pl.Int8()}},
        {"g": ["id"]},
        null_markers=frozenset({"", "?"}),
    )
    assert tables["g"]["balls"].to_list() == [3, None, 1]


def test_default_null_markers_do_not_treat_question_mark_as_null(tmp_path: Path):
    # Lahman's behavior is unchanged: "?" is just a bad value, not a null marker, by default.
    config = SourceConfig(
        name="x", page_url="x", version_pattern=re.compile(r"v(\d+)"), download_urls=(),
        tables={"g.csv": "g"}, columns={"g": {"balls": "balls"}}, license="x", attribution="x",
    )  # fmt: skip
    z = make_zip(tmp_path / "z.zip", {"g.csv": "balls\n3\n?\n1\n"})
    with pytest.raises(CastError):
        read_tables(z, config, {"g": {"balls": pl.Int8()}}, {"g": ["balls"]})
