"""`read_tables`'s optional member-name normalizer, for sources whose per-partition zips
prefix every CSV with a key (Retrosheet: the season year, e.g. "2024batting.csv")."""

import re
import zipfile
from pathlib import Path

import polars as pl
import pytest

from pipelines.core.config import SourceConfig
from pipelines.core.convert import InventoryError, read_tables

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
