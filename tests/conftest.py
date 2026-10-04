from __future__ import annotations

import pathlib

import polars as pl
import pytest

FIXTURES = pathlib.Path(__file__).parent / "fixtures" / "lahman"
RETROSHEET_FIXTURES = pathlib.Path(__file__).parent / "fixtures" / "retrosheet"
RETROSHEET_SEASONS = (1927, 1956, 2001)


@pytest.fixture
def fixtures() -> pathlib.Path:
    return FIXTURES


@pytest.fixture
def small_zip(fixtures: pathlib.Path) -> pathlib.Path:
    return fixtures / "lahman_small.zip"


@pytest.fixture(scope="session")
def retrosheet_fixtures() -> pathlib.Path:
    return RETROSHEET_FIXTURES


@pytest.fixture(scope="session")
def retrosheet_season_zips(retrosheet_fixtures: pathlib.Path) -> dict[int, pathlib.Path]:
    return {year: retrosheet_fixtures / f"{year}csvs.zip" for year in RETROSHEET_SEASONS}


def read_retrosheet_season(zip_path: pathlib.Path, year: int) -> dict[str, pl.DataFrame]:
    """Convert one fixture season zip exactly as the real pipeline will."""
    from pipelines.retrosheet.convert import read_season

    return read_season(zip_path, str(year))


def read_retrosheet_seasons(
    zips: dict[int, pathlib.Path], years: tuple[int, ...] | None = None
) -> dict[str, pl.DataFrame]:
    """Every fixture season, converted and concatenated per table."""
    years = years or tuple(zips)
    per_table: dict[str, list[pl.DataFrame]] = {}
    for year in years:
        for name, df in read_retrosheet_season(zips[year], year).items():
            per_table.setdefault(name, []).append(df)
    return {name: pl.concat(dfs) for name, dfs in per_table.items()}


def rewrite_zip(src: pathlib.Path, dst: pathlib.Path, fn) -> pathlib.Path:
    """Copy a zip, passing each member's text through fn(name, text) -> text | None (drop)."""
    import zipfile

    with zipfile.ZipFile(src) as zin, zipfile.ZipFile(dst, "w", zipfile.ZIP_DEFLATED) as zout:
        for name in zin.namelist():
            out = fn(name, zin.read(name).decode("utf-8"))
            if out is not None:
                zout.writestr(name, out)
    return dst
