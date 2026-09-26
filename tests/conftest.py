import pathlib

import pytest

FIXTURES = pathlib.Path(__file__).parent / "fixtures" / "lahman"


@pytest.fixture
def fixtures() -> pathlib.Path:
    return FIXTURES


@pytest.fixture
def small_zip(fixtures: pathlib.Path) -> pathlib.Path:
    return fixtures / "lahman_small.zip"
