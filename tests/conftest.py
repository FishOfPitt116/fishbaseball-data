import pathlib

import pytest

FIXTURES = pathlib.Path(__file__).parent / "fixtures" / "lahman"


@pytest.fixture
def fixtures() -> pathlib.Path:
    return FIXTURES


@pytest.fixture
def small_zip(fixtures: pathlib.Path) -> pathlib.Path:
    return fixtures / "lahman_small.zip"


def rewrite_zip(src: pathlib.Path, dst: pathlib.Path, fn) -> pathlib.Path:
    """Copy a zip, passing each member's text through fn(name, text) -> text | None (drop)."""
    import zipfile

    with zipfile.ZipFile(src) as zin, zipfile.ZipFile(dst, "w", zipfile.ZIP_DEFLATED) as zout:
        for name in zin.namelist():
            out = fn(name, zin.read(name).decode("utf-8"))
            if out is not None:
                zout.writestr(name, out)
    return dst
