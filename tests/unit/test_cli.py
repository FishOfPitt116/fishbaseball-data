import json
from pathlib import Path

import pytest

from pipelines.__main__ import main
from tests.unit.test_pipeline import StoreSession
from tests.unit.test_publish import FakeClient


@pytest.fixture
def session(fixtures: Path):
    return StoreSession((fixtures / "sabr_page.html").read_bytes(), FakeClient())


def cli(args, session, tmp_path, **kw):
    return main(
        ["lahman", *args, "--out", str(tmp_path / "out")],
        session=session,
        client=FakeClient(),  # never the real gh
        full_dataset=False,
        **kw,
    )


def test_all_dry_run_succeeds(session, small_zip, tmp_path, capsys):
    assert cli(["all", "--dry-run", "--source-url", str(small_zip)], session, tmp_path) == 0
    assert "dry_run" in capsys.readouterr().out
    assert (tmp_path / "out" / "manifest.json").exists()


def test_stages_run_separately(session, small_zip, tmp_path):
    assert cli(["detect", "--source-url", str(small_zip)], session, tmp_path) == 0
    assert (tmp_path / "out" / "detect.json").exists()
    assert not (tmp_path / "out" / "manifest.json").exists()
    assert cli(["build"], session, tmp_path) == 0
    assert (tmp_path / "out" / "manifest.json").exists()
    assert cli(["publish", "--dry-run"], session, tmp_path) == 0


def test_build_without_detect_fails_clearly(session, tmp_path, capsys):
    assert cli(["build"], session, tmp_path) == 1
    assert "detect" in capsys.readouterr().err


def test_failure_writes_error_json_and_exits_nonzero(session, fixtures, tmp_path):
    rc = cli(["all", "--dry-run", "--source-url", str(fixtures / "lahman_missing_table.zip")],
             session, tmp_path)  # fmt: skip
    assert rc == 1
    err = json.loads((tmp_path / "out" / "error.json").read_text())
    assert err["stage"] == "build" and "Salaries.csv" in err["error"]


def test_success_clears_stale_error_json(session, small_zip, tmp_path):
    out = tmp_path / "out"
    out.mkdir()
    (out / "error.json").write_text("{}")
    assert cli(["all", "--dry-run", "--source-url", str(small_zip)], session, tmp_path) == 0
    assert not (out / "error.json").exists()


def test_unknown_source_is_rejected(session, tmp_path):
    with pytest.raises(SystemExit):
        main(["nope", "all"], session=session)
