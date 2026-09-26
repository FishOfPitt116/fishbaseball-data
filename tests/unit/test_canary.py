import dataclasses
import json
from pathlib import Path

import pytest

from pipelines.core.canary import main, run_checks, should_alert_availability
from pipelines.lahman import LAHMAN
from tests.unit.test_alerts import FakeIssues
from tests.unit.test_detect import Resp, Session

ZIP_URL = "https://x/lahman.zip"
CONFIG = dataclasses.replace(LAHMAN, download_urls=(ZIP_URL,))


def by_name(checks):
    return {c.name: c for c in checks}


def session(fixtures: Path, zip_path: Path | None = None, page: bytes | None = None):
    routes = {
        LAHMAN.page_url: Resp(
            page if page is not None else (fixtures / "sabr_page.html").read_bytes()
        )
    }
    if zip_path is not None:
        routes[ZIP_URL] = Resp(zip_path.read_bytes(), content_type="application/zip")
    return Session(routes)


def test_all_checks_pass_on_good_upstream(fixtures, small_zip):
    checks = by_name(run_checks(CONFIG, session(fixtures, small_zip)))
    assert set(checks) == {
        "page_reachable",
        "version_parses",
        "csv_link_present",
        "download_is_zip",
        "zip_contents",
    }
    assert all(c.ok for c in checks.values()), {n: c.detail for n, c in checks.items() if not c.ok}


def test_unreachable_page_is_an_availability_failure(fixtures):
    checks = by_name(run_checks(CONFIG, Session({})))
    assert not checks["page_reachable"].ok and checks["page_reachable"].kind == "availability"


def test_changed_page_text_is_a_format_failure(fixtures, small_zip):
    checks = by_name(
        run_checks(CONFIG, session(fixtures, small_zip, page=b"<html>redesigned</html>"))
    )
    assert not checks["version_parses"].ok and checks["version_parses"].kind == "format"
    assert not checks["csv_link_present"].ok and checks["csv_link_present"].kind == "format"


def test_html_instead_of_zip_is_a_format_failure(fixtures):
    s = session(fixtures)
    s.routes[ZIP_URL] = Resp(
        (fixtures / "box_login_page.html").read_bytes(), content_type="text/html"
    )
    c = by_name(run_checks(CONFIG, s))["download_is_zip"]
    assert not c.ok and c.kind == "format"


def test_missing_download_is_an_availability_failure(fixtures):
    c = by_name(run_checks(CONFIG, session(fixtures)))["download_is_zip"]
    assert not c.ok and c.kind == "availability"


def test_header_drift_is_a_format_failure(fixtures, small_zip, tmp_path):
    from tests.conftest import rewrite_zip

    z = rewrite_zip(
        small_zip,
        tmp_path / "z.zip",
        lambda n, t: (
            t.replace("playerID,yearID,stint", "playerID,yearID,stintX", 1)
            if n == "Batting.csv"
            else t
        ),
    )
    c = by_name(run_checks(CONFIG, session(fixtures, z)))["zip_contents"]
    assert not c.ok and c.kind == "format" and "Batting.csv" in c.detail


def test_no_download_urls_skips_download_checks(fixtures):
    checks = by_name(run_checks(dataclasses.replace(LAHMAN, download_urls=()), session(fixtures)))
    assert checks["download_is_zip"].skipped and checks["zip_contents"].skipped
    assert checks["download_is_zip"].ok


def test_availability_alerts_only_after_three_consecutive_failures():
    assert should_alert_availability(["failure", "failure"]) is True
    assert should_alert_availability(["failure", "success"]) is False
    assert should_alert_availability(["failure"]) is False
    assert should_alert_availability([]) is False


def test_cli_writes_results_and_fails_on_failure(fixtures, tmp_path, capsys):
    rc = main(["lahman", "--out", str(tmp_path)], session=Session({}))
    assert rc == 1
    results = json.loads((tmp_path / "canary.json").read_text())
    assert any(r["name"] == "page_reachable" and not r["ok"] for r in results)


@pytest.mark.parametrize(
    "previous,expect_issue", [("failure,failure", True), ("success,failure", False)]
)
def test_cli_alert_availability_threshold(previous, expect_issue, tmp_path, monkeypatch):
    fake = FakeIssues()
    monkeypatch.setattr("pipelines.core.canary.GhIssueClient", lambda repo: fake)
    monkeypatch.setenv("GITHUB_REPOSITORY", "o/r")
    (tmp_path / "canary.json").write_text(
        json.dumps(
            [
                {
                    "name": "page_reachable",
                    "kind": "availability",
                    "ok": False,
                    "detail": "down",
                    "skipped": False,
                }
            ]
        )
    )
    assert (
        main(
            ["lahman", "--out", str(tmp_path), "--alert", "--run-url", "u", "--previous", previous]
        )
        == 0
    )
    assert bool(fake.issues) is expect_issue


def test_cli_alerts_format_failure_immediately(tmp_path, monkeypatch):
    fake = FakeIssues()
    monkeypatch.setattr("pipelines.core.canary.GhIssueClient", lambda repo: fake)
    monkeypatch.setenv("GITHUB_REPOSITORY", "o/r")
    (tmp_path / "canary.json").write_text(
        json.dumps(
            [
                {
                    "name": "version_parses",
                    "kind": "format",
                    "ok": False,
                    "detail": "no match",
                    "skipped": False,
                }
            ]
        )
    )
    main(["lahman", "--out", str(tmp_path), "--alert", "--run-url", "u", "--previous", ""])
    (issue,) = fake.issues.values()
    assert issue["labels"] == ["pipeline:lahman", "stage:canary-format"]


def test_cli_resolve_closes_canary_issues(tmp_path, monkeypatch):
    fake = FakeIssues()
    fake.create("t", "b", ["pipeline:lahman", "stage:canary-format"])
    fake.create("t", "b", ["pipeline:lahman", "stage:build"])
    monkeypatch.setattr("pipelines.core.canary.GhIssueClient", lambda repo: fake)
    monkeypatch.setenv("GITHUB_REPOSITORY", "o/r")
    main(["lahman", "--out", str(tmp_path), "--resolve", "--run-url", "u"])
    assert [i["open"] for i in fake.issues.values()] == [False, True]
