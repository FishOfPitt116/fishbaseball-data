"""Hits the real SABR site. Run with: pytest tests/integration"""

import pytest
import requests

from pipelines.core.canary import run_checks
from pipelines.core.detect import USER_AGENT, parse_page
from pipelines.lahman import LAHMAN

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def session():
    s = requests.Session()
    try:
        s.get(LAHMAN.page_url, timeout=30, headers={"User-Agent": USER_AGENT}).raise_for_status()
    except requests.RequestException as e:
        pytest.skip(f"SABR unreachable: {e}")
    return s


def test_live_page_version_parses(session):
    page = session.get(LAHMAN.page_url, timeout=30, headers={"User-Agent": USER_AGENT})
    info = parse_page(page.text, LAHMAN.version_pattern)
    assert int(info.version) >= 2025 and info.released >= "2026-01-02"


def test_live_canary_page_checks_pass(session):
    checks = {c.name: c for c in run_checks(LAHMAN, session)}
    assert checks["page_reachable"].ok and checks["version_parses"].ok
    assert checks["csv_link_present"].ok


def test_live_full_dry_run_validates_real_data(session, tmp_path):
    from datetime import datetime, timezone

    from pipelines.core.pipeline import run_all
    from pipelines.lahman import LAHMAN_SCHEMA

    result = run_all(
        LAHMAN, LAHMAN_SCHEMA, out_dir=tmp_path, repo="o/r", client=None, session=session,
        dry_run=True, now=datetime.now(timezone.utc), pipeline_version="test",
    )  # fmt: skip
    assert result["status"] == "dry_run" and len(result["assets"]) == 27 + 3
