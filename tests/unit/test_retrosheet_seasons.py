from pathlib import Path

import pytest

from pipelines.retrosheet.seasons import SeasonDiscoveryError, discover_seasons, season_zip_url

FIXTURE = Path(__file__).parent.parent / "fixtures" / "retrosheet" / "csvdownloads.html"


class FakeSession:
    def __init__(self, html: str, status: int = 200):
        self.html, self.status_code = html, status
        self.calls: list[tuple[str, dict]] = []

    def get(self, url, *, timeout=None, headers=None):
        self.calls.append((url, dict(headers or {})))
        return self

    @property
    def text(self):
        return self.html

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


def test_discovers_the_full_real_range():
    seasons = discover_seasons(FakeSession(FIXTURE.read_text()))
    assert seasons[0] == "1897" and seasons[-1] == "2025"
    assert len(seasons) == 129
    assert seasons == sorted(seasons)


def test_seasons_are_contiguous():
    seasons = discover_seasons(FakeSession(FIXTURE.read_text()))
    assert [int(y) for y in seasons] == list(range(1897, 2026))


def test_no_season_links_raises():
    with pytest.raises(SeasonDiscoveryError):
        discover_seasons(FakeSession("<html><body>nothing here</body></html>"))


def test_http_error_propagates():
    with pytest.raises(RuntimeError):
        discover_seasons(FakeSession(FIXTURE.read_text(), status=500))


def test_sends_user_agent():
    session = FakeSession(FIXTURE.read_text())
    discover_seasons(session)
    assert (
        "User-Agent" in session.calls[0][1]
        and "fishbaseball-data" in session.calls[0][1]["User-Agent"]
    )


def test_season_zip_url_matches_the_real_pattern():
    assert season_zip_url("2024") == "https://www.retrosheet.org/downloads/2024/2024csvs.zip"
    assert season_zip_url("1897") == "https://www.retrosheet.org/downloads/1897/1897csvs.zip"
