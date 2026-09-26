import dataclasses
import hashlib
import json
from pathlib import Path

import pytest

from pipelines.core.detect import UpstreamFormatError, detect, parse_page
from pipelines.lahman import LAHMAN

LATEST_URL = "https://gh/latest.json"
PAGE_URL = LAHMAN.page_url
ZIP_URL = "https://x/lahman.zip"


class Resp:
    def __init__(self, body: bytes, status: int = 200, content_type: str = "text/html"):
        self._body, self.status_code = body, status
        self.headers = {"Content-Type": content_type}

    @property
    def text(self) -> str:
        return self._body.decode("utf-8")

    def json(self):
        return json.loads(self._body)

    def iter_content(self, chunk_size: int = 8192):
        yield self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def close(self):
        pass


class Session:
    def __init__(self, routes: dict[str, Resp]):
        self.routes = routes

    def get(self, url: str, **kwargs):
        return self.routes.get(url, Resp(b"", 404))


def make_session(fixtures: Path, zip_path: Path, latest: dict | None) -> Session:
    routes = {
        PAGE_URL: Resp((fixtures / "sabr_page.html").read_bytes()),
        ZIP_URL: Resp(zip_path.read_bytes(), content_type="application/zip"),
    }
    if latest is not None:
        routes[LATEST_URL] = Resp(json.dumps(latest).encode(), content_type="application/json")
    return Session(routes)


CONFIG = dataclasses.replace(LAHMAN, download_urls=(ZIP_URL,))


def sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def run(tmp_path, session, **kw):
    return detect(CONFIG, out_dir=tmp_path, session=session, latest_url=LATEST_URL, **kw)


def test_parse_page_extracts_version_date_and_note(fixtures: Path):
    info = parse_page((fixtures / "sabr_page.html").read_text(), LAHMAN.version_pattern)
    assert info.version == "2025"
    assert info.released == "2026-01-02"
    assert info.update_note is not None and "February 18, 2026" in info.update_note


def test_missing_pattern_raises():
    with pytest.raises(UpstreamFormatError):
        parse_page("<html>nothing here</html>", LAHMAN.version_pattern)


def test_no_previous_release_is_changed(tmp_path, fixtures, small_zip):
    result = run(tmp_path, make_session(fixtures, small_zip, None))
    assert result["changed"] is True and result["reason"] == "new_version"
    assert result["sabr_version"] == "2025" and result["released"] == "2026-01-02"
    assert result["upstream_sha256"] == sha(small_zip)
    assert (tmp_path / "detect.json").exists()
    assert Path(result["zip_path"]).exists()


def test_unchanged_sha_is_not_changed(tmp_path, fixtures, small_zip):
    latest = {"upstream_seen": {"version": "2025", "sha256": sha(small_zip)}}
    result = run(tmp_path, make_session(fixtures, small_zip, latest))
    assert result["changed"] is False and result["reason"] == "none"


def test_same_version_new_sha_is_files_changed(tmp_path, fixtures, small_zip):
    latest = {"upstream_seen": {"version": "2025", "sha256": "0" * 64}}
    result = run(tmp_path, make_session(fixtures, small_zip, latest))
    assert result["changed"] is True and result["reason"] == "files_changed"


def test_new_version_reason(tmp_path, fixtures, small_zip):
    latest = {"upstream_seen": {"version": "2024", "sha256": "0" * 64}}
    result = run(tmp_path, make_session(fixtures, small_zip, latest))
    assert result["changed"] is True and result["reason"] == "new_version"


def test_force_overrides_no_change(tmp_path, fixtures, small_zip):
    latest = {"upstream_seen": {"version": "2025", "sha256": sha(small_zip)}}
    result = run(tmp_path, make_session(fixtures, small_zip, latest), force=True)
    assert result["changed"] is True and result["reason"] == "forced"


def test_detect_json_matches_returned_result(tmp_path, fixtures, small_zip):
    result = run(tmp_path, make_session(fixtures, small_zip, None))
    assert json.loads((tmp_path / "detect.json").read_text()) == result


def test_source_url_local_zip_is_used(tmp_path, fixtures, small_zip):
    session = make_session(fixtures, small_zip, None)
    result = run(tmp_path, session, source_url=str(small_zip))
    assert result["upstream_sha256"] == sha(small_zip)
