from pathlib import Path

import pytest

from pipelines.core.download import DownloadError, download, ensure_zip


class FakeResponse:
    def __init__(self, body: bytes, status: int = 200, content_type: str = "application/zip"):
        self._body = body
        self.status_code = status
        self.headers = {"Content-Type": content_type, "Content-Length": str(len(body))}

    def iter_content(self, chunk_size: int = 8192):
        for i in range(0, len(self._body), chunk_size):
            yield self._body[i : i + chunk_size]

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def close(self):
        pass


class FakeSession:
    def __init__(self, responses: dict[str, FakeResponse]):
        self.responses = responses
        self.calls: list[tuple[str, dict]] = []

    def get(self, url: str, **kwargs):
        self.calls.append((url, kwargs))
        return self.responses[url]


def test_zip_accepted(tmp_path: Path, small_zip: Path):
    session = FakeSession({"https://x/a.zip": FakeResponse(small_zip.read_bytes())})
    out = download(("https://x/a.zip",), tmp_path / "l.zip", session=session, user_agent="ua")
    assert out.read_bytes() == small_zip.read_bytes()
    assert session.calls[0][1]["headers"]["User-Agent"] == "ua"


def test_html_page_rejected(tmp_path: Path, fixtures: Path):
    html = (fixtures / "box_login_page.html").read_bytes()
    session = FakeSession({"https://x/a.zip": FakeResponse(html, content_type="text/html")})
    with pytest.raises(DownloadError, match="got HTML"):
        download(("https://x/a.zip",), tmp_path / "l.zip", session=session)
    assert not (tmp_path / "l.zip").exists()


def test_oversize_rejected(tmp_path: Path, small_zip: Path):
    session = FakeSession({"https://x/a.zip": FakeResponse(small_zip.read_bytes())})
    with pytest.raises(DownloadError, match="size cap"):
        download(("https://x/a.zip",), tmp_path / "l.zip", session=session, max_bytes=100)
    assert not (tmp_path / "l.zip").exists()


def test_falls_through_to_next_url(tmp_path: Path, small_zip: Path, fixtures: Path):
    session = FakeSession(
        {
            "https://x/bad": FakeResponse(b"nope", status=404, content_type="text/html"),
            "https://x/good": FakeResponse(small_zip.read_bytes()),
        }
    )
    out = download(("https://x/bad", "https://x/good"), tmp_path / "l.zip", session=session)
    assert out.exists()
    assert [c[0] for c in session.calls] == ["https://x/bad", "https://x/good"]


def test_all_urls_failing_raises_with_every_reason(tmp_path: Path):
    session = FakeSession({"https://x/a": FakeResponse(b"", status=500)})
    with pytest.raises(DownloadError, match="https://x/a"):
        download(("https://x/a",), tmp_path / "l.zip", session=session)


def test_source_url_overrides_configured_urls(tmp_path: Path, small_zip: Path):
    session = FakeSession({"https://override/z.zip": FakeResponse(small_zip.read_bytes())})
    download(
        ("https://configured/z.zip",),
        tmp_path / "l.zip",
        source_url="https://override/z.zip",
        session=session,
    )
    assert [c[0] for c in session.calls] == ["https://override/z.zip"]


def test_source_url_may_be_a_local_file(tmp_path: Path, small_zip: Path):
    out = download((), tmp_path / "l.zip", source_url=str(small_zip))
    assert out.read_bytes() == small_zip.read_bytes()


def test_local_html_file_rejected(tmp_path: Path, fixtures: Path):
    with pytest.raises(DownloadError, match="got HTML"):
        download((), tmp_path / "l.zip", source_url=str(fixtures / "box_login_page.html"))


def test_ensure_zip_checks_magic_bytes(tmp_path: Path):
    p = tmp_path / "x.zip"
    p.write_bytes(b"PK\x03\x04garbage")
    with pytest.raises(DownloadError, match="not a valid zip"):
        ensure_zip(p)
