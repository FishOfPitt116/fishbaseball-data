import zipfile
from pathlib import Path

import pytest

from pipelines.core.box import BoxFile, download_folder, is_box_share, list_folder, parse_items
from pipelines.core.download import DownloadError, download
from pipelines.lahman import LAHMAN

SHARE = "https://sabr.box.com/s/y1prhc795jk8zvmelfd3jq7tl389y6cd"
NAME = "y1prhc795jk8zvmelfd3jq7tl389y6cd"


class Resp:
    def __init__(self, body: bytes, status: int = 200, content_type: str = "text/html"):
        self._body, self.status_code = body, status
        self.headers = {"Content-Type": content_type}

    @property
    def text(self) -> str:
        return self._body.decode("utf-8")

    def iter_content(self, chunk_size: int = 8192):
        yield self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def close(self):
        pass


class BoxSession:
    """Serves paginated folder pages and file downloads (contents from a zip)."""

    def __init__(self, fixtures: Path, files: dict[str, bytes], pages: int = 3):
        self.pages = {
            n: (fixtures / f"box_folder_page{n}.html").read_bytes() for n in range(1, pages + 1)
        }
        ids = {f.file_id: f.name for n in (1, 2) for f in parse_items(self.pages[n].decode())}
        self.bodies = {fid: files[name] for fid, name in ids.items() if name in files}
        self.calls: list[str] = []

    def get(self, url: str, **kwargs):
        self.calls.append(url)
        if url.startswith(SHARE):
            page = int(url.split("page=")[1]) if "page=" in url else 1
            return Resp(self.pages.get(page, self.pages[3]))
        if "box_download_shared_file" in url and f"shared_name={NAME}" in url:
            fid = url.split("file_id=")[1]
            if fid in self.bodies:
                return Resp(self.bodies[fid], content_type="text/csv")
        return Resp(b"", 404)


@pytest.fixture
def csvs(small_zip: Path) -> dict[str, bytes]:
    with zipfile.ZipFile(small_zip) as z:
        return {n: z.read(n) for n in z.namelist()} | {"readme2025.txt": b"hello"}


def test_is_box_share():
    assert is_box_share(SHARE) and is_box_share("https://sabr.app.box.com/s/abc123/")
    assert not is_box_share("https://example.com/s/abc") and not is_box_share("/tmp/x.zip")


def test_parse_items_reads_file_names_and_ids(fixtures: Path):
    items = parse_items((fixtures / "box_folder_page1.html").read_text())
    assert len(items) == 20
    batting = next(i for i in items if i.name == "Batting.csv")
    assert batting == BoxFile("Batting.csv", batting.file_id) and batting.file_id.startswith("f_")


def test_parse_items_of_an_empty_page(fixtures: Path):
    assert parse_items((fixtures / "box_folder_page3.html").read_text()) == []


def test_list_folder_follows_pages_until_a_page_adds_nothing(fixtures, csvs):
    s = BoxSession(fixtures, csvs)
    files = list_folder(SHARE, s)
    assert len([f for f in files if f.name.endswith(".csv")]) == 27
    assert [u.split("page=")[1] for u in s.calls] == ["1", "2", "3"]


def test_list_folder_keeps_paginating_if_upstream_adds_pages(fixtures, csvs):
    s = BoxSession(fixtures, csvs)
    s.pages[3] = s.pages[2].replace(b"f_2084", b"f_9084").replace(b"Appearances", b"Zzz")
    s.pages[4] = s.pages[1][:0] + (fixtures / "box_folder_page3.html").read_bytes()
    files = list_folder(SHARE, s)
    assert any(f.file_id.startswith("f_9084") for f in files)
    assert s.calls[-1].endswith("page=4")


def test_download_folder_builds_zip_of_csvs(tmp_path, fixtures, csvs):
    out = download_folder(SHARE, tmp_path / "l.zip", BoxSession(fixtures, csvs))
    with zipfile.ZipFile(out) as z:
        assert sorted(z.namelist()) == sorted(LAHMAN.tables)  # 27 CSVs, readme left out
        assert z.read("Batting.csv") == csvs["Batting.csv"]


def test_zip_is_deterministic(tmp_path, fixtures, csvs):
    a = download_folder(SHARE, tmp_path / "a.zip", BoxSession(fixtures, csvs))
    b = download_folder(SHARE, tmp_path / "b.zip", BoxSession(fixtures, csvs))
    assert a.read_bytes() == b.read_bytes()


def test_html_file_download_fails_loudly(tmp_path, fixtures, csvs):
    s = BoxSession(fixtures, csvs)
    login = (fixtures / "box_login_page.html").read_bytes()
    bad = next(iter(s.bodies))
    s.bodies[bad] = login
    orig = s.get

    def get(url, **kw):
        r = orig(url, **kw)
        if url.endswith(bad):
            r.headers["Content-Type"] = "text/html"
        return r

    s.get = get
    with pytest.raises(DownloadError, match="got HTML"):
        download_folder(SHARE, tmp_path / "l.zip", s)


def test_empty_folder_fails(tmp_path, fixtures, csvs):
    s = BoxSession(fixtures, csvs)
    s.pages[1] = s.pages[3]
    with pytest.raises(DownloadError, match="no files"):
        download_folder(SHARE, tmp_path / "l.zip", s)


def test_size_cap_applies_across_files(tmp_path, fixtures, csvs):
    with pytest.raises(DownloadError, match="size cap"):
        download_folder(SHARE, tmp_path / "l.zip", BoxSession(fixtures, csvs), max_bytes=1000)


def test_download_dispatches_box_share_urls(tmp_path, fixtures, csvs):
    out = download((SHARE,), tmp_path / "l.zip", session=BoxSession(fixtures, csvs))
    with zipfile.ZipFile(out) as z:
        assert len(z.namelist()) == 27
