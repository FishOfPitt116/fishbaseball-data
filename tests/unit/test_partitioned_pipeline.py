"""End-to-end orchestration for a partitioned source, against the real 1927/1956/2001
fixture season zips: `run_all` wired through `pipelines.core.pipeline`'s dispatch into
`pipelines.core.partitioned_pipeline` (since `RETROSHEET.partitions is not None`)."""

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipelines.core.partitioned_pipeline import stage_detect
from pipelines.core.pipeline import run_all
from pipelines.retrosheet import RETROSHEET, RETROSHEET_SCHEMA
from pipelines.retrosheet.seasons import DOWNLOADS_PAGE, season_zip_url
from tests.unit.test_publish import FakeClient

REPO = "FishOfPitt116/fishbaseball-data"
NOW = datetime(2026, 10, 5, 12, 0, 0, tzinfo=timezone.utc)
BASE = f"https://github.com/{REPO}/releases/download"


class Resp:
    def __init__(self, body: bytes, status: int = 200, headers: dict | None = None):
        self._body, self.status_code, self.headers = body, status, headers or {}
        self.content = body

    text = property(lambda self: self._body.decode("utf-8"))

    def json(self):
        return json.loads(self._body)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(self.status_code)

    def iter_content(self, chunk_size: int = 1 << 16):
        yield self._body

    def close(self):
        pass


def _page_for(seasons: list[str]) -> str:
    links = "".join(f'<a href="downloads/{s}/{s}csvs.zip">{s}</a>' for s in seasons)
    return f"<html><body>{links}</body></html>"


class RetrosheetSession:
    """Serves the downloads page, each season's zip (by ETag), and whatever the fake
    release store currently holds — everything `check_all_partitions`/`download_partition`/
    `publish`/`_fetch_parquet` need, with no real network access."""

    def __init__(self, zips: dict[str, Path], store: FakeClient, etags: dict[str, str]):
        self.zips, self.store, self.etags = zips, store, etags

    def get(self, url: str, headers: dict | None = None, **kwargs):
        if url == DOWNLOADS_PAGE:
            return Resp(_page_for(sorted(self.zips)).encode())
        for season, path in self.zips.items():
            if url == season_zip_url(season):
                return Resp(path.read_bytes())
        prefix = BASE + "/"
        if url.startswith(prefix):
            tag, name = url[len(prefix) :].split("/", 1)
            if (tag, name) in self.store.assets:
                return Resp(self.store.assets[(tag, name)])
        return Resp(b"", 404)

    def head(self, url: str, headers: dict | None = None, **kwargs):
        for season in self.zips:
            if url == season_zip_url(season):
                etag = self.etags[season]
                if (headers or {}).get("If-None-Match") == etag:
                    return Resp(b"", 304)
                return Resp(b"", 200, {"ETag": etag})
        return Resp(b"", 404)


@pytest.fixture
def env(tmp_path: Path, retrosheet_season_zips: dict[int, Path], monkeypatch: pytest.MonkeyPatch):
    # These tests check business logic (what got built/published), not request pacing — that
    # has its own dedicated test below — so skip the real delay between requests.
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)
    store = FakeClient()
    zips = {str(year): path for year, path in retrosheet_season_zips.items()}
    etags = {season: f'"{season}-v1"' for season in zips}
    session = RetrosheetSession(zips, store, etags)

    def go(*, dry_run: bool = False, force: bool = False, now: datetime = NOW):
        return run_all(
            RETROSHEET, RETROSHEET_SCHEMA, out_dir=tmp_path / "b", repo=REPO,
            client=None if dry_run else store, session=session, force=force,
            dry_run=dry_run, now=now, pipeline_version="0.1.0",
        )  # fmt: skip

    return go, store, etags


def test_first_release_converts_every_discovered_season(env):
    go, store, _ = env
    result = go()
    assert result["status"] == "published"
    uploaded = {name for (_, name) in store.assets}
    assert {"game_info.parquet", "all_players.parquet"} <= uploaded
    for season in ("1927", "1956", "2001"):
        assert f"plays_{season}.parquet" in uploaded


def test_rerun_with_unchanged_etags_reports_no_change(env):
    go, store, _ = env
    go()
    releases_before = [c for c in store.calls if c[0] == "create_release"]
    assert go()["status"] == "no_change"
    assert [c for c in store.calls if c[0] == "create_release"] == releases_before


def test_changed_season_republishes_only_that_partition_and_keeps_full_history(env):
    go, store, etags = env
    first = go()
    first_manifest = json.loads(store.assets[(first["tag"], "manifest.json")])

    etags["2001"] = '"2001-v2"'  # the only season whose upstream ETag changed
    second = go()
    assert second["status"] == "published" and second["tag"] != first["tag"]
    second_manifest = json.loads(store.assets[(second["tag"], "manifest.json")])

    uploaded = {name for (tag, name) in store.assets if tag == second["tag"]}
    assert "plays_2001.parquet" in uploaded
    assert "plays_1927.parquet" not in uploaded  # carried forward, never re-uploaded
    assert "game_info.parquet" in uploaded  # single-file table: always re-uploaded whole

    carried = second_manifest["tables"]["plays"]["partitions"]["1927"]
    assert carried["url"] == first_manifest["tables"]["plays"]["partitions"]["1927"]["url"]
    # game_info is rebuilt whole every release, from carried-forward + fresh rows, so its
    # total row count is unaffected by only one season having changed.
    assert (
        second_manifest["tables"]["game_info"]["rows"]
        == first_manifest["tables"]["game_info"]["rows"]
    )


def test_dry_run_does_not_call_the_client(env):
    go, store, _ = env
    assert go(dry_run=True)["status"] == "dry_run"
    assert store.calls == []


def test_force_publishes_even_with_unchanged_etags(env):
    go, store, _ = env
    first = go()
    second = go(force=True)
    assert second["status"] == "published" and second["tag"] != first["tag"]


def test_pipeline_dispatch_forwards_delay_to_partitioned_stage_detect(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    import pipelines.core.pipeline as core_pipeline

    captured: dict = {}

    def fake_partitioned_detect(config, **kwargs):
        captured.update(kwargs)
        return {
            "changed": False, "reason": "content_unchanged", "changed_seasons": [],
            "zip_paths": {}, "upstream_seen": {},
        }  # fmt: skip

    monkeypatch.setattr("pipelines.core.partitioned_pipeline.stage_detect", fake_partitioned_detect)
    core_pipeline.stage_detect(
        RETROSHEET, out_dir=tmp_path, repo=REPO, session=object(), source_url=None,
        force=False, delay=1.5,
    )  # fmt: skip
    assert captured["delay"] == 1.5


def test_pipeline_dispatch_uses_the_partitioned_default_when_not_given(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    import pipelines.core.pipeline as core_pipeline
    from pipelines.core.partitioned_pipeline import DEFAULT_PARTITION_DELAY

    captured: dict = {}

    def fake_partitioned_detect(config, **kwargs):
        captured.update(kwargs)
        return {
            "changed": False, "reason": "content_unchanged", "changed_seasons": [],
            "zip_paths": {}, "upstream_seen": {},
        }  # fmt: skip

    monkeypatch.setattr("pipelines.core.partitioned_pipeline.stage_detect", fake_partitioned_detect)
    core_pipeline.stage_detect(
        RETROSHEET, out_dir=tmp_path, repo=REPO, session=object(), source_url=None, force=False
    )
    assert captured["delay"] == DEFAULT_PARTITION_DELAY


def test_stage_detect_paces_both_checks_and_downloads(
    tmp_path: Path, retrosheet_season_zips: dict[int, Path], monkeypatch: pytest.MonkeyPatch
):
    store = FakeClient()
    zips = {str(year): path for year, path in retrosheet_season_zips.items()}
    etags = {season: f'"{season}-v1"' for season in zips}
    session = RetrosheetSession(zips, store, etags)
    sleeps: list[float] = []
    monkeypatch.setattr(time, "sleep", sleeps.append)

    result = stage_detect(RETROSHEET, out_dir=tmp_path / "b", repo=REPO, session=session, delay=0.3)

    assert result["changed_seasons"] == ["1927", "1956", "2001"]
    # 2 pauses checking 3 seasons' ETags + 2 pauses downloading all 3 (first release: none
    # carried forward) — never a pause before the very first request of either pass.
    assert sleeps == [0.3] * 4
