import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipelines.core.pipeline import StageError, run_all
from pipelines.lahman import LAHMAN, LAHMAN_SCHEMA
from tests.conftest import rewrite_zip
from tests.unit.test_publish import FakeClient

REPO = "o/fishbaseball-data"
NOW = datetime(2026, 10, 2, 13, 20, 11, tzinfo=timezone.utc)
BASE = f"https://github.com/{REPO}/releases/download"


class Resp:
    def __init__(self, body: bytes, status: int = 200):
        self._body, self.status_code = body, status
        self.headers = {}

    text = property(lambda self: self._body.decode("utf-8"))

    def json(self):
        return json.loads(self._body)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(self.status_code)


class StoreSession:
    """Serves the SABR page plus whatever the fake release store currently holds."""

    def __init__(self, page: bytes, store: FakeClient):
        self.page, self.store = page, store

    def get(self, url: str, **kwargs):
        if url == LAHMAN.page_url:
            return Resp(self.page)
        prefix = BASE + "/"
        if url.startswith(prefix):
            tag, name = url[len(prefix) :].split("/", 1)
            if (tag, name) in self.store.assets:
                return Resp(self.store.assets[(tag, name)])
        return Resp(b"", 404)


@pytest.fixture
def env(tmp_path: Path, fixtures: Path):
    store = FakeClient()
    session = StoreSession((fixtures / "sabr_page.html").read_bytes(), store)

    def go(zip_path: Path, *, dry_run=False, force=False, now=NOW, sub="b"):
        return run_all(
            LAHMAN, LAHMAN_SCHEMA, out_dir=tmp_path / sub, repo=REPO,
            client=None if dry_run else store, session=session, source_url=str(zip_path),
            force=force, dry_run=dry_run, now=now, pipeline_version="0.1.0", full_dataset=False,
        )  # fmt: skip

    return go, store


def test_dry_run_first_release_plans_without_publishing(env, small_zip):
    go, store = env
    result = go(small_zip, dry_run=True)
    assert result["status"] == "dry_run" and result["tag"] == "lahman-2026-10-02"
    assert store.calls == []


def test_dry_run_writes_stage_outputs(env, small_zip, tmp_path):
    go, _ = env
    go(small_zip, dry_run=True)
    for f in ("detect.json", "manifest.json", "NOTICE.md", "build.json", "tables/batting.parquet"):
        assert (tmp_path / "b" / f).exists(), f


def test_publish_then_rerun_reports_no_change(env, small_zip):
    go, store = env
    assert go(small_zip)["status"] == "published"
    releases_before = [c for c in store.calls if c[0] == "create_release"]
    second = go(small_zip)
    assert second["status"] == "no_change"
    assert [c for c in store.calls if c[0] == "create_release"] == releases_before


def test_bom_only_change_records_upstream_seen_without_release(env, small_zip, fixtures):
    go, store = env
    go(small_zip)
    result = go(fixtures / "lahman_bom.zip")
    assert result["status"] == "content_unchanged"
    assert len([c for c in store.calls if c[0] == "create_release"]) == 1
    pointer = json.loads(store.assets[("lahman-latest", "latest.json")])
    assert pointer["latest"] == "lahman-2026-10-02"
    assert pointer["upstream_seen"]["sha256"] != ""
    assert go(fixtures / "lahman_bom.zip")["status"] == "no_change"  # now remembered


def test_changed_content_publishes_new_tagged_release(env, small_zip, tmp_path):
    go, store = env
    go(small_zip)

    def bump(name, text):
        if name != "Batting.csv":
            return text
        return text.replace("29,8,60,164,7,6,137", "29,8,60,165,7,6,137")  # Ruth 1927 RBI

    changed = rewrite_zip(small_zip, tmp_path / "changed.zip", bump)
    result = go(changed)
    assert result["status"] == "published" and result["tag"] == "lahman-2026-10-02-2"
    pointer = json.loads(store.assets[("lahman-latest", "latest.json")])
    assert pointer["latest"] == "lahman-2026-10-02-2"


def test_failed_validation_publishes_nothing_and_names_the_stage(env, small_zip, tmp_path):
    go, store = env

    def corrupt(name, text):
        return text.replace(",60,164,", ",61,164,") if name == "Batting.csv" else text

    bad = rewrite_zip(small_zip, tmp_path / "bad.zip", corrupt)
    with pytest.raises(StageError) as e:
        go(bad)
    assert e.value.stage == "build" and "ruthba01" in str(e.value)
    assert "create_release" not in store.names()


def test_missing_table_zip_fails_in_build(env, fixtures):
    go, _ = env
    with pytest.raises(StageError) as e:
        go(fixtures / "lahman_missing_table.zip")
    assert e.value.stage == "build"


def test_html_download_fails_in_detect(env, fixtures):
    go, _ = env
    with pytest.raises(StageError) as e:
        go(fixtures / "box_login_page.html")
    assert e.value.stage == "detect"
