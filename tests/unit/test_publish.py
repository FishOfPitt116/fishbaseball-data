import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipelines.core.convert import read_tables, write_parquet
from pipelines.core.manifest import build_manifest
from pipelines.core.publish import (
    GhClient,
    PublishError,
    make_latest,
    make_notes,
    make_notice,
    mark_withdrawn,
    publish,
    record_upstream_seen,
)
from pipelines.lahman import LAHMAN
from pipelines.lahman.schema import DTYPES, PRIMARY_KEYS, SCHEMA_VERSION, YEAR_COLUMNS

REPO = "FishOfPitt116/fishbaseball-data"
TAG = "lahman-2026-10-02"
NOW = datetime(2026, 10, 2, 13, 20, 11, tzinfo=timezone.utc)
POINTER_TAG = "lahman-latest"


class FakeClient:
    def __init__(self, tags=(), corrupt: str | None = None):
        self.calls: list[tuple] = []
        self.tags = list(tags)
        self.corrupt = corrupt
        self.assets: dict[tuple[str, str], bytes] = {}
        self.titles: dict[str, str] = {}

    def existing_tags(self):
        self.calls.append(("existing_tags",))
        return self.tags

    def create_release(self, tag, title, notes, assets, *, latest=False):
        self.calls.append(("create_release", tag, latest))
        self.titles[tag] = title
        self.tags.append(tag)
        for a in assets:
            self.assets[(tag, a.name)] = a.read_bytes()

    def ensure_release(self, tag, title, notes):
        self.calls.append(("ensure_release", tag))
        self.titles.setdefault(tag, title)

    def upload_asset(self, tag, path, *, clobber=True):
        self.calls.append(("upload_asset", tag, path.name, clobber))
        self.assets[(tag, path.name)] = path.read_bytes()

    def fetch(self, tag, filename):
        self.calls.append(("fetch", tag, filename))
        data = self.assets[(tag, filename)]
        return b"corrupt" if filename == self.corrupt else data

    def names(self):
        return [c[0] for c in self.calls]


@pytest.fixture
def ctx(tmp_path: Path, small_zip: Path):
    tables = read_tables(small_zip, LAHMAN, DTYPES, PRIMARY_KEYS)
    out = tmp_path / "build"
    for name, df in tables.items():
        write_parquet(df, out / "tables" / f"{name}.parquet")
    upstream = {
        "version": "2025",
        "released": "2026-01-02",
        "page_url": LAHMAN.page_url,
        "sha256": hashlib.sha256(small_zip.read_bytes()).hexdigest(),
        "file": "lahman_2025_csv.zip",
    }
    manifest = build_manifest(
        LAHMAN,
        tag=TAG,
        repo=REPO,
        upstream=upstream,
        tables=tables,
        parquet_dir=out / "tables",
        primary_keys=PRIMARY_KEYS,
        year_columns=YEAR_COLUMNS,
        dtypes=DTYPES,
        schema_version=SCHEMA_VERSION,
        pipeline_version="0.1.0",
        built_at=NOW,
    )
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2))
    (out / "NOTICE.md").write_text(make_notice(LAHMAN, manifest))
    return {"manifest": manifest, "out": out, "zip": small_zip}


def run(ctx, client, *, dry_run=False, previous_latest=None):
    return publish(
        client,
        config=LAHMAN,
        manifest=ctx["manifest"],
        build_dir=ctx["out"],
        zip_path=ctx["zip"],
        previous_latest=previous_latest,
        dry_run=dry_run,
        now=NOW,
    )


def test_dry_run_makes_no_api_calls(ctx):
    client = FakeClient()
    plan = run(ctx, client, dry_run=True)
    assert client.calls == []
    assert plan["tag"] == TAG and plan["published"] is False
    assert "manifest.json" in plan["assets"] and "batting.parquet" in plan["assets"]
    assert "lahman_2025_csv.zip" in plan["assets"] and "NOTICE.md" in plan["assets"]


def test_dry_run_also_works_without_a_client(ctx):
    assert run(ctx, None, dry_run=True)["published"] is False


def test_publishes_assets_then_pointer_last(ctx):
    client = FakeClient()
    result = run(ctx, client)
    assert result["published"] is True
    names = client.names()
    assert names.index("create_release") < names.index("fetch") < names.index("upload_asset")
    assert ("create_release", TAG, False) in client.calls  # never make_latest
    uploaded = {a for (t, a) in client.assets if t == TAG}
    assert {"manifest.json", "NOTICE.md", "lahman_2025_csv.zip", "batting.parquet"} <= uploaded
    assert len([a for a in uploaded if a.endswith(".parquet")]) == 27
    pointer = json.loads(client.assets[(POINTER_TAG, "latest.json")])
    assert pointer["latest"] == TAG and pointer["by_schema"] == {"1": TAG}
    assert client.calls[-1] == ("upload_asset", POINTER_TAG, "latest.json", True)


def test_verification_failure_leaves_pointer_unchanged(ctx):
    client = FakeClient(corrupt="manifest.json")
    with pytest.raises(PublishError, match="manifest.json"):
        run(ctx, client)
    assert not any(c[0] in ("upload_asset", "ensure_release") for c in client.calls)


def test_parquet_hash_mismatch_fails_verification(ctx):
    first = next(iter(ctx["manifest"]["tables"].values()))["file"]  # the one file verified
    client = FakeClient(corrupt=first)
    with pytest.raises(PublishError, match=first):
        run(ctx, client)
    assert "upload_asset" not in client.names()


def test_same_day_tag_collision_is_rejected(ctx):
    # the manifest's tag is fixed at build time; publishing over an existing tag must fail
    client = FakeClient(tags=[TAG])
    with pytest.raises(PublishError, match="already exists"):
        run(ctx, client)
    assert "create_release" not in client.names()


def test_make_latest_keeps_other_schema_pointers(ctx):
    prev = {
        "latest": "lahman-2026-01-05",
        "by_schema": {"0": "lahman-2025-01-01", "1": "lahman-2026-01-05"},
    }
    seen = {"version": "2025", "sha256": "a" * 64}
    latest = make_latest("lahman", prev, ctx["manifest"], seen, NOW)
    assert latest["by_schema"] == {"0": "lahman-2025-01-01", "1": TAG}
    assert latest["upstream_seen"]["sha256"] == "a" * 64
    assert latest["upstream_seen"]["checked_at"] == "2026-10-02T13:20:11Z"
    assert latest["updated_at"] == "2026-10-02T13:20:11Z"


def test_make_latest_appends_a_releases_entry_for_the_new_manifest(ctx):
    prev = {"latest": None, "by_schema": {}, "releases": []}
    seen = {"version": "2025", "sha256": "a" * 64}
    latest = make_latest("lahman", prev, ctx["manifest"], seen, NOW)
    assert latest["releases"] == [
        {
            "tag": TAG,
            "version": "2025",
            "schema_version": SCHEMA_VERSION,
            "built_at": ctx["manifest"]["built_at"],
            "withdrawn": False,
        }
    ]


def test_make_latest_keeps_earlier_releases_in_the_index(ctx):
    earlier = {
        "tag": "lahman-2026-01-05", "version": "2025", "schema_version": 1,
        "built_at": "2026-01-05T00:00:00Z", "withdrawn": False,
    }  # fmt: skip
    prev = {"latest": earlier["tag"], "by_schema": {"1": earlier["tag"]}, "releases": [earlier]}
    latest = make_latest(
        "lahman", prev, ctx["manifest"], {"version": "2025", "sha256": "a" * 64}, NOW
    )
    assert latest["releases"] == [
        earlier,
        {**earlier, "tag": TAG, "built_at": ctx["manifest"]["built_at"]},
    ]


def test_make_latest_without_a_previous_pointer_starts_a_fresh_index(ctx):
    latest = make_latest(
        "lahman", None, ctx["manifest"], {"version": "2025", "sha256": "a" * 64}, NOW
    )
    assert [r["tag"] for r in latest["releases"]] == [TAG]


def test_make_latest_without_a_manifest_leaves_the_index_unchanged(ctx):
    entry = {
        "tag": "lahman-2026-01-05", "version": "2025", "schema_version": 1,
        "built_at": "2026-01-05T00:00:00Z", "withdrawn": False,
    }  # fmt: skip
    prev = {"latest": entry["tag"], "by_schema": {"1": entry["tag"]}, "releases": [entry]}
    latest = make_latest("lahman", prev, None, {"version": "2025", "sha256": "a" * 64}, NOW)
    assert latest["releases"] == [entry]


def test_mark_withdrawn_flags_one_release_and_leaves_others(ctx):
    a = {"tag": "t1", "version": "1", "schema_version": 1, "built_at": "x", "withdrawn": False}
    b = {"tag": "t2", "version": "1", "schema_version": 1, "built_at": "y", "withdrawn": False}
    pointer = {"latest": "t2", "by_schema": {"1": "t2"}, "releases": [a, b]}
    updated = mark_withdrawn(pointer, "t1")
    assert [r["withdrawn"] for r in updated["releases"]] == [True, False]
    assert updated["latest"] == "t2"  # withdrawing an old release doesn't move the pointer


def test_mark_withdrawn_unknown_tag_raises(ctx):
    pointer = {"latest": "t1", "by_schema": {}, "releases": [{"tag": "t1", "withdrawn": False}]}
    with pytest.raises(KeyError, match="nope"):
        mark_withdrawn(pointer, "nope")


def test_record_upstream_seen_keeps_latest_pointer(ctx):
    prev = {
        "source": "lahman",
        "latest": "lahman-2026-01-05",
        "by_schema": {"1": "lahman-2026-01-05"},
    }
    client = FakeClient()
    record_upstream_seen(
        client, "lahman", prev, {"version": "2025", "sha256": "b" * 64}, NOW, ctx["out"]
    )
    pointer = json.loads(client.assets[(POINTER_TAG, "latest.json")])
    assert pointer["latest"] == "lahman-2026-01-05"
    assert pointer["upstream_seen"]["sha256"] == "b" * 64
    assert "create_release" not in client.names()


def test_notes_mention_version_changes_and_attribution(ctx):
    notes = make_notes(LAHMAN, ctx["manifest"])
    assert "2025" in notes and "2026-01-02" in notes and LAHMAN.attribution in notes


def test_notice_states_license_and_negro_leagues_source(ctx):
    n = make_notice(LAHMAN, ctx["manifest"])
    assert "CC BY-SA 3.0" in n and "Seamheads" in n


def test_gh_client_builds_commands(tmp_path: Path):
    calls = []

    def fake_run(cmd, **kw):
        calls.append(cmd)

        class R:
            stdout = "[]"
            returncode = 0

        return R()

    c = GhClient(REPO, run=fake_run)
    f = tmp_path / "a.parquet"
    f.write_bytes(b"x")
    c.create_release("t1", "Title", "notes", [f], latest=False)
    c.upload_asset("lahman-latest", f, clobber=True)
    c.existing_tags()
    flat = [" ".join(map(str, cmd)) for cmd in calls]
    assert any(
        s.startswith("gh release create t1") and "--latest=false" in s and "--repo " + REPO in s
        for s in flat
    )
    assert any("gh release upload lahman-latest" in s and "--clobber" in s for s in flat)
    assert any("gh release list" in s for s in flat)


def test_release_and_pointer_titles_are_their_tags(ctx):
    client = FakeClient()
    plan = run(ctx, client)
    assert plan["title"] == TAG and client.titles[TAG] == TAG
    assert client.titles[POINTER_TAG] == POINTER_TAG


def test_title_is_the_tag_even_after_a_previous_release(ctx):
    ctx["manifest"]["changes"]["previous"] = "lahman-2026-01-05"
    assert run(ctx, None, dry_run=True)["title"] == TAG


def test_notes_describe_the_upstream_version_instead_of_naming_the_release(ctx):
    first = make_notes(LAHMAN, ctx["manifest"]).splitlines()[0]
    assert first == "Built from upstream Lahman database version 2025 (released 2026-01-02)."
