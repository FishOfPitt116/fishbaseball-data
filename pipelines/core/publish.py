from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from pipelines.core.config import SourceConfig

POINTER_SUFFIX = "-latest"


class PublishError(Exception):
    pass


class ReleaseClient(Protocol):
    def existing_tags(self) -> list[str]: ...
    def create_release(
        self, tag: str, title: str, notes: str, assets: Sequence[Path], *, latest: bool = False
    ) -> None: ...
    def ensure_release(self, tag: str, title: str, notes: str) -> None: ...
    def upload_asset(self, tag: str, path: Path, *, clobber: bool = True) -> None: ...
    def fetch(self, tag: str, filename: str) -> bytes: ...


class GhClient:
    """Release operations through the `gh` CLI (GH_TOKEN comes from the environment)."""

    def __init__(self, repo: str, run: Callable[..., Any] = subprocess.run):
        self.repo, self._run = repo, run

    def _gh(self, *args: str, check: bool = True) -> Any:
        return self._run(["gh", *args, "--repo", self.repo], capture_output=True, check=check)

    def existing_tags(self) -> list[str]:
        out = self._gh("release", "list", "--limit", "1000", "--json", "tagName").stdout
        return [r["tagName"] for r in json.loads(out)]

    def create_release(
        self, tag: str, title: str, notes: str, assets: Sequence[Path], *, latest: bool = False
    ) -> None:
        self._gh(
            "release", "create", tag, "--title", title, "--notes", notes,
            f"--latest={'true' if latest else 'false'}", *map(str, assets),
        )  # fmt: skip

    def ensure_release(self, tag: str, title: str, notes: str) -> None:
        if self._gh("release", "view", tag, check=False).returncode != 0:
            self.create_release(tag, title, notes, [], latest=False)

    def upload_asset(self, tag: str, path: Path, *, clobber: bool = True) -> None:
        self._gh("release", "upload", tag, str(path), *(["--clobber"] if clobber else []))

    def fetch(self, tag: str, filename: str) -> bytes:
        out = self._gh("release", "download", tag, "--pattern", filename, "--output", "-").stdout
        return out if isinstance(out, bytes) else out.encode()


def _utc(now: datetime) -> str:
    return now.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def pointer_tag(source: str) -> str:
    return f"{source}{POINTER_SUFFIX}"


def make_notice(config: SourceConfig, manifest: Mapping[str, Any], *, extra: str = "") -> str:
    """`extra` is for a source-specific sentence (e.g. Lahman's Negro Leagues/Seamheads
    attribution) that doesn't belong hardcoded in a function every source shares."""
    up = manifest["upstream"]
    version_line = f"version {up['version']}"
    if up.get("released"):
        version_line += f", released {up['released']}"
    notice = (
        f"# {config.name.capitalize()} data notice\n\n"
        f"{config.attribution}\n\n"
        f"License: {config.license}. Derived files here (Parquet conversions) are shared "
        "under the same license.\n\n"
        f"Upstream: {up['page_url']} — {version_line}. These files are converted, unmodified "
        "in content, from the upstream CSV release; column names were changed to snake_case "
        "(see `column_map` in manifest.json)."
    )
    if extra:
        notice += f" {extra}"
    return notice + "\n"


def make_notes(config: SourceConfig, manifest: Mapping[str, Any]) -> str:
    up, ch = manifest["upstream"], manifest["changes"]
    version_line = (
        f"Built from upstream {config.name.capitalize()} database version {up['version']}"
    )
    if up.get("released"):
        version_line += f" (released {up['released']})"
    lines = [
        version_line + ".",
        f"Schema version {manifest['schema_version']}. Built {manifest['built_at']}.",
        "",
        "## Changes",
    ]
    if ch["previous"] is None:
        lines.append("First release.")
    else:
        lines.append(f"Since `{ch['previous']}`:")
        lines.append(f"- tables added: {ch['tables_added'] or 'none'}")
        lines.append(f"- tables changed: {ch['tables_changed'] or 'none'}")
        if partitions_changed := ch.get("partitions_changed"):
            for table, seasons in partitions_changed.items():
                lines.append(f"- {table}: partitions rebuilt: {seasons}")
        for table, delta in ch["row_deltas"].items():
            if isinstance(delta, dict):
                parts = ", ".join(f"{k}: {v:+d}" for k, v in sorted(delta.items()))
                lines.append(f"- {table}: {parts}")
            else:
                lines.append(f"- {table}: {delta:+d} rows")
    lines += ["", config.attribution]
    return "\n".join(lines) + "\n"


def make_latest(
    source: str,
    previous: Mapping[str, Any] | None,
    manifest: Mapping[str, Any] | None,
    upstream_seen: Mapping[str, Any],
    now: datetime,
) -> dict[str, Any]:
    """The mutable `latest.json` pointer. With a manifest it moves `latest` (and the entry for
    its schema version); without one it only records what upstream we last saw."""
    prev = previous or {}
    by_schema = dict(prev.get("by_schema", {}))
    releases = [dict(r) for r in prev.get("releases", [])]
    latest = prev.get("latest")
    if manifest is not None:
        latest = manifest["tag"]
        by_schema[str(manifest["schema_version"])] = manifest["tag"]
        releases.append(
            {
                "tag": manifest["tag"],
                "version": manifest["version"],
                "schema_version": manifest["schema_version"],
                "built_at": manifest["built_at"],
                "withdrawn": False,
            }
        )
    return {
        "source": source,
        "latest": latest,
        "by_schema": by_schema,
        "releases": releases,
        "upstream_seen": {**upstream_seen, "checked_at": _utc(now)},
        "updated_at": _utc(now),
    }


def mark_withdrawn(pointer: Mapping[str, Any], tag: str) -> dict[str, Any]:
    """Flag `tag` as withdrawn in a `latest.json` object, for the rollback runbook. Does not
    move `latest`/`by_schema`; do that (point them at the replacement release) separately."""
    releases = [dict(r) for r in pointer.get("releases", [])]
    if not any(r["tag"] == tag for r in releases):
        raise KeyError(f"no release {tag!r} in the releases index")
    for r in releases:
        if r["tag"] == tag:
            r["withdrawn"] = True
    return {**pointer, "releases": releases}


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _table_entries(manifest: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Every (file, sha256) pair this manifest describes: one per single-file table, or one
    per partition for a partitioned table."""
    entries = []
    for entry in manifest["tables"].values():
        if entry.get("partitioned"):
            entries += list(entry["partitions"].values())
        else:
            entries.append(entry)
    return entries


def _fresh_table_files(manifest: Mapping[str, Any], build_dir: Path) -> list[dict[str, Any]]:
    """Which of `_table_entries` this release actually uploads: every single-file table
    (always rebuilt in full) plus only the partitions whose Parquet was written this run — an
    unchanged partition's manifest entry just points at whichever older release already holds
    it, and its file was never written here, so it's never re-uploaded."""
    tables_dir = build_dir / "tables"
    return [e for e in _table_entries(manifest) if (tables_dir / e["file"]).exists()]


def _stage_assets(
    manifest: Mapping[str, Any], build_dir: Path, zip_path: Path | None, dest: Path
) -> list[Path]:
    dest.mkdir(parents=True, exist_ok=True)
    sources = [build_dir / "tables" / e["file"] for e in _fresh_table_files(manifest, build_dir)]
    sources += [build_dir / "manifest.json", build_dir / "NOTICE.md"]
    staged = []
    for src in sources:
        shutil.copyfile(src, dest / src.name)
        staged.append(dest / src.name)
    if zip_path is not None:
        shutil.copyfile(zip_path, dest / manifest["upstream"]["file"])
        staged.append(dest / manifest["upstream"]["file"])
    return staged


def _asset_names(manifest: Mapping[str, Any], build_dir: Path) -> list[str]:
    names = [e["file"] for e in _fresh_table_files(manifest, build_dir)]
    names += ["manifest.json", "NOTICE.md"]
    if "file" in manifest.get("upstream", {}):
        names.append(manifest["upstream"]["file"])
    return names


def _verify(client: ReleaseClient, manifest: Mapping[str, Any], build_dir: Path) -> None:
    tag = manifest["tag"]
    remote = client.fetch(tag, "manifest.json")
    if _sha256(remote) != _sha256((build_dir / "manifest.json").read_bytes()):
        raise PublishError(f"verification failed: manifest.json from {tag} differs from local")
    fresh = _fresh_table_files(manifest, build_dir)
    if not fresh:
        return  # nothing else was actually uploaded this release
    entry = fresh[0]
    if _sha256(client.fetch(tag, entry["file"])) != entry["sha256"]:
        raise PublishError(f"verification failed: {entry['file']} from {tag} has wrong SHA-256")


def _pointer_notes(source: str) -> str:
    return (
        f"Mutable pointer for `{source}` data. Its `latest.json` asset names the current release; "
        "it holds no data itself. Do not depend on this release's other contents."
    )


def _upload_pointer(
    client: ReleaseClient, source: str, latest: Mapping[str, Any], build_dir: Path
) -> None:
    build_dir.mkdir(parents=True, exist_ok=True)
    path = build_dir / "latest.json"
    path.write_text(json.dumps(latest, indent=2) + "\n")
    tag = pointer_tag(source)
    client.ensure_release(tag, tag, _pointer_notes(source))
    client.upload_asset(tag, path, clobber=True)


def publish(
    client: ReleaseClient | None,
    *,
    config: SourceConfig,
    manifest: Mapping[str, Any],
    build_dir: Path,
    zip_path: Path | None,
    previous_latest: Mapping[str, Any] | None,
    dry_run: bool,
    now: datetime,
    upstream_seen: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """`upstream_seen` is what gets written to the `latest.json` pointer for change detection
    on the next run. Default (`None`): Lahman's shape, `{"version", "sha256"}` from the single
    upstream zip. A partitioned source (no single zip) passes its own shape explicitly — e.g.
    per-partition ETags — since there's nothing to derive it from here."""
    tag = manifest["tag"]
    title = tag  # several releases can share an upstream version; the dated tag is the name
    notes = make_notes(config, manifest)
    plan: dict[str, Any] = {
        "tag": tag, "title": title, "assets": _asset_names(manifest, build_dir), "notes": notes,
        "published": False,
    }  # fmt: skip
    if dry_run:
        return plan
    if client is None:
        raise PublishError("a release client is required unless --dry-run")
    if tag in client.existing_tags():
        raise PublishError(f"release {tag} already exists; versioned releases are immutable")
    assets = _stage_assets(manifest, build_dir, zip_path, build_dir / "release_assets")
    client.create_release(tag, title, notes, assets, latest=False)
    _verify(client, manifest, build_dir)
    if upstream_seen is None:
        upstream_seen = {
            "version": manifest["upstream"]["version"],
            "sha256": manifest["upstream"]["sha256"],
        }
    _upload_pointer(
        client,
        config.name,
        make_latest(config.name, previous_latest, manifest, upstream_seen, now),
        build_dir,
    )
    plan["published"] = True
    return plan


def record_upstream_seen(
    client: ReleaseClient,
    source: str,
    previous_latest: Mapping[str, Any],
    upstream_seen: Mapping[str, Any],
    now: datetime,
    build_dir: Path,
) -> None:
    """Upstream changed bytes but not content: remember the new hash, release nothing."""
    latest = make_latest(source, previous_latest, None, upstream_seen, now)
    _upload_pointer(client, source, latest, build_dir)
