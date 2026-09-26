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


def make_notice(config: SourceConfig, manifest: Mapping[str, Any]) -> str:
    up = manifest["upstream"]
    return (
        f"# {config.name.capitalize()} data notice\n\n"
        f"{config.attribution}\n\n"
        f"License: {config.license} (http://creativecommons.org/licenses/by-sa/3.0/). "
        f"Derived files here (Parquet conversions) are shared under the same license.\n\n"
        f"Upstream: {up['page_url']} — version {up['version']}, released {up['released']}. "
        "Negro Leagues data is licensed by SABR from Seamheads.com. "
        "These files are converted, unmodified in content, from SABR's CSV release; "
        "column names were changed to snake_case (see `column_map` in manifest.json).\n"
    )


def make_notes(config: SourceConfig, manifest: Mapping[str, Any]) -> str:
    up, ch = manifest["upstream"], manifest["changes"]
    lines = [
        f"{config.name.capitalize()} {up['version']}, released {up['released']} (upstream).",
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
        for table, delta in ch["row_deltas"].items():
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
    latest = prev.get("latest")
    if manifest is not None:
        latest = manifest["tag"]
        by_schema[str(manifest["schema_version"])] = manifest["tag"]
    return {
        "source": source,
        "latest": latest,
        "by_schema": by_schema,
        "upstream_seen": {**upstream_seen, "checked_at": _utc(now)},
        "updated_at": _utc(now),
    }


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _stage_assets(
    manifest: Mapping[str, Any], build_dir: Path, zip_path: Path, dest: Path
) -> list[Path]:
    dest.mkdir(parents=True, exist_ok=True)
    sources = [build_dir / "tables" / t["file"] for t in manifest["tables"].values()]
    sources += [build_dir / "manifest.json", build_dir / "NOTICE.md"]
    staged = []
    for src in sources:
        shutil.copyfile(src, dest / src.name)
        staged.append(dest / src.name)
    shutil.copyfile(zip_path, dest / manifest["upstream"]["file"])
    staged.append(dest / manifest["upstream"]["file"])
    return staged


def _asset_names(manifest: Mapping[str, Any]) -> list[str]:
    return [
        *(t["file"] for t in manifest["tables"].values()),
        "manifest.json",
        manifest["upstream"]["file"],
        "NOTICE.md",
    ]


def _verify(client: ReleaseClient, manifest: Mapping[str, Any], build_dir: Path) -> None:
    tag = manifest["tag"]
    remote = client.fetch(tag, "manifest.json")
    if _sha256(remote) != _sha256((build_dir / "manifest.json").read_bytes()):
        raise PublishError(f"verification failed: manifest.json from {tag} differs from local")
    entry = next(iter(manifest["tables"].values()))
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
    client.ensure_release(tag, f"{source.capitalize()} latest pointer", _pointer_notes(source))
    client.upload_asset(tag, path, clobber=True)


def publish(
    client: ReleaseClient | None,
    *,
    config: SourceConfig,
    manifest: Mapping[str, Any],
    build_dir: Path,
    zip_path: Path,
    previous_latest: Mapping[str, Any] | None,
    dry_run: bool,
    now: datetime,
) -> dict[str, Any]:
    tag = manifest["tag"]
    title = f"{config.name.capitalize()} {manifest['version']}"
    if manifest["changes"]["previous"] is not None:
        title += f" ({tag.removeprefix(config.name + '-')})"
    notes = make_notes(config, manifest)
    plan: dict[str, Any] = {
        "tag": tag, "title": title, "assets": _asset_names(manifest), "notes": notes,
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
    seen = {"version": manifest["upstream"]["version"], "sha256": manifest["upstream"]["sha256"]}
    _upload_pointer(
        client,
        config.name,
        make_latest(config.name, previous_latest, manifest, seen, now),
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
