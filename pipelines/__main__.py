"""CLI: python -m pipelines <source> <detect|build|publish|all>
[--source-url URL] [--force] [--dry-run] [--out DIR] [--delay SECONDS]"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipelines.core.http import new_session
from pipelines.core.pipeline import StageError, run_all, stage_build, stage_detect, stage_publish
from pipelines.core.publish import GhClient
from pipelines.sources import SOURCES

PIPELINE_VERSION = "0.1.0"
DEFAULT_REPO = "FishOfPitt116/fishbaseball-data"


def _read_json(path: Path, needs: str) -> dict[str, Any]:
    if not path.exists():
        raise StageError(needs, f"{path} not found: run the `{needs}` stage first")
    return json.loads(path.read_text())


def _print_progress(message: str) -> None:
    # `flush=True`: a hosted CI runner has been observed cancelling a step that produces no
    # output for a long stretch (confirmed real: ~19 minutes checking/downloading ~129
    # Retrosheet seasons), so each line must reach the log immediately, not sit buffered.
    print(message, flush=True)


def main(
    argv: Sequence[str] | None = None,
    *,
    session: Any = None,
    client: Any = None,
    full_dataset: bool = True,
    now: datetime | None = None,
) -> int:
    ap = argparse.ArgumentParser(prog="python -m pipelines")
    ap.add_argument("source", choices=sorted(SOURCES))
    ap.add_argument("command", choices=["detect", "build", "publish", "all"])
    ap.add_argument("--source-url", default=None, help="zip URL or local path; overrides all")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--out", default="build")
    ap.add_argument(
        "--delay", type=float, default=None,
        help="seconds between per-partition requests (partitioned sources only; "
        "default set by the stage itself)",
    )  # fmt: skip
    args = ap.parse_args(argv)

    config, schema = SOURCES[args.source]
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    repo = os.environ.get("GITHUB_REPOSITORY", DEFAULT_REPO)
    session = session or new_session()
    now = now or datetime.now(timezone.utc)
    if client is None and not args.dry_run:
        client = GhClient(repo)
    common: dict[str, Any] = {
        "out_dir": out_dir, "repo": repo, "session": session, "progress": _print_progress,
    }  # fmt: skip
    error_file = out_dir / "error.json"
    try:
        if args.command == "detect":
            result: dict[str, Any] = stage_detect(
                config, source_url=args.source_url, force=args.force, delay=args.delay, **common
            )
        elif args.command == "build":
            found = _read_json(out_dir / "detect.json", "detect")
            result = stage_build(
                config, schema, client=client, found=found, now=now, force=args.force,
                pipeline_version=PIPELINE_VERSION, full_dataset=full_dataset, **common,
            )  # fmt: skip
        elif args.command == "publish":
            found = _read_json(out_dir / "detect.json", "detect")
            build = _read_json(out_dir / "build.json", "build")
            result = stage_publish(
                config, client=client, found=found, build=build, dry_run=args.dry_run, now=now,
                **common,
            )  # fmt: skip
        else:
            result = run_all(
                config, schema, client=client, source_url=args.source_url, force=args.force,
                dry_run=args.dry_run, now=now, pipeline_version=PIPELINE_VERSION,
                full_dataset=full_dataset, delay=args.delay, **common,
            )  # fmt: skip
    except StageError as e:
        error_file.write_text(json.dumps({"stage": e.stage, "error": str(e)}, indent=2) + "\n")
        print(str(e), file=sys.stderr)
        return 1
    error_file.unlink(missing_ok=True)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
