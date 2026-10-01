"""Daily upstream health checks. Availability failures (site down, timeouts) alert after three
consecutive failed runs; format failures (page or files changed shape) alert immediately.

CLI: python -m pipelines.core.canary <source> [--out DIR]                       run checks
     python -m pipelines.core.canary <source> --alert --run-url U --previous a,b   alert
     python -m pipelines.core.canary <source> --resolve --run-url U             close alerts
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import tempfile
import zipfile
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from pipelines.core.alerts import GhIssueClient, report_failure, resolve
from pipelines.core.config import SourceConfig
from pipelines.core.detect import USER_AGENT, UpstreamFormatError, parse_page
from pipelines.core.download import DownloadError, download
from pipelines.core.http import new_session

CANARY_STAGES = ("canary-format", "canary-availability")
CSV_LINK = re.compile(
    r"<a[^>]+href=\"https://[a-z.]*box\.com/[^\"]+\"[^>]*>\s*Comma-delimited", re.I
)
CONSECUTIVE_FAILURES = 3


@dataclass
class Check:
    name: str
    kind: str  # "availability" | "format"
    ok: bool
    detail: str = ""
    skipped: bool = False


def should_alert_availability(previous_conclusions: Sequence[str]) -> bool:
    """`previous_conclusions` are the latest completed runs, newest first. The current run is
    the third failure in a row only if the two before it also failed."""
    prior = CONSECUTIVE_FAILURES - 1
    return len(previous_conclusions) >= prior and all(
        c == "failure" for c in previous_conclusions[:prior]
    )


def _zip_contents(zip_path: Path, config: SourceConfig) -> Check:
    problems: list[str] = []
    with zipfile.ZipFile(zip_path) as zf:
        members = {n.split("/")[-1]: n for n in zf.namelist() if n.lower().endswith(".csv")}
        if missing := sorted(set(config.tables) - set(members)):
            problems.append(f"missing {missing}")
        if extra := sorted(set(members) - set(config.tables)):
            problems.append(f"unexpected {extra}")
        for csv_name, member in sorted(members.items()):
            if csv_name not in config.tables:
                continue
            with zf.open(member) as f:
                header = f.readline().decode("utf-8").lstrip("﻿").rstrip("\r\n")
            cols = next(csv.reader(io.StringIO(header)))
            if set(cols) != set(config.columns[config.tables[csv_name]]):
                problems.append(f"{csv_name} headers changed")
    return Check("zip_contents", "format", not problems, "; ".join(problems))


def run_checks(config: SourceConfig, session: Any) -> list[Check]:
    checks: list[Check] = []
    headers = {"User-Agent": USER_AGENT}
    html = ""
    try:
        resp = session.get(config.page_url, timeout=30, headers=headers)
        resp.raise_for_status()
        html = resp.text
        checks.append(Check("page_reachable", "availability", True))
    except Exception as e:
        checks.append(Check("page_reachable", "availability", False, str(e)))
    if html:
        try:
            info = parse_page(html, config.version_pattern)
            checks.append(
                Check("version_parses", "format", True, f"{info.version} {info.released}")
            )
        except UpstreamFormatError as e:
            checks.append(Check("version_parses", "format", False, str(e)))
        found = bool(CSV_LINK.search(html))
        checks.append(Check("csv_link_present", "format", found, "" if found else "no CSV link"))
    if not config.download_urls:
        note = "no download URL configured"
        checks.append(Check("download_is_zip", "availability", True, note, skipped=True))
        checks.append(Check("zip_contents", "format", True, note, skipped=True))
        return checks
    with tempfile.TemporaryDirectory() as tmp:
        try:
            path = download(
                config.download_urls,
                Path(tmp) / "canary.zip",
                session=session,
                user_agent=USER_AGENT,
            )
        except DownloadError as e:
            fmt = any(m in str(e) for m in ("got HTML", "not a zip", "not a valid zip"))
            checks.append(
                Check("download_is_zip", "format" if fmt else "availability", False, str(e))
            )
            checks.append(Check("zip_contents", "format", True, "no zip to check", skipped=True))
            return checks
        checks.append(Check("download_is_zip", "availability", True))
        checks.append(_zip_contents(path, config))
    return checks


def _alert(source: str, checks: list[dict[str, Any]], run_url: str, previous: list[str]) -> None:
    client = GhIssueClient(os.environ["GITHUB_REPOSITORY"])
    failed = [c for c in checks if not c["ok"]]
    for kind in ("format", "availability"):
        of_kind = [c for c in failed if c["kind"] == kind]
        if not of_kind:
            continue
        if kind == "availability" and not should_alert_availability(previous):
            continue
        detail = "\n".join(f"{c['name']}: {c['detail']}" for c in of_kind)
        report_failure(client, source, f"canary-{kind}", detail, run_url)


def main(argv: Sequence[str] | None = None, *, session: Any = None) -> int:
    from pipelines.sources import SOURCES

    ap = argparse.ArgumentParser(prog="pipelines.core.canary")
    ap.add_argument("source", choices=sorted(SOURCES))
    ap.add_argument("--out", default="build")
    ap.add_argument("--alert", action="store_true")
    ap.add_argument("--resolve", action="store_true")
    ap.add_argument("--run-url", default="")
    ap.add_argument(
        "--previous", default="", help="comma-separated prior conclusions, newest first"
    )
    args = ap.parse_args(argv)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    results_file = out / "canary.json"

    if args.resolve:
        client = GhIssueClient(os.environ["GITHUB_REPOSITORY"])
        resolve(client, args.source, args.run_url, list(CANARY_STAGES))
        return 0
    if args.alert:
        checks = json.loads(results_file.read_text())
        _alert(args.source, checks, args.run_url, [p for p in args.previous.split(",") if p])
        return 0

    config, _ = SOURCES[args.source]
    checks = run_checks(config, session or new_session())
    results_file.write_text(json.dumps([asdict(c) for c in checks], indent=2) + "\n")
    for c in checks:
        state = "skip" if c.skipped else ("ok" if c.ok else "FAIL")
        print(f"{state:5} {c.name} {c.detail}".rstrip())
    return 0 if all(c.ok for c in checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
