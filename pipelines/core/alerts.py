"""Failure alerts through GitHub issues: one open issue per (source, stage), commented on
repeat failures and closed on the next successful run.

CLI: python -m pipelines.core.alerts <source> --run-url URL [--out DIR] [--resolve]
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any, Protocol


class IssueClient(Protocol):
    def ensure_label(self, name: str) -> None: ...
    def find_open(self, labels: Sequence[str]) -> list[int]: ...
    def comment(self, number: int, body: str) -> None: ...
    def create(self, title: str, body: str, labels: Sequence[str]) -> int: ...
    def close(self, number: int, comment: str) -> None: ...


class GhIssueClient:
    def __init__(self, repo: str, run: Callable[..., Any] = subprocess.run):
        self.repo, self._run = repo, run

    def _gh(self, *args: str) -> Any:
        return self._run(
            ["gh", *args, "--repo", self.repo], capture_output=True, text=True, check=True
        )

    def ensure_label(self, name: str) -> None:
        self._gh("label", "create", name, "--force")

    def find_open(self, labels: Sequence[str]) -> list[int]:
        args = ["issue", "list", "--state", "open", "--json", "number"]
        for label in labels:
            args += ["--label", label]
        return [i["number"] for i in json.loads(self._gh(*args).stdout)]

    def comment(self, number: int, body: str) -> None:
        self._gh("issue", "comment", str(number), "--body", body)

    def create(self, title: str, body: str, labels: Sequence[str]) -> int:
        args = ["issue", "create", "--title", title, "--body", body]
        for label in labels:
            args += ["--label", label]
        url = self._gh(*args).stdout.strip()
        return int(url.rsplit("/", 1)[-1]) if url else 0

    def close(self, number: int, comment: str) -> None:
        self._gh("issue", "close", str(number), "--comment", comment)


def report_failure(client: IssueClient, source: str, stage: str, error: str, run_url: str) -> int:
    labels = [f"pipeline:{source}", f"stage:{stage}"]
    for label in labels:
        client.ensure_label(label)
    body = f"Stage: `{stage}`\nRun: {run_url}\n\n```\n{error.strip()}\n```\n"
    if existing := client.find_open(labels):
        client.comment(existing[0], f"Failed again.\n\n{body}")
        return existing[0]
    return client.create(f"{source} pipeline failed at {stage}", body, labels)


def resolve(client: IssueClient, source: str, run_url: str) -> int:
    """Close every open alert for `source`; returns how many were closed."""
    numbers = client.find_open([f"pipeline:{source}"])
    for n in numbers:
        client.close(n, f"Resolved by a successful run: {run_url}")
    return len(numbers)


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="pipelines.core.alerts")
    ap.add_argument("source")
    ap.add_argument("--run-url", required=True)
    ap.add_argument("--out", default="build")
    ap.add_argument("--resolve", action="store_true", help="close open alerts (after a success)")
    args = ap.parse_args(argv)
    client = GhIssueClient(os.environ["GITHUB_REPOSITORY"])
    if args.resolve:
        resolve(client, args.source, args.run_url)
        return 0
    err_file = Path(args.out) / "error.json"
    info = json.loads(err_file.read_text()) if err_file.exists() else {}
    report_failure(
        client,
        args.source,
        info.get("stage", "unknown"),
        info.get("error", "see run log"),
        args.run_url,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
