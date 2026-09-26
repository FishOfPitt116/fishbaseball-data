import json
from pathlib import Path

from pipelines.core.alerts import GhIssueClient, main, report_failure, resolve

RUN = "https://github.com/o/r/actions/runs/1"


class FakeIssues:
    def __init__(self):
        self.issues: dict[int, dict] = {}
        self.labels: set[str] = set()
        self.next = 1

    def ensure_label(self, name):
        self.labels.add(name)

    def find_open(self, labels):
        return [n for n, i in self.issues.items() if i["open"] and set(labels) <= set(i["labels"])]

    def comment(self, number, body):
        self.issues[number]["comments"].append(body)

    def create(self, title, body, labels):
        n = self.next
        self.next += 1
        self.issues[n] = {
            "title": title,
            "body": body,
            "labels": labels,
            "open": True,
            "comments": [],
        }
        return n

    def close(self, number, comment):
        self.issues[number]["open"] = False
        self.issues[number]["comments"].append(comment)


def test_failure_opens_an_issue_with_stage_error_and_run_url():
    c = FakeIssues()
    n = report_failure(c, "lahman", "build", "cast failed", RUN)
    issue = c.issues[n]
    assert issue["labels"] == ["pipeline:lahman", "stage:build"]
    assert "build" in issue["title"] and "cast failed" in issue["body"] and RUN in issue["body"]
    assert {"pipeline:lahman", "stage:build"} <= c.labels


def test_repeat_failure_comments_instead_of_opening_another():
    c = FakeIssues()
    first = report_failure(c, "lahman", "build", "boom", RUN)
    second = report_failure(c, "lahman", "build", "boom again", RUN + "2")
    assert first == second and len(c.issues) == 1
    assert len(c.issues[first]["comments"]) == 1 and "boom again" in c.issues[first]["comments"][0]


def test_different_stage_opens_a_separate_issue():
    c = FakeIssues()
    report_failure(c, "lahman", "build", "x", RUN)
    report_failure(c, "lahman", "publish", "y", RUN)
    assert len(c.issues) == 2


def test_success_closes_open_issues_for_the_source():
    c = FakeIssues()
    report_failure(c, "lahman", "build", "x", RUN)
    report_failure(c, "lahman", "publish", "y", RUN)
    other = c.create("other", "", ["pipeline:retrosheet"])
    assert resolve(c, "lahman", RUN) == 2
    assert all(not i["open"] for n, i in c.issues.items() if n != other)
    assert c.issues[other]["open"] is True


def test_resolve_with_nothing_open_is_a_noop():
    assert resolve(FakeIssues(), "lahman", RUN) == 0


def test_cli_reads_stage_and_error_from_error_json(tmp_path: Path, monkeypatch):
    (tmp_path / "error.json").write_text(json.dumps({"stage": "detect", "error": "no pattern"}))
    fake = FakeIssues()
    monkeypatch.setattr("pipelines.core.alerts.GhIssueClient", lambda repo: fake)
    monkeypatch.setenv("GITHUB_REPOSITORY", "o/r")
    assert main(["lahman", "--run-url", RUN, "--out", str(tmp_path)]) == 0
    (issue,) = fake.issues.values()
    assert issue["labels"] == ["pipeline:lahman", "stage:detect"] and "no pattern" in issue["body"]


def test_cli_resolve_flag(monkeypatch, tmp_path: Path):
    fake = FakeIssues()
    fake.create("t", "b", ["pipeline:lahman", "stage:build"])
    monkeypatch.setattr("pipelines.core.alerts.GhIssueClient", lambda repo: fake)
    monkeypatch.setenv("GITHUB_REPOSITORY", "o/r")
    assert main(["lahman", "--resolve", "--run-url", RUN, "--out", str(tmp_path)]) == 0
    assert all(not i["open"] for i in fake.issues.values())


def test_gh_issue_client_builds_commands():
    calls = []

    class R:
        stdout = '[{"number": 7}]'
        returncode = 0

    class Created:
        stdout = "https://github.com/o/r/issues/8\n"
        returncode = 0

    def fake_run(cmd, **kw):
        calls.append(" ".join(map(str, cmd)))
        return Created() if "create" in cmd and "issue" in cmd else R()

    c = GhIssueClient("o/r", run=fake_run)
    assert c.find_open(["pipeline:lahman", "stage:build"]) == [7]
    assert c.create("t", "b", ["pipeline:lahman"]) == 8
    c.close(7, "fixed")
    assert any(
        "gh issue list" in s and "--label pipeline:lahman" in s and "--state open" in s
        for s in calls
    )
    assert any("gh issue create" in s and "--label pipeline:lahman" in s for s in calls)
    assert any("gh issue close 7" in s for s in calls)


def test_resolve_can_be_limited_to_stages():
    c = FakeIssues()
    report_failure(c, "lahman", "build", "x", RUN)
    report_failure(c, "lahman", "canary-format", "y", RUN)
    assert resolve(c, "lahman", RUN, stages=["canary-format", "canary-availability"]) == 1
    open_stages = [i["labels"][1] for i in c.issues.values() if i["open"]]
    assert open_stages == ["stage:build"]
