"""`--delay` plumbing through the CLI: `python -m pipelines <source> detect|all --delay N`
reaches `stage_detect`/`run_all` as a float; omitted, it stays `None` (each stage's own
partitioned-only default then applies)."""

import pytest

from pipelines.__main__ import main


@pytest.fixture
def captured(monkeypatch: pytest.MonkeyPatch) -> dict:
    calls: dict = {}

    def fake_run_all(*args, **kwargs):
        calls.update(kwargs)
        return {"status": "no_change", "reason": "x"}

    def fake_stage_detect(*args, **kwargs):
        calls.update(kwargs)
        return {"changed": False, "reason": "x"}

    monkeypatch.setattr("pipelines.__main__.run_all", fake_run_all)
    monkeypatch.setattr("pipelines.__main__.stage_detect", fake_stage_detect)
    return calls


def test_delay_flag_is_forwarded_on_all(captured):
    rc = main(["retrosheet", "all", "--delay", "1.5"], session=object(), client=object())
    assert rc == 0
    assert captured["delay"] == 1.5


def test_delay_flag_is_forwarded_on_detect(captured):
    rc = main(["retrosheet", "detect", "--delay", "2"], session=object(), client=object())
    assert rc == 0
    assert captured["delay"] == 2.0


def test_delay_defaults_to_none(captured):
    main(["retrosheet", "all"], session=object(), client=object())
    assert captured["delay"] is None
