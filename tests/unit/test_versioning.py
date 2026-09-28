from datetime import date

import pytest

from pipelines.core.versioning import UpstreamRegressionError, decide, next_tag

HASHES = {"batting": "a" * 64, "people": "b" * 64}


def prev(version="2025", hashes=None):
    hashes = hashes or HASHES
    return {"version": version, "tables": {t: {"content_sha256": h} for t, h in hashes.items()}}


def test_no_previous_release_publishes():
    d = decide(None, "2025", HASHES)
    assert d.release is True and d.reason == "first_release"


def test_newer_sabr_version_publishes():
    d = decide(prev("2024"), "2025", HASHES)
    assert d.release is True and d.reason == "new_version"


def test_same_version_changed_content_publishes():
    d = decide(prev(), "2025", {**HASHES, "batting": "c" * 64})
    assert d.release is True and d.reason == "content_changed"


def test_same_version_added_table_publishes():
    d = decide(prev(), "2025", {**HASHES, "salaries": "d" * 64})
    assert d.release is True and d.reason == "content_changed"


def test_same_version_equal_hashes_does_not_publish():
    d = decide(prev(), "2025", dict(HASHES))
    assert d.release is False and d.reason == "content_unchanged"


def test_older_version_fails():
    with pytest.raises(UpstreamRegressionError):
        decide(prev("2026"), "2025", HASHES)


def test_force_publishes_even_with_identical_hashes():
    d = decide(prev(), "2025", dict(HASHES), force=True)
    assert d.release is True and d.reason == "forced"


def test_force_does_not_relabel_a_real_content_change():
    d = decide(prev(), "2025", {**HASHES, "batting": "c" * 64}, force=True)
    assert d.release is True and d.reason == "content_changed"


def test_force_does_not_override_an_upstream_regression():
    with pytest.raises(UpstreamRegressionError):
        decide(prev("2026"), "2025", HASHES, force=True)


def test_tag_is_publish_date():
    assert next_tag("lahman", date(2026, 10, 2), []) == "lahman-2026-10-02"


def test_same_day_second_release_gets_suffix():
    assert next_tag("lahman", date(2026, 10, 2), ["lahman-2026-10-02"]) == "lahman-2026-10-02-2"
    taken = ["lahman-2026-10-02", "lahman-2026-10-02-2"]
    assert next_tag("lahman", date(2026, 10, 2), taken) == "lahman-2026-10-02-3"


def test_other_days_and_sources_do_not_collide():
    assert next_tag(
        "lahman", date(2026, 10, 2), ["lahman-2026-10-01", "retrosheet-2026-10-02"]
    ) == ("lahman-2026-10-02")
