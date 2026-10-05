from pathlib import Path

import pytest

from pipelines.core.config import PartitionConfig
from pipelines.core.partitions import (
    PartitionCheck,
    check_all_partitions,
    check_partition,
    decide_partitions,
    download_partition,
)

UA = "fishbaseball-data/test"


class HeadResp:
    def __init__(self, status: int, etag: str | None = None):
        self.status_code = status
        self.headers = {"ETag": etag} if etag else {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeSession:
    """head_routes: url -> list of HeadResp, consumed in order (or a single HeadResp)."""

    def __init__(self, head_routes=None, get_routes=None):
        self.head_routes = {
            k: (v if isinstance(v, list) else [v]) for k, v in (head_routes or {}).items()
        }
        self.get_routes = get_routes or {}
        self.head_calls: list[tuple[str, dict]] = []

    def head(self, url, *, headers=None, timeout=None, allow_redirects=True):
        self.head_calls.append((url, dict(headers or {})))
        return self.head_routes[url].pop(0)

    def get(self, url, **kwargs):
        return self.get_routes[url]


# ---- check_partition ------------------------------------------------------------------


def test_no_previous_etag_is_reported_changed():
    s = FakeSession(head_routes={"https://x/2024.zip": HeadResp(200, etag='"a"')})
    check = check_partition(
        "https://x/2024.zip", "2024", session=s, previous_etag=None, user_agent=UA
    )
    assert check == PartitionCheck(key="2024", changed=True, etag='"a"')


def test_304_is_unchanged():
    s = FakeSession(head_routes={"https://x/2024.zip": HeadResp(304)})
    check = check_partition(
        "https://x/2024.zip", "2024", session=s, previous_etag='"a"', user_agent=UA
    )
    assert check.changed is False and check.etag == '"a"'
    assert s.head_calls[0][1]["If-None-Match"] == '"a"'


def test_200_with_same_etag_is_unchanged():
    # a server that doesn't honor conditional HEAD but still reports a stable ETag
    s = FakeSession(head_routes={"https://x/2024.zip": HeadResp(200, etag='"a"')})
    check = check_partition(
        "https://x/2024.zip", "2024", session=s, previous_etag='"a"', user_agent=UA
    )
    assert check.changed is False and check.etag == '"a"'


def test_200_with_different_etag_is_changed():
    s = FakeSession(head_routes={"https://x/2024.zip": HeadResp(200, etag='"b"')})
    check = check_partition(
        "https://x/2024.zip", "2024", session=s, previous_etag='"a"', user_agent=UA
    )
    assert check.changed is True and check.etag == '"b"'


def test_no_etag_in_response_at_all_is_changed():
    s = FakeSession(head_routes={"https://x/2024.zip": HeadResp(200)})
    check = check_partition(
        "https://x/2024.zip", "2024", session=s, previous_etag='"a"', user_agent=UA
    )
    assert check.changed is True and check.etag is None


def test_sends_user_agent():
    s = FakeSession(head_routes={"https://x/2024.zip": HeadResp(200, etag='"a"')})
    check_partition("https://x/2024.zip", "2024", session=s, previous_etag=None, user_agent=UA)
    assert s.head_calls[0][1]["User-Agent"] == UA


def test_http_error_raises():
    s = FakeSession(head_routes={"https://x/2024.zip": HeadResp(500)})
    with pytest.raises(RuntimeError):
        check_partition("https://x/2024.zip", "2024", session=s, previous_etag=None, user_agent=UA)


# ---- download_partition -----------------------------------------------------------------


def test_download_partition_returns_path_and_hash(tmp_path: Path, small_zip: Path):
    class DlSession:
        def get(self, url, *, stream, timeout, headers):
            class R:
                def raise_for_status(self):
                    pass

                def iter_content(self, chunk_size):
                    yield small_zip.read_bytes()

                def close(self):
                    pass

            return R()

    path, digest = download_partition(
        "https://x/2024.zip", "2024", out_dir=tmp_path, session=DlSession(), user_agent=UA
    )
    assert path.read_bytes() == small_zip.read_bytes()
    import hashlib

    assert digest == hashlib.sha256(small_zip.read_bytes()).hexdigest()


# ---- check_all_partitions ----------------------------------------------------------------


def make_partitions(keys):
    return PartitionConfig(
        discover=lambda session: keys,
        url=lambda key: f"https://x/{key}.zip",
        partitioned_tables=frozenset({"plays"}),
        convert=lambda zip_path, key: {},
    )


class StubConfig:
    def __init__(self, partitions):
        self.partitions = partitions


def test_check_all_partitions_checks_every_discovered_key():
    partitions = make_partitions(["2023", "2024"])
    s = FakeSession(
        head_routes={
            "https://x/2023.zip": HeadResp(304),
            "https://x/2024.zip": HeadResp(200, etag='"new"'),
        }
    )
    results = check_all_partitions(
        StubConfig(partitions),
        session=s,
        previous_upstream_seen={"2023": {"etag": '"a"'}},
        user_agent=UA,
    )
    assert results["2023"].changed is False
    assert results["2024"].changed is True


def test_check_all_partitions_only_restricts_to_given_keys():
    partitions = make_partitions(["2023", "2024", "2025"])
    s = FakeSession(head_routes={"https://x/2024.zip": HeadResp(200, etag='"a"')})
    results = check_all_partitions(
        StubConfig(partitions),
        session=s,
        previous_upstream_seen=None,
        user_agent=UA,
        only=["2024"],
    )
    assert set(results) == {"2024"}


def test_check_all_partitions_unknown_only_key_raises():
    partitions = make_partitions(["2024"])
    with pytest.raises(ValueError, match="2099"):
        check_all_partitions(
            StubConfig(partitions),
            session=FakeSession(),
            previous_upstream_seen=None,
            user_agent=UA,
            only=["2099"],
        )


def test_check_all_partitions_with_no_prior_history_is_all_changed():
    partitions = make_partitions(["2024"])
    s = FakeSession(head_routes={"https://x/2024.zip": HeadResp(200, etag='"a"')})
    results = check_all_partitions(
        StubConfig(partitions), session=s, previous_upstream_seen=None, user_agent=UA
    )
    assert results["2024"].changed is True


def test_check_all_partitions_paces_requests_with_delay(monkeypatch):
    # A small, volunteer-run site shouldn't see ~129 requests land back-to-back: pace them,
    # one sleep *between* each pair of requests, none before the first.
    partitions = make_partitions(["2023", "2024", "2025"])
    s = FakeSession(
        head_routes={
            "https://x/2023.zip": HeadResp(200, etag='"a"'),
            "https://x/2024.zip": HeadResp(200, etag='"b"'),
            "https://x/2025.zip": HeadResp(200, etag='"c"'),
        }
    )
    sleeps: list[float] = []
    monkeypatch.setattr("pipelines.core.partitions.time.sleep", sleeps.append)
    check_all_partitions(
        StubConfig(partitions), session=s, previous_upstream_seen=None, user_agent=UA, delay=0.5
    )
    assert sleeps == [0.5, 0.5]


def test_check_all_partitions_default_delay_is_zero(monkeypatch):
    partitions = make_partitions(["2023", "2024"])
    s = FakeSession(
        head_routes={
            "https://x/2023.zip": HeadResp(200, etag='"a"'),
            "https://x/2024.zip": HeadResp(200, etag='"b"'),
        }
    )
    sleeps: list[float] = []
    monkeypatch.setattr("pipelines.core.partitions.time.sleep", sleeps.append)
    check_all_partitions(
        StubConfig(partitions), session=s, previous_upstream_seen=None, user_agent=UA
    )
    assert sleeps == []


# ---- decide_partitions -------------------------------------------------------------------


def test_decide_partitions_first_release_publishes():
    d = decide_partitions({"2024": PartitionCheck("2024", True, '"a"')}, first_release=True)
    assert d.release is True and d.reason == "first_release" and d.changed_seasons == ["2024"]


def test_decide_partitions_some_changed_publishes():
    checks = {
        "2023": PartitionCheck("2023", False, '"a"'),
        "2024": PartitionCheck("2024", True, '"b"'),
    }
    d = decide_partitions(checks, first_release=False)
    assert d.release is True and d.reason == "content_changed" and d.changed_seasons == ["2024"]


def test_decide_partitions_nothing_changed_does_not_publish():
    checks = {
        "2023": PartitionCheck("2023", False, '"a"'),
        "2024": PartitionCheck("2024", False, '"b"'),
    }
    d = decide_partitions(checks, first_release=False)
    assert d.release is False and d.reason == "content_unchanged" and d.changed_seasons == []


def test_decide_partitions_forced_with_nothing_changed_still_publishes():
    checks = {"2024": PartitionCheck("2024", False, '"a"')}
    d = decide_partitions(checks, first_release=False, force=True)
    assert d.release is True and d.reason == "forced" and d.changed_seasons == []


def test_decide_partitions_forced_with_real_changes_reports_content_changed():
    checks = {"2024": PartitionCheck("2024", True, '"a"')}
    d = decide_partitions(checks, first_release=False, force=True)
    assert d.release is True and d.reason == "content_changed"


def test_decide_partitions_changed_seasons_are_sorted():
    checks = {
        "2025": PartitionCheck("2025", True, '"a"'),
        "1911": PartitionCheck("1911", True, '"b"'),
        "2001": PartitionCheck("2001", False, '"c"'),
    }
    d = decide_partitions(checks, first_release=False)
    assert d.changed_seasons == ["1911", "2025"]
