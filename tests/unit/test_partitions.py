from pathlib import Path

import pytest

from pipelines.core.config import PartitionConfig
from pipelines.core.partitions import (
    PartitionCheck,
    check_all_partitions,
    check_partition,
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
