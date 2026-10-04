import re

from pipelines.core.config import PartitionConfig, SourceConfig


def make_config(**overrides: object) -> SourceConfig:
    defaults: dict[str, object] = dict(
        name="x",
        page_url="https://example.test",
        version_pattern=re.compile(r"v(\d+)"),
        download_urls=("https://example.test/x.zip",),
        tables={"A.csv": "a"},
        columns={"a": {"col": "col"}},
        license="MIT",
        attribution="x",
    )
    return SourceConfig(**{**defaults, **overrides})  # type: ignore[arg-type]


def test_partitions_defaults_to_none_like_lahman():
    config = make_config()
    assert config.partitions is None


def test_source_config_accepts_a_partition_config():
    partitions = PartitionConfig(
        discover=lambda session: ["2024", "2025"],
        url=lambda key: f"https://example.test/{key}.zip",
        partitioned_tables=frozenset({"plays"}),
    )
    config = make_config(partitions=partitions)
    assert config.partitions is partitions
    assert partitions.discover(None) == ["2024", "2025"]
    assert partitions.url("2025") == "https://example.test/2025.zip"
    assert partitions.partitioned_tables == {"plays"}


def test_partition_config_is_frozen_and_comparable():
    a = PartitionConfig(discover=lambda s: [], url=lambda k: "", partitioned_tables=frozenset())
    assert a.partitioned_tables == frozenset()
