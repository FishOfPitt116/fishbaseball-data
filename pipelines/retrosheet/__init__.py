import re

from pipelines.core.config import PartitionConfig, SourceConfig, SourceSchema
from pipelines.retrosheet.columns import COLUMNS
from pipelines.retrosheet.schema import (
    DTYPES,
    PRIMARY_KEYS,
    SCHEMA_VERSION,
    SEASON_COLUMNS,
    validate,
)
from pipelines.retrosheet.seasons import discover_seasons, season_zip_url
from pipelines.retrosheet.tables import TABLES

# R4: plays/batting/pitching/fielding/team_stats are big enough to publish one Parquet file
# per season, reusing unchanged seasons from an older release (see pipelines.core.partitions).
# game_info and all_players are small enough (~226k and ~131k rows across all of history) to
# stay single files, rebuilt in full each release — the same shape as every Lahman table.
PARTITIONED_TABLES = frozenset({"plays", "batting", "pitching", "fielding", "team_stats"})

PARTITIONS = PartitionConfig(
    discover=discover_seasons, url=season_zip_url, partitioned_tables=PARTITIONED_TABLES
)

RETROSHEET = SourceConfig(
    name="retrosheet",
    page_url="https://www.retrosheet.org/",
    # Retrosheet has no single "version" page-text pattern like Lahman's; its "version" is
    # effectively the season range it currently covers, tracked per partition instead (see
    # the companion design doc, section 1: releases revise past seasons, not just add new
    # ones, so there's no one upstream version string to parse).
    version_pattern=re.compile(r"$never-matches^"),
    download_urls=(),
    tables=TABLES,
    columns=COLUMNS,
    license="See NOTICE (Retrosheet's own notice, not a named license)",
    attribution=(
        "The information used here was obtained free of charge from and is copyrighted by "
        'Retrosheet. Interested parties may contact Retrosheet at "www.retrosheet.org".'
    ),
    partitions=PARTITIONS,
)

RETROSHEET_SCHEMA = SourceSchema(
    version=SCHEMA_VERSION,
    dtypes=DTYPES,
    primary_keys=PRIMARY_KEYS,
    year_columns=SEASON_COLUMNS,
    validate=validate,
)
