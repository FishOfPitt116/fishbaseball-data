import re

from pipelines.core.config import SourceConfig, SourceSchema
from pipelines.lahman.columns import COLUMNS
from pipelines.lahman.schema import DTYPES, PRIMARY_KEYS, SCHEMA_VERSION, YEAR_COLUMNS, validate
from pipelines.lahman.tables import TABLES

# SABR publishes the CSVs as a Box shared folder (the "Comma-delimited version" link on the page);
# pipelines.core.box zips it. If SABR moves the folder, update this and add a fixture (see runbook).
LAHMAN = SourceConfig(
    name="lahman",
    page_url="https://sabr.org/lahman-database/",
    version_pattern=re.compile(r"Version (\d{4}), released ([A-Z][a-z]+ \d{1,2}, \d{4})"),
    download_urls=("https://sabr.box.com/s/y1prhc795jk8zvmelfd3jq7tl389y6cd",),
    tables=TABLES,
    columns=COLUMNS,
    license="CC BY-SA 3.0",
    attribution="Lahman Baseball Database © SABR, via Sean Lahman. CC BY-SA 3.0.",
    extra_notice=(
        "See http://creativecommons.org/licenses/by-sa/3.0/ for the full CC BY-SA 3.0 text. "
        "Negro Leagues data is licensed by SABR from Seamheads.com."
    ),
)

LAHMAN_SCHEMA = SourceSchema(
    version=SCHEMA_VERSION,
    dtypes=DTYPES,
    primary_keys=PRIMARY_KEYS,
    year_columns=YEAR_COLUMNS,
    validate=validate,
)
