import re

from pipelines.core.config import SourceConfig, SourceSchema
from pipelines.lahman.columns import COLUMNS
from pipelines.lahman.schema import DTYPES, PRIMARY_KEYS, SCHEMA_VERSION, YEAR_COLUMNS, validate
from pipelines.lahman.tables import TABLES

# Box does not expose stable direct-download URLs for the CSV folder (see docs, open question 1),
# so there are no automatic download URLs yet; use --source-url with a zip of the CSVs.
LAHMAN = SourceConfig(
    name="lahman",
    page_url="https://sabr.org/lahman-database/",
    version_pattern=re.compile(r"Version (\d{4}), released ([A-Z][a-z]+ \d{1,2}, \d{4})"),
    download_urls=(),
    tables=TABLES,
    columns=COLUMNS,
    license="CC BY-SA 3.0",
    attribution="Lahman Baseball Database © SABR, via Sean Lahman. CC BY-SA 3.0.",
)

LAHMAN_SCHEMA = SourceSchema(
    version=SCHEMA_VERSION,
    dtypes=DTYPES,
    primary_keys=PRIMARY_KEYS,
    year_columns=YEAR_COLUMNS,
    validate=validate,
)
