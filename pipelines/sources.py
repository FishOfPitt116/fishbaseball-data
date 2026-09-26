from pipelines.core.config import SourceConfig, SourceSchema
from pipelines.lahman import LAHMAN, LAHMAN_SCHEMA

SOURCES: dict[str, tuple[SourceConfig, SourceSchema]] = {"lahman": (LAHMAN, LAHMAN_SCHEMA)}
