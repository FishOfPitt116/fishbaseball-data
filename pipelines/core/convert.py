from __future__ import annotations

import hashlib
import io
import zipfile
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path

import polars as pl

from pipelines.core.config import SourceConfig

BOM = "﻿"
SAMPLE_BAD_VALUES = 5


class ConvertError(Exception):
    pass


class InventoryError(ConvertError):
    pass


class CastError(ConvertError):
    pass


def _csv_members(
    zf: zipfile.ZipFile, normalize: Callable[[str], str] | None = None
) -> dict[str, str]:
    """Map CSV name (after `normalize`, if given) -> zip member path, ignoring folders,
    macOS cruft and non-CSVs. `normalize` lets a source whose per-partition zips prefix every
    file (Retrosheet: the season, e.g. "2024batting.csv") match the same `config.tables` keys
    every partition; two raw names that normalize to the same name are a hard failure, not a
    silent overwrite."""
    members: dict[str, str] = {}
    for info in zf.infolist():
        parts = info.filename.split("/")
        base = parts[-1]
        if (
            info.is_dir()
            or "__MACOSX" in parts
            or base.startswith(".")
            or not base.lower().endswith(".csv")
        ):
            continue
        name = normalize(base) if normalize else base
        if name in members:
            raise InventoryError(f"duplicate CSV in zip (as {name!r}): {base}")
        members[name] = info.filename
    return members


def inventory(names: Sequence[str], config: SourceConfig) -> None:
    expected, found = set(config.tables), set(names)
    if missing := sorted(expected - found):
        raise InventoryError(f"missing expected tables in zip: {', '.join(missing)}")
    if extra := sorted(found - expected):
        raise InventoryError(
            f"new upstream table(s): {', '.join(extra)}: add to tables.py/columns.py and the schema"
        )


def _read_csv(raw: bytes, table: str, null_markers: frozenset[str]) -> pl.DataFrame:
    """Incidental leading/trailing whitespace (confirmed real: Retrosheet's 1976 `game_info`
    has a `windspeed` of "17 " for one game) is stripped before null-marker/cast handling —
    it's never semantically significant in this upstream data, only ever a formatting slip."""
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as e:
        raise ConvertError(f"{table}: not valid UTF-8: {e}") from e
    text = text.removeprefix(BOM)
    df = pl.read_csv(io.BytesIO(text.encode("utf-8")), infer_schema=False, encoding="utf8")
    df = df.with_columns([pl.col(c).str.strip_chars() for c in df.columns])
    return df.with_columns(
        [
            pl.when(pl.col(c).is_in(list(null_markers))).then(None).otherwise(pl.col(c)).alias(c)
            for c in df.columns
        ]
    )


def _rename(
    df: pl.DataFrame, table: str, mapping: Mapping[str, str], *, allow_missing: bool = False
) -> pl.DataFrame:
    cols = set(df.columns)
    if unmapped := sorted(cols - set(mapping)):
        raise ConvertError(f"{table}: unmapped column(s) {unmapped}: add to columns.py")
    if absent := sorted(set(mapping) - cols):
        if not allow_missing:
            raise ConvertError(f"{table}: column(s) {absent} missing from the CSV")
        df = df.with_columns([pl.lit(None, dtype=pl.Utf8).alias(c) for c in absent])
    return df.rename(dict(mapping))


def _cast_expr(col: str, dtype: pl.DataType, strict: bool, date_format: str) -> pl.Expr:
    if dtype == pl.Date:
        return pl.col(col).str.to_date(date_format, strict=strict)
    return pl.col(col).cast(dtype, strict=strict)


def _cast(
    df: pl.DataFrame, table: str, dtypes: Mapping[str, pl.DataType], date_format: str
) -> pl.DataFrame:
    problems: list[str] = []
    for col, dtype in dtypes.items():
        if dtype == pl.Utf8:
            continue
        lax = df.select(_cast_expr(col, dtype, False, date_format)).to_series()
        bad = df.filter(lax.is_null() & df[col].is_not_null())[col]
        if len(bad):
            sample = bad.head(SAMPLE_BAD_VALUES).to_list()
            problems.append(f"{table}.{col} -> {dtype}: {len(bad)} bad value(s), e.g. {sample}")
    if problems:
        raise CastError("cast failures:\n" + "\n".join(problems))
    return df.with_columns([_cast_expr(c, t, True, date_format) for c, t in dtypes.items()]).select(
        list(dtypes)
    )


def sort_frame(df: pl.DataFrame, primary_key: Sequence[str]) -> pl.DataFrame:
    """Sort by primary key, then remaining columns, so output is fully deterministic."""
    rest = [c for c in df.columns if c not in primary_key]
    return df.sort([*primary_key, *rest], nulls_last=True)


def read_tables(
    zip_path: Path,
    config: SourceConfig,
    dtypes: Mapping[str, Mapping[str, pl.DataType]],
    primary_keys: Mapping[str, Sequence[str]],
    *,
    normalize_member_name: Callable[[str], str] | None = None,
    date_format: str = "%Y-%m-%d",
    null_markers: frozenset[str] = frozenset({""}),
    allow_missing_columns: bool = False,
) -> dict[str, pl.DataFrame]:
    """Inventory, read (UTF-8, BOM stripped, all Utf8), rename, cast strictly, sort by key.
    `normalize_member_name` is for sources with per-partition-prefixed CSV names; see
    `_csv_members`. `date_format` is Lahman's hyphenated form by default; Retrosheet's `date`
    columns are `YYYYMMDD` and pass `date_format="%Y%m%d"`. `null_markers` is which raw string
    values become null before casting; Lahman only has blank, Retrosheet also uses `"?"`.
    `allow_missing_columns` null-fills a column the schema expects but one partition's CSV
    lacks (confirmed real: Retrosheet's 1899 `plays.csv` is missing 16 columns every other
    season has) instead of raising — an unexpected *extra* column is always still an error.
    Lahman passes none of these, so its behavior is unchanged."""
    with zipfile.ZipFile(zip_path) as zf:
        members = _csv_members(zf, normalize_member_name)
        inventory(list(members), config)
        tables: dict[str, pl.DataFrame] = {}
        for csv_name, member in sorted(members.items()):
            table = config.tables[csv_name]
            df = _rename(
                _read_csv(zf.read(member), table, null_markers), table, config.columns[table],
                allow_missing=allow_missing_columns,
            )  # fmt: skip
            tables[table] = sort_frame(
                _cast(df, table, dtypes[table], date_format), primary_keys[table]
            )
    return tables


def write_parquet(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path, compression="zstd", statistics=True)


def content_hash(df: pl.DataFrame, primary_key: Sequence[str]) -> str:
    """SHA-256 of a canonical CSV rendering (sorted, UTF-8, LF, no index). Independent of the
    Parquet writer version, so it can be compared across releases."""
    csv = sort_frame(df, primary_key).write_csv(
        separator=",",
        line_terminator="\n",
        include_header=True,
        null_value="",
        float_scientific=False,
    )
    return hashlib.sha256(csv.encode("utf-8")).hexdigest()
