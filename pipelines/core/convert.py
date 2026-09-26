from __future__ import annotations

import hashlib
import io
import zipfile
from collections.abc import Mapping, Sequence
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


def _csv_members(zf: zipfile.ZipFile) -> dict[str, str]:
    """Map CSV base name -> zip member name, ignoring folders, macOS cruft and non-CSVs."""
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
        if base in members:
            raise InventoryError(f"duplicate CSV in zip: {base}")
        members[base] = info.filename
    return members


def inventory(names: Sequence[str], config: SourceConfig) -> None:
    expected, found = set(config.tables), set(names)
    if missing := sorted(expected - found):
        raise InventoryError(f"missing expected tables in zip: {', '.join(missing)}")
    if extra := sorted(found - expected):
        raise InventoryError(
            f"new upstream table(s): {', '.join(extra)}: add to tables.py/columns.py and the schema"
        )


def _read_csv(raw: bytes, table: str) -> pl.DataFrame:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as e:
        raise ConvertError(f"{table}: not valid UTF-8: {e}") from e
    text = text.removeprefix(BOM)
    df = pl.read_csv(io.BytesIO(text.encode("utf-8")), infer_schema=False, encoding="utf8")
    return df.with_columns(
        [pl.when(pl.col(c) == "").then(None).otherwise(pl.col(c)).alias(c) for c in df.columns]
    )


def _rename(df: pl.DataFrame, table: str, mapping: Mapping[str, str]) -> pl.DataFrame:
    cols = set(df.columns)
    if unmapped := sorted(cols - set(mapping)):
        raise ConvertError(f"{table}: unmapped column(s) {unmapped}: add to columns.py")
    if absent := sorted(set(mapping) - cols):
        raise ConvertError(f"{table}: column(s) {absent} missing from the CSV")
    return df.rename(dict(mapping))


def _cast_expr(col: str, dtype: pl.DataType, strict: bool) -> pl.Expr:
    if dtype == pl.Date:
        return pl.col(col).str.to_date("%Y-%m-%d", strict=strict)
    return pl.col(col).cast(dtype, strict=strict)


def _cast(df: pl.DataFrame, table: str, dtypes: Mapping[str, pl.DataType]) -> pl.DataFrame:
    problems: list[str] = []
    for col, dtype in dtypes.items():
        if dtype == pl.Utf8:
            continue
        lax = df.select(_cast_expr(col, dtype, strict=False)).to_series()
        bad = df.filter(lax.is_null() & df[col].is_not_null())[col]
        if len(bad):
            sample = bad.head(SAMPLE_BAD_VALUES).to_list()
            problems.append(f"{table}.{col} -> {dtype}: {len(bad)} bad value(s), e.g. {sample}")
    if problems:
        raise CastError("cast failures:\n" + "\n".join(problems))
    return df.with_columns([_cast_expr(c, t, strict=True) for c, t in dtypes.items()]).select(
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
) -> dict[str, pl.DataFrame]:
    """Inventory, read (UTF-8, BOM stripped, all Utf8), rename, cast strictly, sort by key."""
    with zipfile.ZipFile(zip_path) as zf:
        members = _csv_members(zf)
        inventory(list(members), config)
        tables: dict[str, pl.DataFrame] = {}
        for csv_name, member in sorted(members.items()):
            table = config.tables[csv_name]
            df = _rename(_read_csv(zf.read(member), table), table, config.columns[table])
            tables[table] = sort_frame(_cast(df, table, dtypes[table]), primary_keys[table])
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
