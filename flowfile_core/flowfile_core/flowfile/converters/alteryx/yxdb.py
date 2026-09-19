"""Convert Alteryx ``.yxdb`` files into Parquet (or CSV).

Reading is delegated to the ``yxdb`` package (PyPI ``yxdb`` 1.1.1, MIT, by Tom Larsen,
https://github.com/tlarsendataguy-yxdb/yxdb-py), an optional dependency installed with
``pip install 'flowfile[yxdb]'`` (``flowfile[alteryx]`` remains as an alias) — except for
AMP-engine files, which that package cannot read; see the AMP section at the end of this docstring.

Why this is a separate step: the importer never decodes ``.yxdb`` data itself. An uploaded ``.yxmd``
carries paths, not data, so conversion runs on the user's own machine via
``flowfile convert yxdb``; the Input Data mapper then points at the Parquet sibling that this
module writes next to the source (``<stem>.parquet``).

Dtype table — widest-safe, so no Alteryx value can overflow or silently round:

==============  =====================  ==================================================
Alteryx         Polars                 Note
==============  =====================  ==================================================
Bool            Boolean
Byte            Int32                  widened
Int16           Int32                  widened
Int32           Int32
Int64           Int64
Float           Float32
Double          Float64
FixedDecimal    Decimal(size, scale)    falls back to Float64 when Polars rejects the
                                       precision (Alteryx allows ``size`` > 38)
String          String                 latin-1 on disk
WString         String
V_String        String                 latin-1 on disk
V_WString       String                 utf-16-le on disk
Date            Date
DateTime        Datetime("us")
Blob            Binary
SpatialObj      Binary                 column renamed ``<field>__spatial``, with a warning
==============  =====================  ==================================================

Nulls stay null; a file holding zero records yields an empty frame with the full schema.
Alteryx ``Time`` fields cannot be read at all — ``yxdb`` 1.1.1 has no extractor for them, and
a file containing one fails with that reason rather than a generic format error.

AMP-engine files (the ``Alteryx e2 Database file`` container, written by default in recent
Designer versions) are a *different layout* that ``yxdb`` cannot read at all. Those, and only
those, are handed to ``sigilyx`` (PyPI ``sigilyx`` 0.4.0, Apache-2.0, Rust core) from a second
optional extra, ``pip install 'flowfile[alteryx-amp]'``. That reader is **experimental and has
been measured dropping records**: on a mixed fixed-width E2 file (String/Int16/Int32/Double,
24 columns, 10 000 records) it returned 10 000 rows of which 9 918 were entirely null, with no
error and no warning, while ``record_count`` still said 10 000. So every E2 conversion is
guarded: if a single fully-null row comes back, the conversion is **refused** and no Parquet is
written — see :func:`_dropped_records_message`. E2 reading streams in batches, which is also
why ``allow_unverified_e2_types`` is not used: it exists only on sigilyx's eager readers, and
on the file above it changed nothing.
"""

from __future__ import annotations

import time
import xml.etree.ElementTree as ET
from collections.abc import Iterator
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any

import polars as pl

__all__ = ["ConversionStats", "convert_tree", "convert_yxdb", "read_yxdb"]

INSTALL_HINT = "Reading .yxdb files needs the optional dependency: pip install 'flowfile[yxdb]'"
AMP_INSTALL_HINT = (
    "This .yxdb was written by Alteryx's AMP engine ('Alteryx e2 Database file'), a different "
    "layout, and reading it needs a second optional dependency: pip install 'flowfile[alteryx-amp]'"
)
SPATIAL_SUFFIX = "__spatial"
BATCH_ROWS = 65_536

E1_MAGIC = b"Alteryx Database File"
E2_MAGIC = b"Alteryx e2 Database file"

_DTYPES: dict[str, pl.DataType] = {
    "Bool": pl.Boolean(),
    "Byte": pl.Int32(),
    "Int16": pl.Int32(),
    "Int32": pl.Int32(),
    "Int64": pl.Int64(),
    "Float": pl.Float32(),
    "Double": pl.Float64(),
    "String": pl.String(),
    "WString": pl.String(),
    "V_String": pl.String(),
    "V_WString": pl.String(),
    "Date": pl.Date(),
    "DateTime": pl.Datetime("us"),
    "Blob": pl.Binary(),
    "SpatialObj": pl.Binary(),
}
_SUPPORTED_TYPES = frozenset(_DTYPES) | {"FixedDecimal"}

# The AMP reader has its own extractors, including one for Time, and widens nothing.
_E2_DTYPES: dict[str, pl.DataType] = {**_DTYPES, "Time": pl.Time()}
_E2_SUPPORTED_TYPES = frozenset(_E2_DTYPES) | {"FixedDecimal"}
_E2_WIDENED_TYPES = frozenset({"Byte", "Int16"})
_E2_UNVERIFIED_TYPES = frozenset({"Blob", "SpatialObj", "Time", "WString"})
_NARROW_STRING_TYPES = frozenset({"String", "V_String"})
_MAX_DECIMAL_PRECISION = 38
REPLACEMENT_CHAR = "�"


@dataclass
class ConversionStats:
    """What happened to one file: the line the CLI prints and what the tests assert on."""

    source: Path
    destination: Path | None = None
    rows: int = 0
    columns: int = 0
    seconds: float = 0.0
    warnings: list[str] = field(default_factory=list)
    skipped: bool = False
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None


@dataclass(frozen=True)
class _Field:
    name: str
    type: str
    size: int
    scale: int


def convert_yxdb(src: Path, dst: Path | None = None, *, csv: bool = False, overwrite: bool = False) -> ConversionStats:
    """Convert one ``.yxdb`` file to Parquet (or CSV).

    ``dst`` defaults to ``<stem>.parquet`` beside the source — the sibling the Alteryx Input
    Data mapper points at. An existing destination is kept unless ``overwrite`` is set, so a
    run over a large tree is resumable.

    An AMP-engine (E2) file takes the streaming path in :func:`_convert_e2` instead, and is
    refused outright when its reader drops records.

    Raises:
        RuntimeError: an optional dependency is missing, the file cannot be read, or its records
            did not survive the AMP reader.
    """
    src = Path(src)
    suffix = ".csv" if csv else ".parquet"
    dst = Path(dst) if dst is not None else src.with_suffix(suffix)
    if dst.exists() and not overwrite:
        return ConversionStats(source=src, destination=dst, skipped=True)

    started = time.perf_counter()
    if _container(src) == "e2":
        return _convert_e2(src, dst, csv=csv, started=started)
    frame, warnings = read_yxdb(src)
    if csv:
        frame, csv_warnings = _csv_safe(frame)
        warnings.extend(csv_warnings)
    dst.parent.mkdir(parents=True, exist_ok=True)
    if csv:
        frame.write_csv(dst)
    else:
        frame.write_parquet(dst)
    return ConversionStats(src, dst, frame.height, frame.width, time.perf_counter() - started, warnings)


def convert_tree(
    src_dir: Path, dst_dir: Path | None = None, *, csv: bool = False, overwrite: bool = False
) -> list[ConversionStats]:
    """Convert every ``.yxdb`` under ``src_dir``, mirroring the tree under ``dst_dir``.

    A file that cannot be read is recorded on its own :class:`ConversionStats` rather than
    raised, so one unreadable file never stops the walk.
    """
    src_dir = Path(src_dir)
    results: list[ConversionStats] = []
    for path in sorted(src_dir.rglob("*.yxdb")):
        target = None
        if dst_dir is not None:
            target = Path(dst_dir) / path.relative_to(src_dir).with_suffix(".csv" if csv else ".parquet")
        try:
            results.append(convert_yxdb(path, target, csv=csv, overwrite=overwrite))
        except Exception as exc:
            reason = str(exc) if isinstance(exc, RuntimeError) else f"{type(exc).__name__}: {exc}"
            results.append(ConversionStats(source=path, error=reason))
    return results


def read_yxdb(src: Path) -> tuple[pl.DataFrame, list[str]]:
    """Read a ``.yxdb`` into a Polars frame, plus the warnings raised while mapping dtypes.

    An AMP-engine (E2) file goes through :func:`_read_e2`, which refuses a file whose records
    did not survive its reader rather than handing back a frame full of blanks.
    """
    if _container(Path(src)) == "e2":
        return _read_e2(Path(src))
    reader = _open_reader(Path(src))
    try:
        fields = _parse_record_info(reader.meta_info_str)
        columns: list[list[Any]] = [[] for _ in fields]
        while reader.next():
            for index, column in enumerate(columns):
                column.append(reader.read_index(index))
    finally:
        reader.close()

    warnings: list[str] = []
    series = [_build_series(f, values, warnings) for f, values in zip(fields, columns, strict=True)]
    return pl.DataFrame(series), warnings


def _open_reader(src: Path):
    try:
        from yxdb.yxdb_reader import YxdbReader
    except ImportError as exc:
        raise RuntimeError(INSTALL_HINT) from exc
    try:
        return YxdbReader(path=str(src))
    except Exception as exc:
        raise RuntimeError(_diagnose(src)) from exc


def _diagnose(src: Path, exc: Exception | None = None) -> str:
    """Name the real reason a file failed to open — YxdbReader reports every cause identically."""
    if _container(src) == "e2":
        return f"could not read this AMP-engine (E2) .yxdb: {exc}" if exc else "unreadable AMP-engine (E2) .yxdb"
    try:
        with src.open("rb") as handle:
            header = handle.read(512)
            if header[:21] != b"Alteryx Database File":
                return f"not a supported .yxdb (header says {header[:24].decode('latin1').strip()!r})"
            meta = handle.read((int.from_bytes(header[80:84], "little") * 2) - 2).decode("utf_16_le")
        unsupported = sorted({f.type for f in _parse_record_info(meta)} - _SUPPORTED_TYPES)
    except Exception:
        return "unreadable .yxdb header"
    if unsupported:
        return f"field type(s) {', '.join(unsupported)} are not supported by yxdb 1.1.1"
    return "unreadable .yxdb"


def _parse_record_info(meta: str) -> list[_Field]:
    root = ET.fromstring(meta)
    record_info = root if root.tag == "RecordInfo" else root.find("RecordInfo")
    if record_info is None:
        raise RuntimeError("the .yxdb metadata has no RecordInfo")
    return [
        _Field(f.get("name", ""), f.get("type", ""), int(f.get("size") or 0), int(f.get("scale") or 0))
        for f in record_info.iter("Field")
    ]


def _build_series(f: _Field, values: list[Any], warnings: list[str]) -> pl.Series:
    if f.type == "SpatialObj":
        name = f"{f.name}{SPATIAL_SUFFIX}"
        warnings.append(f"'{f.name}' holds Alteryx spatial objects; kept as raw bytes in '{name}'.")
        return pl.Series(name, [_as_bytes(v) for v in values], dtype=pl.Binary())
    if f.type == "Blob":
        return pl.Series(f.name, [_as_bytes(v) for v in values], dtype=pl.Binary())
    if f.type == "FixedDecimal":
        return _decimal_series(f, values, warnings)
    if f.type == "Date":
        return pl.Series(f.name, [None if v is None else v.date() for v in values], dtype=pl.Date())
    dtype = _DTYPES.get(f.type)
    if dtype is None:
        raise RuntimeError(f"Alteryx field type '{f.type}' is not supported (field '{f.name}')")
    return pl.Series(f.name, values, dtype=dtype)


def _decimal_series(f: _Field, values: list[Any], warnings: list[str]) -> pl.Series:
    """yxdb hands FixedDecimal back as a float; re-quantize to the scale Alteryx declared."""
    quantum = Decimal(1).scaleb(-f.scale)
    try:
        return pl.Series(
            f.name,
            [None if v is None else Decimal(str(v)).quantize(quantum) for v in values],
            dtype=pl.Decimal(f.size, f.scale),
        )
    except Exception:
        warnings.append(f"'{f.name}' is a FixedDecimal({f.size},{f.scale}) Polars cannot hold; widened to Float64.")
        return pl.Series(f.name, values, dtype=pl.Float64())


def _as_bytes(value: Any) -> bytes | None:
    return None if value is None else bytes(value)


def _csv_safe(frame: pl.DataFrame) -> tuple[pl.DataFrame, list[str]]:
    """CSV cannot hold raw bytes, so binary columns go out hex-encoded."""
    binary = [name for name, dtype in frame.schema.items() if dtype == pl.Binary]
    if not binary:
        return frame, []
    encoded = frame.with_columns([pl.col(name).bin.encode("hex") for name in binary])
    return encoded, [f"binary column(s) {', '.join(binary)} were hex-encoded for CSV."]


# --- AMP engine (Alteryx e2) ----------------------------------------------------------------
# A different container that `yxdb` cannot open at all. `sigilyx` can, experimentally; the
# guard below is what makes "experimentally" safe to ship.


def _container(src: Path) -> str | None:
    """``e1``, ``e2``, or None when the magic string is neither."""
    try:
        with src.open("rb") as handle:
            header = handle.read(len(E2_MAGIC))
    except OSError:
        return None
    if header.startswith(E2_MAGIC):
        return "e2"
    if header.startswith(E1_MAGIC):
        return "e1"
    return None


def _read_e2(src: Path) -> tuple[pl.DataFrame, list[str]]:
    lazy, warnings, tally = _scan_e2(src)
    try:
        frame = lazy.collect()
    except Exception as exc:
        raise RuntimeError(_diagnose(src, exc)) from exc
    _refuse_dropped_records(tally)
    return frame, warnings + _encoding_warnings(tally)


def _convert_e2(src: Path, dst: Path, *, csv: bool, started: float) -> ConversionStats:
    """Stream an AMP file to disk, then refuse it if its records did not survive the reader.

    The output goes to a ``.part`` file so that a refusal — or a crash mid-stream — leaves no
    sibling behind: a short file would otherwise be skipped as done by the next resumable run.
    """
    lazy, warnings, tally = _scan_e2(src)
    if csv:
        lazy, csv_warnings = _csv_safe_lazy(lazy)
        warnings.extend(csv_warnings)
    dst.parent.mkdir(parents=True, exist_ok=True)
    partial = dst.with_name(f"{dst.name}.part")
    try:
        if csv:
            lazy.sink_csv(partial)
        else:
            lazy.sink_parquet(partial)
    except Exception as exc:
        partial.unlink(missing_ok=True)
        raise RuntimeError(_diagnose(src, exc)) from exc
    try:
        _refuse_dropped_records(tally)
    except RuntimeError:
        partial.unlink(missing_ok=True)
        raise
    partial.replace(dst)
    warnings.extend(_encoding_warnings(tally))
    return ConversionStats(src, dst, tally["rows"], tally["columns"], time.perf_counter() - started, warnings)


def _scan_e2(src: Path) -> tuple[pl.LazyFrame, list[str], dict[str, int]]:
    """A streaming LazyFrame over an AMP file, its dtype warnings, and the tally of what came back.

    The IO plugin declares the schema this module promises rather than the one sigilyx reads, so
    the widening and the ``FixedDecimal`` precision are part of the scan. It also counts what the
    guard needs: rows, fully-null rows, and U+FFFD in single-byte string columns.
    """
    from polars.io.plugins import register_io_source

    sigilyx = _import_sigilyx()
    fields = _e2_fields(src, sigilyx)
    schema, casts, renames, warnings = _e2_plan(fields)
    warnings.extend(_e2_unverified_warnings(fields))
    narrow = [f.name for f in fields if f.type in _NARROW_STRING_TYPES]
    tally = {"rows": 0, "columns": len(fields), "all_null_rows": 0} | dict.fromkeys(narrow, 0)
    path = str(src)

    def source(
        with_columns: list[str] | None,
        predicate: pl.Expr | None,
        n_rows: int | None,
        batch_size: int | None,
    ) -> Iterator[pl.DataFrame]:
        for key in tally:
            if key != "columns":
                tally[key] = 0
        rows_per_batch = min(batch_size or BATCH_ROWS, BATCH_ROWS)
        for batch in sigilyx.read_yxdb_batches(path, rows_per_batch, n_rows=n_rows):
            tally["rows"] += batch.height
            tally["all_null_rows"] += _all_null_rows(batch)
            _count_replacements(batch, narrow, tally)
            if casts:
                batch = batch.with_columns(casts)
            if renames:
                batch = batch.rename(renames)
            if with_columns is not None:
                batch = batch.select(with_columns)
            if predicate is not None:
                batch = batch.filter(predicate)
            yield batch

    return register_io_source(io_source=source, schema=schema), warnings, tally


def _import_sigilyx() -> Any:
    try:
        import sigilyx
    except ImportError as exc:
        raise RuntimeError(AMP_INSTALL_HINT) from exc
    return sigilyx


def _e2_fields(src: Path, sigilyx: Any) -> list[_Field]:
    """Alteryx's own field list, read from the header without decoding a single record."""
    try:
        meta = sigilyx.read_schema(str(src))
    except Exception as exc:
        raise RuntimeError(_diagnose(src, exc)) from exc
    return [
        _Field(f.get("name", ""), f.get("type", ""), int(f.get("size") or 0), int(f.get("scale") or 0)) for f in meta
    ]


def _e2_plan(fields: list[_Field]) -> tuple[dict[str, pl.DataType], list[pl.Expr], dict[str, str], list[str]]:
    """The declared schema, the per-batch casts that produce it, the spatial renames, the warnings."""
    schema: dict[str, pl.DataType] = {}
    casts: list[pl.Expr] = []
    renames: dict[str, str] = {}
    warnings: list[str] = []
    for f in fields:
        if f.type not in _E2_SUPPORTED_TYPES:
            raise RuntimeError(f"Alteryx field type '{f.type}' is not supported (field '{f.name}')")
        name = f.name
        if f.type == "FixedDecimal":
            dtype = _e2_decimal_dtype(f, warnings)
            casts.append(pl.col(f.name).cast(dtype))
        else:
            dtype = _E2_DTYPES[f.type]
            if f.type in _E2_WIDENED_TYPES:
                casts.append(pl.col(f.name).cast(dtype))
            if f.type == "SpatialObj":
                name = f"{f.name}{SPATIAL_SUFFIX}"
                renames[f.name] = name
                warnings.append(f"'{f.name}' holds Alteryx spatial objects; kept as raw bytes in '{name}'.")
        schema[name] = dtype
    return schema, casts, renames, warnings


def _e2_decimal_dtype(f: _Field, warnings: list[str]) -> pl.DataType:
    """Restore the precision Alteryx declared; sigilyx reads its own, narrower one."""
    if 0 < f.size <= _MAX_DECIMAL_PRECISION and 0 <= f.scale <= f.size:
        return pl.Decimal(f.size, f.scale)
    warnings.append(f"'{f.name}' is a FixedDecimal({f.size},{f.scale}) Polars cannot hold; widened to Float64.")
    return pl.Float64()


def _e2_unverified_warnings(fields: list[_Field]) -> list[str]:
    """Four of sigilyx's AMP decoders have never been checked against real files — say which."""
    affected = [f for f in fields if f.type in _E2_UNVERIFIED_TYPES]
    if not affected:
        return []
    types = ", ".join(sorted({f.type for f in affected}))
    columns = ", ".join(f"'{f.name}'" for f in affected)
    return [
        f"this file was written by Alteryx's AMP engine and holds {types} column(s) ({columns}); "
        "sigilyx's AMP decoders for those types are experimental — check the values."
    ]


def _all_null_rows(batch: pl.DataFrame) -> int:
    if batch.height == 0 or batch.width == 0:
        return 0
    return int(batch.select(pl.all_horizontal(pl.all().is_null()).alias("blank"))["blank"].sum())


def _count_replacements(batch: pl.DataFrame, narrow: list[str], tally: dict[str, int]) -> None:
    """Tally U+FFFD in Alteryx's single-byte string fields — see :func:`_encoding_warnings`."""
    if not narrow:
        return
    hits = batch.select(
        pl.col(name).str.contains(REPLACEMENT_CHAR, literal=True).sum().alias(name) for name in narrow
    ).row(0)
    for name, count in zip(narrow, hits, strict=True):
        tally[name] += int(count or 0)


def _refuse_dropped_records(tally: dict[str, int]) -> None:
    """Fail loudly on the failure mode sigilyx's AMP reader has been measured in: blank records."""
    if tally["all_null_rows"]:
        raise RuntimeError(_dropped_records_message(tally))


def _dropped_records_message(tally: dict[str, int]) -> str:
    return (
        f"refused: the AMP-engine (E2) reader returned {tally['rows']} record(s) of which "
        f"{tally['all_null_rows']} are entirely null. sigilyx's E2 support is experimental and has been "
        "measured dropping records on mixed fixed-width layouts, so nothing was written — a file of "
        "blanks would be worse than no file. Export this table to CSV from Alteryx, or write it with "
        "'Use AMP Engine' off, and convert that instead."
    )


def _encoding_warnings(tally: dict[str, int]) -> list[str]:
    """sigilyx decodes Alteryx's narrow String/V_String fields as UTF-8, replacing what is not.

    Alteryx writes those fields in a single-byte encoding, so a byte above 0x7F that is not valid
    UTF-8 — the ``ñ`` in ``La Cañada Flintridge`` — comes back as U+FFFD and cannot be recovered
    from the converted file. The conversion is not refused, but every affected column is named.
    """
    counters = {"rows", "columns", "all_null_rows"}
    damaged = [(name, count) for name, count in tally.items() if name not in counters and count]
    if not damaged:
        return []
    columns = ", ".join(f"'{name}' ({count} value(s))" for name, count in damaged)
    return [
        f"{columns} came back holding U+FFFD: sigilyx reads Alteryx's single-byte String fields as "
        "UTF-8 and replaces bytes that are not, so accented characters are lost. Check those values."
    ]


def _csv_safe_lazy(frame: pl.LazyFrame) -> tuple[pl.LazyFrame, list[str]]:
    """:func:`_csv_safe` for the streaming path; CSV cannot hold raw bytes."""
    binary = [name for name, dtype in frame.collect_schema().items() if dtype == pl.Binary]
    if not binary:
        return frame, []
    encoded = frame.with_columns([pl.col(name).bin.encode("hex") for name in binary])
    return encoded, [f"binary column(s) {', '.join(binary)} were hex-encoded for CSV."]
