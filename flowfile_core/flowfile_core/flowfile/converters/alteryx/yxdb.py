"""Convert Alteryx ``.yxdb`` files into Parquet (or CSV).

Reading is delegated to the ``yxdb`` package (PyPI ``yxdb`` 1.1.1, MIT, by Tom Larsen), an
optional dependency installed with ``pip install 'flowfile[alteryx]'``.

Why this is a separate step: the importer never parses ``.yxdb`` itself. An uploaded ``.yxmd``
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
"""

from __future__ import annotations

import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any

import polars as pl

__all__ = ["ConversionStats", "convert_tree", "convert_yxdb", "read_yxdb"]

INSTALL_HINT = "Reading .yxdb files needs the optional dependency: pip install 'flowfile[alteryx]'"
SPATIAL_SUFFIX = "__spatial"

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

    Raises:
        RuntimeError: the optional ``yxdb`` dependency is missing, or the file cannot be read.
    """
    src = Path(src)
    suffix = ".csv" if csv else ".parquet"
    dst = Path(dst) if dst is not None else src.with_suffix(suffix)
    if dst.exists() and not overwrite:
        return ConversionStats(source=src, destination=dst, skipped=True)

    started = time.perf_counter()
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
    """Read a ``.yxdb`` into a Polars frame, plus the warnings raised while mapping dtypes."""
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


def _diagnose(src: Path) -> str:
    """Name the real reason a file failed to open — YxdbReader reports every cause identically."""
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
