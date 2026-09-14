"""Excel -> Polars-DataFrame reader, the single excel read path for core and the worker.

Three engines back the Read node, picked by :func:`select_excel_engine` from the node's settings:
``calamine`` (fastexcel) is the fast default, ``openpyxl`` is the permissive one that also does type
inference, and ``xlsx2csv`` covers headerless reads (calamine always takes the first row as the header).

All three read the same cells for the same settings: ``start_row``/``start_column`` are 0-based offsets,
``end_row``/``end_column`` are 0-based inclusive bounds where ``0`` means unbounded, so ``end_row=8`` reads
through Excel row 9. Toggling 'Type inference' swaps the engine and must not change the selection.

The calamine path deliberately relabels its own columns from the header row read via openpyxl:
fastexcel renames any non-string header cell to ``__UNNAMED__n``, which loses real names on sheets
whose headers are dates or numbers (issue #356). The two readers do not agree on width, though --
calamine drops fully-empty columns while openpyxl reports the worksheet's declared used range, which
Excel widens to cover format-only cells -- so the relabel is applied only when the widths match and
falls back to calamine's own names otherwise. Assigning the mismatched list is what used to raise
``ShapeError: N column names provided for a DataFrame of width M`` on sheets with metadata above the
table or a stray value off to the side.

Anything the selected engine still cannot read degrades to openpyxl with a warning rather than
failing the run; that path infers dtypes, which is the same thing the node's 'Type inference' option
selects directly.
"""

from __future__ import annotations

import gc
import logging
import re
from collections.abc import Generator
from typing import Literal

import polars as pl
from openpyxl import Workbook, load_workbook
from openpyxl.worksheet.worksheet import Worksheet

from shared.dtype_utils import create_pl_df_type_save

ExcelEngine = Literal["calamine", "openpyxl", "xlsx2csv"]

FALLBACK_ENGINE: ExcelEngine = "openpyxl"

# Placeholder names polars/fastexcel invent for header cells they cannot use.
_UNNAMED_COLUMN = re.compile(r"^(?:__UNNAMED__|_duplicated_)\d+$")

_logger = logging.getLogger(__name__)


def select_excel_engine(settings) -> ExcelEngine:
    """Pick the reader for a node's Excel settings (``InputExcelTable`` in core and the worker)."""
    if settings.type_inference:
        return "openpyxl"
    if settings.start_column > 0:
        return "openpyxl"
    if not settings.has_headers:
        return "xlsx2csv"
    return "calamine"


def read_excel_table(file_path: str, settings, logger=None) -> pl.DataFrame:
    """Read one sheet into a DataFrame, falling back to openpyxl if the chosen engine cannot.

    Args:
        file_path: Absolute path to the workbook.
        settings: The node's Excel settings (``sheet_name``, ``start_row``, ``end_row``,
            ``start_column``, ``end_column``, ``has_headers``, ``type_inference``).
        logger: Anything with ``.info``/``.warning`` -- a ``NodeLogger`` during a run, so the engine
            choice lands in the flow log the user sees. Defaults to this module's logger.
    """
    log = logger if logger is not None else _logger
    engine = select_excel_engine(settings)
    sheet = settings.sheet_name or "first"
    log.info(f"Reading excel file '{file_path}' (sheet={sheet}) with the {engine} engine")
    try:
        return _read_with_engine(engine, file_path, settings, log)
    except Exception as e:
        # A missing workbook is not an engine-capability problem; retrying only blurs the error.
        if engine == FALLBACK_ENGINE or isinstance(e, FileNotFoundError):
            raise
        log.warning(
            f"The {engine} engine could not read '{file_path}' ({type(e).__name__}: {e}); "
            f"retrying with the more permissive {FALLBACK_ENGINE} engine. That engine infers column "
            "types, which is what the node's 'Type inference' option selects directly."
        )
        return _read_with_engine(FALLBACK_ENGINE, file_path, settings, log)


def _select_columns(df: pl.DataFrame, settings) -> pl.DataFrame:
    """Apply the node's column bounds to a frame an engine read at full width."""
    end_column = settings.end_column if settings.end_column > 0 else df.width
    return df.select(df.columns[settings.start_column : end_column])


def _read_with_engine(engine: ExcelEngine, file_path: str, settings, log) -> pl.DataFrame:
    if engine == "calamine":
        df = df_from_calamine_xlsx(
            file_path=file_path,
            sheet_name=settings.sheet_name,
            start_row=settings.start_row,
            end_row=settings.end_row,
            logger=log,
        )
        return _select_columns(df, settings)

    if engine == "xlsx2csv":
        df = pl.read_excel(
            source=file_path,
            read_options={"skip_rows": settings.start_row},
            engine="xlsx2csv",
            sheet_name=settings.sheet_name,
            has_header=settings.has_headers,
            raise_if_empty=False,
        )
        df = _select_columns(df, settings)
        if settings.end_row > 0:
            # Rows start_row..end_row inclusive, minus the header row polars already consumed.
            df = df.head(max(settings.end_row - settings.start_row + 1 - int(settings.has_headers), 0))
        return df

    return df_from_openpyxl(
        file_path=file_path,
        sheet_name=settings.sheet_name,
        min_row=settings.start_row + 1,
        min_col=settings.start_column + 1,
        max_row=settings.end_row + 1 if settings.end_row > 0 else None,
        max_col=settings.end_column if settings.end_column > 0 else None,
        has_headers=settings.has_headers,
    )


def raw_data_openpyxl(
    file_path: str,
    sheet_name: str = None,
    min_row: int = None,
    max_row: int = None,
    min_col: int = None,
    max_col: int = None,
) -> Generator[list, None, None]:
    workbook: Workbook = load_workbook(file_path, data_only=True, read_only=True)
    try:
        sheet_name = workbook.sheetnames[0] if sheet_name is None else sheet_name
        sheet: Worksheet = workbook[sheet_name]
        yield from sheet.iter_rows(min_row=min_row, max_row=max_row, min_col=min_col, max_col=max_col, values_only=True)
    finally:
        # Callers that only want the header row abandon this generator; without the finally the
        # workbook's zip handle leaks on every calamine read.
        workbook.close()
    del workbook
    gc.collect()


def _read_header_row(file_path: str, sheet_name: str, min_row: int, max_row: int) -> tuple:
    """The first row of the range, with the workbook deterministically closed again."""
    rows = raw_data_openpyxl(file_path, sheet_name, min_row, max_row)
    try:
        return next(rows, ())
    finally:
        rows.close()


def _resolve_calamine_columns(header_row: tuple, df: pl.DataFrame, file_path: str, log) -> list[str]:
    """Name a calamine frame from the sheet's own header row, or from calamine when they disagree.

    An equal width means the two readers saw the same table, so the raw header cells win and
    non-string headers keep their real names. A mismatch means openpyxl's used range covers cells
    calamine dropped (or vice versa on workbooks that declare no dimension), and positions no longer
    correspond -- calamine's own names are then the only ones that line up with the data.
    """
    if len(header_row) == df.width:
        return [f"_unnamed_column_{i}" if value is None else str(value) for i, value in enumerate(header_row)]
    log.warning(
        f"'{file_path}' has a ragged used range: the sheet reports {len(header_row)} header cells but the "
        f"table is {df.width} columns wide (blank or format-only cells outside the table). Using the "
        "column names the reader found in the table itself."
    )
    return [
        f"_unnamed_column_{i}" if not name or _UNNAMED_COLUMN.match(name) else name for i, name in enumerate(df.columns)
    ]


def df_from_calamine_xlsx(
    file_path: str, sheet_name: str, start_row: int = 0, end_row: int = 0, logger=None
) -> pl.DataFrame:
    log = logger if logger is not None else _logger
    read_options = {}
    if start_row > 0:
        read_options["header_row"] = start_row
    if end_row > 0:
        read_options["n_rows"] = end_row - start_row
    df = pl.read_excel(
        source=file_path,
        engine="calamine",
        sheet_name=sheet_name,
        read_options=read_options,
        raise_if_empty=False,
        has_header=True,
    )
    header_row = _read_header_row(file_path, sheet_name, start_row + 1, end_row)
    df.columns = ensure_unique(_resolve_calamine_columns(header_row, df, file_path, log))
    return df


def df_from_openpyxl(
    file_path: str,
    sheet_name: str = None,
    min_row: int = None,
    max_row: int = None,
    min_col: int = None,
    max_col: int = None,
    has_headers: bool = True,
) -> pl.DataFrame:
    data_iterator = raw_data_openpyxl(
        file_path=file_path, sheet_name=sheet_name, min_row=min_row, max_row=max_row, min_col=min_col, max_col=max_col
    )
    raw_data = list(data_iterator)
    if len(raw_data) > 0:
        if has_headers:
            columns = []
            for i, col in enumerate(raw_data[0]):
                if col is None:
                    col = f"_unnamed_column_{i}"
                elif not isinstance(col, str):
                    col = str(col)
                columns.append(col)
            columns = ensure_unique(columns)
            df = create_pl_df_type_save(raw_data[1:])
            renames = {o: n for o, n in zip(df.columns, columns, strict=False)}
            df = df.rename(renames)

        else:
            df = create_pl_df_type_save(raw_data)
        return df
    else:
        return pl.DataFrame()


def ensure_unique(lst: list[str]) -> list[str]:
    """
    Ensures that all elements in the input list are unique by appending
    a version number (e.g., '_v1') to duplicates. It continues adding
    version numbers until all items in the list are unique.

    Args:
        lst (List[str]): A list of strings that may contain duplicates.

    Returns:
        List[str]: A new list where all elements are unique.
    """
    seen = {}
    result = []

    for item in lst:
        if item in seen:
            # Advance this item's own counter until free: bumping the candidate's instead
            # regenerates the same name forever (e.g. ["a", "a_v2", "a"]).
            seen[item] += 1
            new_item = f"{item}_v{seen[item]}"
            while new_item in seen:
                seen[item] += 1
                new_item = f"{item}_v{seen[item]}"
            result.append(new_item)
            seen[new_item] = 1
        else:
            result.append(item)
            seen[item] = 1

    return result
