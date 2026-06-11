import gc

import polars as pl

from .errors import format_error, format_error_lf
from .log import log_node, logger
from .state import _output_binaries, get_lazyframe, get_schema, store_lazyframe


@log_node
def execute_read_csv(node_id: int, file_content: str, settings: dict) -> dict:
    """Execute read CSV node - creates a LazyFrame"""
    try:
        import io

        table_settings = settings.get("received_file", {}).get("table_settings", {})

        # Source nodes: read into DataFrame first, then convert to lazy
        df = pl.read_csv(
            io.StringIO(file_content),
            has_header=table_settings.get("has_headers", True),
            separator=table_settings.get("delimiter", ","),
            skip_rows=table_settings.get("starting_from_line", 0),
        )
        lf = df.lazy()
        store_lazyframe(node_id, lf)
        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        return {"success": False, "error": format_error("read", node_id, e)}


def _clean_strings_for_export(df: pl.DataFrame) -> pl.DataFrame:
    """Rebuild string-buffer columns through Python values before IPC export.

    The wasm polars build panics ("capacity overflow") when converting
    view-type string/binary columns to the classic layout
    (CompatLevel.oldest) whenever the column's buffers came through Arrow IPC
    import or the excel reader — and the poison survives compute (slices,
    kernels, group_by all keep it). Categorical/Enum carry the same string
    buffers in their rev-map, so they are rebuilt too (dictionary-encoded
    parquet/IPC inputs arrive as Categorical). Numeric columns are
    unaffected. Rebuilt columns get clean buffers; schema and values are
    preserved. Nested string types (List[str], Struct) are not covered.
    """
    string_backed = (pl.String, pl.Binary, pl.Categorical, pl.Enum)
    fixes = [
        pl.Series(name, df[name].to_list(), dtype=dtype)
        for name, dtype in df.schema.items()
        if dtype.base_type() in string_backed
    ]
    return df.with_columns(fixes) if fixes else df


@log_node
def execute_read_ipc(node_id: int, file_content, settings: dict) -> dict:
    """Execute read for Arrow IPC stream bytes - creates a LazyFrame.

    Parquet decodes to IPC on the JS side (parquet-wasm): the wasm polars build
    has no parquet support, but IPC was kept. Format-agnostic — also serves
    Arrow IPC host inputs.
    """
    df = None
    try:
        import io

        bio = io.BytesIO(file_content)
        # IPC bytes are uncompressed; drop the bridge copy before materializing
        # (peak memory = bridge copy + BytesIO copy + DataFrame otherwise)
        del file_content
        df = pl.read_ipc_stream(bio)
        del bio
        store_lazyframe(node_id, df.lazy())
        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        return {"success": False, "error": format_error("read", node_id, e)}
    finally:
        if df is not None:
            del df
        gc.collect()


def get_node_arrow(node_id: int) -> bytes | None:
    """A node's result frame as Arrow IPC stream bytes (host pull API).

    Returns None when the node has no frame OR its plan fails to collect —
    the documented contract is bytes-or-null, never an exception.
    compat_level=oldest for the same reason as parquet output: consumers
    (arrow-js, duckdb-wasm, parquet-wasm) reject polars' view-type layout.
    """
    lf = get_lazyframe(node_id)
    if lf is None:
        return None
    df = None
    try:
        import io

        df = _clean_strings_for_export(lf.collect())
        bio = io.BytesIO()
        df.write_ipc_stream(bio, compat_level=pl.CompatLevel.oldest())
        return bio.getvalue()
    except Exception:
        logger.warning("get_node_arrow node=%s failed to export", node_id, exc_info=True)
        return None
    finally:
        if df is not None:
            del df
        gc.collect()


def list_excel_sheets(file_content) -> dict:
    """List worksheet names in an xlsx workbook (settings-panel sheet picker).

    file_content is bytes-like (bytes under pytest, memoryview from the Pyodide
    bridge). openpyxl is imported lazily — in the browser it's micropip-installed
    on first Excel use, so it must not be a module-level import.
    """
    try:
        import io

        import openpyxl

        wb = openpyxl.load_workbook(io.BytesIO(file_content), read_only=True)
        try:
            sheets = list(wb.sheetnames)
        finally:
            wb.close()
        return {"success": True, "sheets": sheets}
    except Exception as e:
        return {"success": False, "error": f"Could not read the Excel workbook: {e}"}


@log_node
def execute_read_excel(node_id: int, file_content, settings: dict) -> dict:
    """Execute read node for xlsx content - creates a LazyFrame.

    engine="openpyxl" is explicit: the default (calamine/fastexcel) ships no
    wasm wheel. start_row > 0 reads raw and re-promotes the header row, so
    cells arrive as text in that mode.
    """
    df = None
    try:
        import io

        ts = settings.get("received_file", {}).get("table_settings", {})
        sheet_name = ts.get("sheet_name") or None
        has_headers = ts.get("has_headers", True)
        start_row = ts.get("start_row") or 0

        bio = io.BytesIO(file_content)
        kwargs = {"sheet_name": sheet_name} if sheet_name else {"sheet_id": 1}
        if start_row:
            df = pl.read_excel(bio, engine="openpyxl", has_header=False, **kwargs)
            df = df.slice(start_row)
            if has_headers:
                header = [str(v) if v is not None else f"column_{i + 1}" for i, v in enumerate(df.row(0))]
                df = df.slice(1).rename(dict(zip(df.columns, header, strict=False)))
        else:
            df = pl.read_excel(bio, engine="openpyxl", has_header=has_headers, **kwargs)

        store_lazyframe(node_id, df.lazy())
        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        return {"success": False, "error": format_error("read", node_id, e)}
    finally:
        if df is not None:
            del df
        gc.collect()


@log_node
def execute_manual_input(node_id: int, data_content: str, settings: dict) -> dict:
    """Execute manual input node - creates a LazyFrame"""
    try:
        import io

        raw_data_format = settings.get("raw_data_format")
        if raw_data_format and raw_data_format.get("columns") and raw_data_format.get("data"):
            columns_meta = raw_data_format["columns"]
            data = raw_data_format["data"]

            if len(columns_meta) > 0 and len(data) > 0:
                col_names = [c["name"] for c in columns_meta]
                df_dict = {name: values for name, values in zip(col_names, data, strict=False)}
                df = pl.DataFrame(df_dict)
            else:
                df = pl.DataFrame()
        else:
            manual_input = settings.get("manual_input", {})
            has_headers = manual_input.get("has_headers", True)
            delimiter = manual_input.get("delimiter", ",")

            df = pl.read_csv(io.StringIO(data_content), has_header=has_headers, separator=delimiter)

        lf = df.lazy()
        store_lazyframe(node_id, lf)
        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        return {"success": False, "error": format_error("manual_input", node_id, e)}


@log_node
def execute_output(node_id: int, input_id: int, settings: dict) -> dict:
    """Execute output node - prepares data for download.
    Note: This must collect data to generate the output file.
    Memory-optimized: cleans up DataFrame after generating output."""
    # A previous run's staged bytes are stale the moment we re-execute — drop
    # them even if the JS pull never happened (e.g. it failed) so the heap copy
    # can't outlive its run.
    _output_binaries.pop(node_id, None)

    input_lf = get_lazyframe(input_id)
    if input_lf is None:
        return {
            "success": False,
            "error": f"Output error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully.",
        }

    df = None
    try:
        import io

        # Collect data for output
        df = input_lf.collect()
        row_count = len(df)

        output_settings = settings.get("output_settings", {})
        file_type = output_settings.get("file_type", "csv")
        file_name = output_settings.get("name", "output.csv")
        table_settings = output_settings.get("table_settings", {})

        # Store as lazyframe for schema access
        store_lazyframe(node_id, df.lazy())

        content_kind = "text"
        content = ""
        transport = None
        if file_type == "parquet":
            # The wasm polars build can't write parquet; stage Arrow IPC stream
            # bytes and let JS (parquet-wasm) encode the final .parquet file.
            # compat_level=oldest: polars' default view-type layout
            # (variadicBufferCounts) doesn't parse in parquet-wasm's arrow-rs.
            df = _clean_strings_for_export(df)
            bio = io.BytesIO()
            df.write_ipc_stream(bio, compat_level=pl.CompatLevel.oldest())
            _output_binaries[node_id] = bio.getvalue()
            content_kind = "binary"
            transport = "arrow-ipc"
            mime_type = "application/vnd.apache.parquet"
        elif file_type == "excel":
            # xlsxwriter is imported lazily by polars (micropip-installed in the
            # browser on first use). Bytes can't ride the toJs() bridge — stage
            # them for a one-shot take_output_binary pull from JS.
            bio = io.BytesIO()
            df.write_excel(bio, worksheet=table_settings.get("sheet_name") or "Sheet1")
            _output_binaries[node_id] = bio.getvalue()
            content_kind = "binary"
            mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        else:
            delimiter = table_settings.get("delimiter", ",")
            if delimiter == "tab":
                delimiter = "\t"

            buffer = io.StringIO()
            df.write_csv(buffer, separator=delimiter)
            content = buffer.getvalue()
            mime_type = "text/csv"

        # Free DataFrame memory immediately after writing (rebind, not del, so the
        # except cleanup below stays bound)
        df = None
        gc.collect()

        return {
            "success": True,
            "schema": get_schema(node_id),
            "has_data": True,
            "download": {
                "content": content,
                "content_kind": content_kind,
                "transport": transport,
                "file_name": file_name,
                "file_type": file_type,
                "mime_type": mime_type,
                "row_count": row_count,
            },
        }
    except Exception as e:
        _output_binaries.pop(node_id, None)
        if df is not None:
            del df
            gc.collect()
        return {"success": False, "error": format_error_lf("output", node_id, e, input_lf)}
