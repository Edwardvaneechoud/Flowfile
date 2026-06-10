import gc

import polars as pl

from .errors import format_error, format_error_lf
from .log import log_node
from .state import get_lazyframe, get_schema, store_lazyframe


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

        if file_type == "parquet":
            # Parquet export is not supported in the browser/WASM environment
            del df
            gc.collect()
            return {
                "success": False,
                "error": f"Output error on node #{node_id}: Parquet export is not supported in the browser. Please use CSV format instead.",
            }
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
                "file_name": file_name,
                "file_type": file_type,
                "mime_type": mime_type,
                "row_count": row_count,
            },
        }
    except Exception as e:
        if df is not None:
            del df
            gc.collect()
        return {"success": False, "error": format_error_lf("output", node_id, e, input_lf)}
