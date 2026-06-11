"""In-browser Pyodide execution engine (Polars), as a real package.

Split from a single ~1.6k-line module into focused submodules by responsibility.
The browser writes these files into Pyodide's virtual filesystem and runs
`from engine import *` (so the bare-name calls in flow-store.ts keep working);
pytest imports the package directly. See flowfile_wasm/CLAUDE.md.
"""

from .dtypes import build_empty_lf_from_schema
from .errors import format_error, format_error_lf
from .log import logger, set_log_level
from .nodes_aggregate import (
    build_group_by,
    build_unpivot,
    execute_group_by,
    execute_pivot,
    execute_unpivot,
)
from .nodes_combine import build_join, execute_join
from .nodes_explore import execute_explore_data, prepare_visual_data
from .nodes_io import (
    execute_manual_input,
    execute_output,
    execute_read_csv,
    execute_read_excel,
    execute_read_ipc,
    get_node_arrow,
    list_excel_sheets,
)
from .nodes_polars_code import build_polars_code_schema, execute_polars_code
from .nodes_transform import (
    build_filter,
    build_head,
    build_select,
    build_sort,
    build_unique,
    convert_filter_value,
    convert_filter_values,
    execute_filter,
    execute_head,
    execute_preview,
    execute_select,
    execute_sort,
    execute_unique,
)
from .preview import (
    df_to_preview,
    fetch_preview,
    get_memory_stats,
    invalidate_downstream_previews,
    materialize_preview,
)
from .schema_propagation import propagate_schemas
from .state import (
    clear_all,
    clear_node,
    get_cached_preview,
    get_lazyframe,
    get_schema,
    has_cached_preview,
    run_gc,
    store_lazyframe,
    take_output_binary,
)
from .validation import (
    clean_setting_input,
    prepare_node_for_export,
    validate_flowfile_data,
)

__all__ = [
    # dtypes
    "build_empty_lf_from_schema",
    # errors
    "format_error",
    "format_error_lf",
    # logging
    "logger",
    "set_log_level",
    # state + cache
    "clear_all",
    "clear_node",
    "get_cached_preview",
    "get_lazyframe",
    "get_schema",
    "has_cached_preview",
    "run_gc",
    "store_lazyframe",
    "take_output_binary",
    # preview
    "df_to_preview",
    "fetch_preview",
    "get_memory_stats",
    "invalidate_downstream_previews",
    "materialize_preview",
    # validation / export
    "clean_setting_input",
    "prepare_node_for_export",
    "validate_flowfile_data",
    # nodes: io
    "execute_manual_input",
    "execute_output",
    "execute_read_csv",
    "execute_read_excel",
    "execute_read_ipc",
    "get_node_arrow",
    "list_excel_sheets",
    # nodes: transform
    "build_filter",
    "build_head",
    "build_select",
    "build_sort",
    "build_unique",
    "convert_filter_value",
    "convert_filter_values",
    "execute_filter",
    "execute_head",
    "execute_preview",
    "execute_select",
    "execute_sort",
    "execute_unique",
    # nodes: aggregate
    "build_group_by",
    "build_unpivot",
    "execute_group_by",
    "execute_pivot",
    "execute_unpivot",
    # nodes: combine
    "build_join",
    "execute_join",
    # nodes: polars code
    "build_polars_code_schema",
    "execute_polars_code",
    # nodes: explore
    "execute_explore_data",
    "prepare_visual_data",
    # schema propagation
    "propagate_schemas",
]
