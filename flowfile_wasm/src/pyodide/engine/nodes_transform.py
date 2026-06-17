import polars as pl

from .dtypes import readable_data_type_group
from .errors import format_error_lf
from .log import log_node
from .nodes_formula import _to_expr
from .state import get_lazyframe, get_schema, store_lazyframe


def convert_filter_value(value: str, dtype) -> any:
    """Convert a single filter value to match column data type"""
    if dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64):
        return int(value)
    elif dtype in (pl.Float32, pl.Float64):
        return float(value)
    elif dtype == pl.Boolean:
        return value.lower() == "true"
    return value


def convert_filter_values(values: list[str], dtype) -> list:
    """Convert filter values to match column data type"""
    return [convert_filter_value(v, dtype) for v in values]


def build_filter(input_lf: pl.LazyFrame, settings: dict) -> pl.LazyFrame:
    """Build the filtered LazyFrame from input + settings (no store, no collect)."""
    filter_input = settings.get("filter_input", {})
    mode = filter_input.get("mode", "basic")

    if mode == "advanced":
        expr = filter_input.get("advanced_filter", "")
        if expr:
            return input_lf.filter(eval(expr))
        return input_lf

    basic = filter_input.get("basic_filter", {})
    field = basic.get("field", "")
    operator = basic.get("operator", "equals")
    value = basic.get("value", "")
    value2 = basic.get("value2", "")

    if not field:
        return input_lf

    col = pl.col(field)
    # Get column data type from schema
    schema = input_lf.collect_schema()
    col_dtype = schema.get(field)

    if operator == "equals":
        return input_lf.filter(col == convert_filter_value(value, col_dtype))
    elif operator == "not_equals":
        return input_lf.filter(col != convert_filter_value(value, col_dtype))
    elif operator == "greater_than":
        return input_lf.filter(col > convert_filter_value(value, col_dtype))
    elif operator == "greater_than_or_equals":
        return input_lf.filter(col >= convert_filter_value(value, col_dtype))
    elif operator == "less_than":
        return input_lf.filter(col < convert_filter_value(value, col_dtype))
    elif operator == "less_than_or_equals":
        return input_lf.filter(col <= convert_filter_value(value, col_dtype))
    elif operator == "contains":
        return input_lf.filter(col.str.contains(value))
    elif operator == "not_contains":
        return input_lf.filter(~col.str.contains(value))
    elif operator == "starts_with":
        return input_lf.filter(col.str.starts_with(value))
    elif operator == "ends_with":
        return input_lf.filter(col.str.ends_with(value))
    elif operator == "is_null":
        return input_lf.filter(col.is_null())
    elif operator == "is_not_null":
        return input_lf.filter(col.is_not_null())
    elif operator == "in":
        values = [v.strip() for v in value.split(",")]
        return input_lf.filter(col.is_in(convert_filter_values(values, col_dtype)))
    elif operator == "not_in":
        values = [v.strip() for v in value.split(",")]
        return input_lf.filter(~col.is_in(convert_filter_values(values, col_dtype)))
    elif operator == "between":
        v1 = convert_filter_value(value, col_dtype)
        v2 = convert_filter_value(value2, col_dtype)
        return input_lf.filter((col >= v1) & (col <= v2))
    return input_lf


@log_node
def execute_filter(node_id: int, input_id: int, settings: dict) -> dict:
    """Execute filter node - chains onto input LazyFrame"""
    input_lf = get_lazyframe(input_id)
    if input_lf is None:
        return {
            "success": False,
            "error": f"Filter error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully.",
        }

    field = settings.get("filter_input", {}).get("basic_filter", {}).get("field")
    try:
        result_lf = build_filter(input_lf, settings)
        store_lazyframe(node_id, result_lf)
        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        return {"success": False, "error": format_error_lf("filter", node_id, e, input_lf, field)}


def build_select(input_lf: pl.LazyFrame, settings: dict) -> pl.LazyFrame:
    """Build the column-selected/renamed LazyFrame (no store, no collect)."""
    select_input = settings.get("select_input", [])
    if not select_input:
        return input_lf

    kept = [s for s in select_input if s.get("keep", True)]
    kept.sort(key=lambda x: x.get("position", 0))

    # Get available columns from schema
    schema = input_lf.collect_schema()
    available_cols = set(schema.keys())

    exprs = []
    for s in kept:
        old_name = s.get("old_name", "")
        new_name = s.get("new_name", old_name)
        if old_name in available_cols:
            if old_name != new_name:
                exprs.append(pl.col(old_name).alias(new_name))
            else:
                exprs.append(pl.col(old_name))

    return input_lf.select(exprs) if exprs else input_lf


@log_node
def execute_select(node_id: int, input_id: int, settings: dict) -> dict:
    """Execute select node - column selection/renaming (lazy)"""
    input_lf = get_lazyframe(input_id)
    if input_lf is None:
        return {
            "success": False,
            "error": f"Select error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully.",
        }

    try:
        result_lf = build_select(input_lf, settings)
        store_lazyframe(node_id, result_lf)
        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        return {"success": False, "error": format_error_lf("select", node_id, e, input_lf)}


def build_sort(input_lf: pl.LazyFrame, settings: dict) -> pl.LazyFrame:
    """Build the sorted LazyFrame (no store, no collect)."""
    # sort_input is now a flat list matching flowfile_core: [{column, how}]
    sort_input = settings.get("sort_input", [])
    if not sort_input:
        return input_lf
    by = [c.get("column") for c in sort_input]
    descending = [c.get("how") == "desc" for c in sort_input]
    return input_lf.sort(by, descending=descending)


@log_node
def execute_sort(node_id: int, input_id: int, settings: dict) -> dict:
    """Execute sort node (lazy)"""
    input_lf = get_lazyframe(input_id)
    if input_lf is None:
        return {
            "success": False,
            "error": f"Sort error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully.",
        }

    try:
        result_lf = build_sort(input_lf, settings)
        store_lazyframe(node_id, result_lf)
        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        return {"success": False, "error": format_error_lf("sort", node_id, e, input_lf)}


def build_unique(input_lf: pl.LazyFrame, settings: dict) -> pl.LazyFrame:
    """Build the de-duplicated LazyFrame (no store, no collect)."""
    unique_input = settings.get("unique_input", {})
    subset = unique_input.get("subset") or unique_input.get("columns") or []
    keep = unique_input.get("keep") or unique_input.get("strategy") or "first"
    maintain_order = unique_input.get("maintain_order", True)
    if subset:
        return input_lf.unique(subset=subset, keep=keep, maintain_order=maintain_order)
    return input_lf.unique(keep=keep, maintain_order=maintain_order)


@log_node
def execute_unique(node_id: int, input_id: int, settings: dict) -> dict:
    """Execute unique node (lazy)"""
    input_lf = get_lazyframe(input_id)
    if input_lf is None:
        return {
            "success": False,
            "error": f"Unique error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully.",
        }

    try:
        result_lf = build_unique(input_lf, settings)
        store_lazyframe(node_id, result_lf)
        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        return {"success": False, "error": format_error_lf("unique", node_id, e, input_lf)}


def build_head(input_lf: pl.LazyFrame, settings: dict) -> pl.LazyFrame:
    """Build the head/limit LazyFrame (no store, no collect)."""
    n = settings.get("head_input", {}).get("n", 10)
    return input_lf.head(n)


@log_node
def execute_head(node_id: int, input_id: int, settings: dict) -> dict:
    """Execute head/limit node (lazy)"""
    input_lf = get_lazyframe(input_id)
    if input_lf is None:
        return {
            "success": False,
            "error": f"Head error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully.",
        }

    try:
        result_lf = build_head(input_lf, settings)
        store_lazyframe(node_id, result_lf)
        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        return {"success": False, "error": format_error_lf("head", node_id, e, input_lf)}


def build_record_id(input_lf: pl.LazyFrame, settings: dict) -> pl.LazyFrame:
    """Build a LazyFrame with a row-number column prepended (no store, no collect)."""
    ri = settings.get("record_id_input") or {}
    name = ri.get("name") or "record_id"
    offset = int(ri.get("offset", 1))
    return input_lf.with_row_index(name=name, offset=offset)


@log_node
def execute_record_id(node_id: int, input_id: int, settings: dict) -> dict:
    """Execute record id node - adds a sequential row-number column (lazy)."""
    input_lf = get_lazyframe(input_id)
    if input_lf is None:
        return {
            "success": False,
            "error": f"Record ID error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully.",
        }

    try:
        result_lf = build_record_id(input_lf, settings)
        store_lazyframe(node_id, result_lf)
        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        return {"success": False, "error": format_error_lf("record_id", node_id, e, input_lf)}


def _select_rename_targets(columns: list[tuple[str, str]], settings: dict) -> list[str]:
    """Return, in schema order, the column names the rename rule applies to.

    Ports flowfile_core's FlowDataEngine._select_rename_targets: applies the
    selection_mode filter ("all" / "list" / "data_type"). Unknown names in
    selected_columns are dropped so stale UI state never breaks execution.
    """
    mode = settings.get("selection_mode", "all")
    if mode == "all":
        return [name for name, _ in columns]
    if mode == "list":
        available = {name for name, _ in columns}
        return [c for c in (settings.get("selected_columns") or []) if c in available]
    if mode == "data_type":
        wanted = settings.get("selected_data_type")
        if wanted is None:
            return []
        return [name for name, group in columns if group == wanted]
    return []


def _compute_renamed_names(
    targets: list[str], settings: dict, first_row_values: dict | None = None
) -> list[str]:
    """Return new names aligned 1:1 with `targets` for the configured rename mode.

    Ports flowfile_core's FlowDataEngine._compute_renamed_names. prefix/suffix apply
    string-wise; first_row pulls from first_row_values (raising on null/empty);
    formula evaluates the flowfile expression against a one-column `column_name`
    frame, broadcasting a scalar result and erroring on any other cardinality
    mismatch.
    """
    if not targets:
        return []
    mode = settings.get("rename_mode", "prefix")
    if mode == "prefix":
        prefix = settings.get("prefix") or ""
        return [f"{prefix}{n}" for n in targets]
    if mode == "suffix":
        suffix = settings.get("suffix") or ""
        return [f"{n}{suffix}" for n in targets]
    if mode == "first_row":
        if first_row_values is None:
            return list(targets)
        new = [first_row_values.get(name) for name in targets]
        for original, v in zip(targets, new, strict=True):
            if v is None or (isinstance(v, str) and v.strip() == ""):
                raise ValueError(f"Dynamic rename (first_row) got a null/empty value for column '{original}'.")
        return [str(v) for v in new]
    if mode == "formula":
        formula = (settings.get("formula") or "").strip()
        if not formula:
            return list(targets)
        expr = _to_expr(formula)
        tmp = pl.DataFrame({"column_name": targets})
        results = tmp.select(expr.alias("__ff_rename__"))["__ff_rename__"].to_list()
        if len(results) == 1 and len(targets) > 1:
            results = results * len(targets)
        elif len(results) != len(targets):
            raise ValueError(
                "Dynamic rename formula must produce one value per column "
                f"(got {len(results)} for {len(targets)} target column(s))."
            )
        for original, new in zip(targets, results, strict=True):
            if new is None:
                raise ValueError(f"Dynamic rename formula returned null for column '{original}'.")
        return [str(n) for n in results]
    return list(targets)


def _assert_rename_has_no_duplicates(rename_map: dict[str, str], all_columns: list[tuple[str, str]]) -> None:
    """Raise if the rename map would yield duplicate final column names.

    Catches two collisions: two renames producing the same name, and a rename
    producing a name already held by an untouched column. Ports flowfile_core's
    FlowDataEngine._assert_rename_has_no_duplicates.
    """
    untouched = {name for name, _ in all_columns} - set(rename_map.keys())
    duplicates: set[str] = set()
    seen: set[str] = set()
    for new in rename_map.values():
        if new in seen or new in untouched:
            duplicates.add(new)
        seen.add(new)
    if duplicates:
        raise ValueError("Dynamic rename produces duplicate column name(s): " + ", ".join(sorted(duplicates)))


def resolve_dynamic_rename_map(
    columns: list[tuple[str, str]], settings: dict, first_row_values: dict | None = None
) -> dict[str, str]:
    """Compute the {old_name: new_name} map for a dynamic-rename rule (no-ops omitted).

    `columns` is the incoming schema as (name, data_type_group) tuples; `settings`
    is the dynamic_rename_input dict. Raises ValueError on duplicate output names.
    """
    targets = _select_rename_targets(columns, settings)
    new_names = _compute_renamed_names(targets, settings, first_row_values=first_row_values)
    rename_map = {old: new for old, new in zip(targets, new_names, strict=True) if old != new}
    _assert_rename_has_no_duplicates(rename_map, columns)
    return rename_map


def build_dynamic_rename(input_lf: pl.LazyFrame, settings: dict) -> pl.LazyFrame:
    """Apply a dynamic rename rule (prefix/suffix/formula); lazy, no store, no collect.

    first_row mode needs the first data row, so it is resolved in
    execute_dynamic_rename and returned unchanged here.
    """
    dr = settings.get("dynamic_rename_input") or {}
    if dr.get("rename_mode") == "first_row":
        return input_lf
    schema = input_lf.collect_schema()
    columns = [(name, readable_data_type_group(str(dt))) for name, dt in schema.items()]
    rename_map = resolve_dynamic_rename_map(columns, dr)
    return input_lf.rename(rename_map) if rename_map else input_lf


@log_node
def execute_dynamic_rename(node_id: int, input_id: int, settings: dict) -> dict:
    """Execute dynamic rename - bulk-renames columns by a single rule (lazy)."""
    input_lf = get_lazyframe(input_id)
    if input_lf is None:
        return {
            "success": False,
            "error": f"Rename error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully.",
        }

    try:
        dr = settings.get("dynamic_rename_input") or {}
        if dr.get("rename_mode") == "first_row":
            head = input_lf.head(1).collect()
            if head.height == 0:
                raise ValueError("Dynamic rename (first_row) requires at least one row in the input; got 0.")
            first_row_values = dict(zip(head.columns, head.row(0), strict=True))
            columns = [(n, readable_data_type_group(str(d))) for n, d in input_lf.collect_schema().items()]
            rename_map = resolve_dynamic_rename_map(columns, dr, first_row_values=first_row_values)
            result_lf = (input_lf.rename(rename_map) if rename_map else input_lf).slice(1)
        else:
            result_lf = build_dynamic_rename(input_lf, settings)
        store_lazyframe(node_id, result_lf)
        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        return {"success": False, "error": format_error_lf("dynamic_rename", node_id, e, input_lf)}


def preview_dynamic_rename(settings: dict, incoming_columns: list) -> dict:
    """Resolve a dynamic-rename rule against a given schema (no execution).

    Mirrors flowfile_core's /dynamic_rename/preview endpoint: returns
    {"rename_map": {...}, "error": str | None}. Used by the settings panel for the
    formula-mode preview (prefix/suffix preview is computed client-side in TS).
    first_row is not previewed here — its names depend on row data.
    """
    dr = settings.get("dynamic_rename_input", settings) or {}
    columns = [
        (c.get("name"), c.get("data_type_group") or readable_data_type_group(str(c.get("data_type", ""))))
        for c in incoming_columns
    ]
    try:
        rename_map = resolve_dynamic_rename_map(columns, dr)
    except ValueError as e:
        return {"rename_map": {}, "error": str(e)}
    except Exception as e:
        return {"rename_map": {}, "error": f"Formula error: {e}"}
    return {"rename_map": rename_map, "error": None}


@log_node
def execute_preview(node_id: int, input_id: int) -> dict:
    """Execute preview node - just passes through the LazyFrame"""
    input_lf = get_lazyframe(input_id)
    if input_lf is None:
        return {
            "success": False,
            "error": f"Preview error on node #{node_id}: No input data from node #{input_id}. Make sure the upstream node executed successfully.",
        }

    try:
        store_lazyframe(node_id, input_lf)
        return {"success": True, "schema": get_schema(node_id), "has_data": True}
    except Exception as e:
        return {"success": False, "error": format_error_lf("explore_data", node_id, e, input_lf)}
