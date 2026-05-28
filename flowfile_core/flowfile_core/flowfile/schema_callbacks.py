from typing import TYPE_CHECKING

import polars as pl
from pl_fuzzy_frame_match.output_column_name_utils import set_name_in_fuzzy_mappings
from pl_fuzzy_frame_match.pre_process import rename_fuzzy_right_mapping
from polars import datatypes

from flowfile_core.configs.flow_logger import main_logger
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn, PlType
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import fetch_unique_values
from flowfile_core.schemas import input_schema, transform_schema

if TYPE_CHECKING:
    from flowfile_core.schemas.transform_schema import JoinSelectManagerMixin


def _ensure_all_columns_have_select(
    left_cols: list[str],
    right_cols: list[str],
    manager: "JoinSelectManagerMixin",
):
    """Append any incoming columns missing from the manager's select lists.

    Applies to fuzzy-match, join, and cross-join managers — any class that exposes
    `.left_select.renames` and `.right_select.renames`. Newly added columns default
    to `keep=True, is_available=True, join_key=False`, so unseen upstream columns
    flow through to the output without requiring the user to re-open the node.
    """
    right_cols_in_select = {c.old_name for c in manager.right_manager.renames}
    left_cols_in_select = {c.old_name for c in manager.left_manager.renames}

    manager.left_manager.renames.extend(
        [transform_schema.SelectInput(col) for col in left_cols if col not in left_cols_in_select]
    )
    manager.right_manager.renames.extend(
        [transform_schema.SelectInput(col) for col in right_cols if col not in right_cols_in_select]
    )


def _order_join_inputs_based_on_col_order(
    col_order: list[str], join_inputs: transform_schema.JoinInputsManager
) -> None:
    """
    Ensure that the select columns in the fuzzy match input match the order of the incoming columns.
    This function modifies the join_inputs object in-place.

    Returns:
        None
    """
    select_map = {select.old_name: select for select in join_inputs.renames}
    ordered_renames = [select_map[col] for col in col_order if col in select_map]
    join_inputs.select_inputs.renames = ordered_renames


def calculate_fuzzy_match_schema(
    fm_input: transform_schema.FuzzyMatchInputManager,
    left_schema: list[FlowfileColumn],
    right_schema: list[FlowfileColumn],
):
    _ensure_all_columns_have_select(
        left_cols=[col.column_name for col in left_schema],
        right_cols=[col.column_name for col in right_schema],
        manager=fm_input,
    )

    _order_join_inputs_based_on_col_order(
        col_order=[col.column_name for col in left_schema], join_inputs=fm_input.left_select
    )
    _order_join_inputs_based_on_col_order(
        col_order=[col.column_name for col in right_schema], join_inputs=fm_input.right_select
    )
    for column in fm_input.left_select.renames:
        if column.join_key:
            column.keep = True
    for column in fm_input.right_select.renames:
        if column.join_key:
            column.keep = True

    left_schema_dict, right_schema_dict = ({ls.name: ls for ls in left_schema}, {rs.name: rs for rs in right_schema})
    fm_input.auto_rename()
    right_renames = {column.old_name: column.new_name for column in fm_input.right_select.renames}
    new_join_mapping = rename_fuzzy_right_mapping(fm_input.join_mapping, right_renames)
    output_schema = []
    for column in fm_input.left_select.renames:
        column_schema = left_schema_dict.get(column.old_name)
        if column_schema and (column.keep or column.join_key):
            output_schema.append(
                FlowfileColumn.from_input(
                    column.new_name, column_schema.data_type, example_values=column_schema.example_values
                )
            )
    for column in fm_input.right_select.renames:
        column_schema = right_schema_dict.get(column.old_name)
        if column_schema and (column.keep or column.join_key):
            output_schema.append(
                FlowfileColumn.from_input(
                    column.new_name, column_schema.data_type, example_values=column_schema.example_values
                )
            )
    set_name_in_fuzzy_mappings(new_join_mapping)
    output_schema.extend(
        [FlowfileColumn.from_input(fuzzy_mapping.output_column_name, "Float64") for fuzzy_mapping in new_join_mapping]
    )
    return output_schema


def calculate_cross_join_schema(
    cj_input: transform_schema.CrossJoinInputManager,
    left_schema: list[FlowfileColumn],
    right_schema: list[FlowfileColumn],
) -> list[FlowfileColumn]:
    """Predict the output schema of a cross-join node.

    Mirrors `FlowDataEngine.do_cross_join`: any upstream column missing from the
    stored selection is passed through with `keep=True`, then overlapping names
    are resolved with a `_right` suffix via `auto_rename`.
    """
    _ensure_all_columns_have_select(
        left_cols=[col.column_name for col in left_schema],
        right_cols=[col.column_name for col in right_schema],
        manager=cj_input,
    )
    _order_join_inputs_based_on_col_order(
        col_order=[col.column_name for col in left_schema], join_inputs=cj_input.left_select
    )
    _order_join_inputs_based_on_col_order(
        col_order=[col.column_name for col in right_schema], join_inputs=cj_input.right_select
    )
    cj_input.auto_rename(rename_mode="suffix")

    left_schema_dict = {ls.name: ls for ls in left_schema}
    right_schema_dict = {rs.name: rs for rs in right_schema}
    output_schema: list[FlowfileColumn] = []
    for column in cj_input.left_select.renames:
        column_schema = left_schema_dict.get(column.old_name)
        if column_schema and column.keep:
            output_schema.append(
                FlowfileColumn.from_input(
                    column.new_name, column_schema.data_type, example_values=column_schema.example_values
                )
            )
    for column in cj_input.right_select.renames:
        column_schema = right_schema_dict.get(column.old_name)
        if column_schema and column.keep:
            output_schema.append(
                FlowfileColumn.from_input(
                    column.new_name, column_schema.data_type, example_values=column_schema.example_values
                )
            )
    return output_schema


def calculate_join_schema(
    j_input: transform_schema.JoinInputManager,
    left_schema: list[FlowfileColumn],
    right_schema: list[FlowfileColumn],
    auto_generate_selection: bool = True,
) -> list[FlowfileColumn]:
    """Predict the output schema of a standard SQL-style join.

    Mirrors `FlowDataEngine.join`:
    - unseen upstream columns are appended with `keep=True`;
    - missing join-key columns that only live in `join_mapping` are also added;
    - for `semi` / `anti` joins the right-side is dropped from output;
    - if `auto_generate_selection` is True, overlapping names are renamed with
      a `_right` suffix via `auto_rename`.
    """
    _ensure_all_columns_have_select(
        left_cols=[col.column_name for col in left_schema],
        right_cols=[col.column_name for col in right_schema],
        manager=j_input,
    )
    j_input.set_join_keys()

    if j_input.input.how in ("semi", "anti"):
        for jk in j_input.input.right_select.renames:
            jk.keep = False

    left_old_names = {c.old_name for c in j_input.input.left_select.renames}
    right_old_names = {c.old_name for c in j_input.input.right_select.renames}
    for jk in j_input.join_mapping:
        if jk.left_col not in left_old_names:
            j_input.input.left_select.renames.append(
                transform_schema.SelectInput(jk.left_col, keep=False, join_key=True)
            )
        if jk.right_col not in right_old_names:
            j_input.input.right_select.renames.append(
                transform_schema.SelectInput(jk.right_col, keep=False, join_key=True)
            )

    _order_join_inputs_based_on_col_order(
        col_order=[col.column_name for col in left_schema], join_inputs=j_input.left_select
    )
    _order_join_inputs_based_on_col_order(
        col_order=[col.column_name for col in right_schema], join_inputs=j_input.right_select
    )

    if auto_generate_selection:
        j_input.auto_rename()

    left_schema_dict = {ls.name: ls for ls in left_schema}
    right_schema_dict = {rs.name: rs for rs in right_schema}
    output_schema: list[FlowfileColumn] = []
    for column in j_input.input.left_select.renames:
        column_schema = left_schema_dict.get(column.old_name)
        if column_schema and column.keep:
            output_schema.append(
                FlowfileColumn.from_input(
                    column.new_name, column_schema.data_type, example_values=column_schema.example_values
                )
            )
    for column in j_input.input.right_select.renames:
        column_schema = right_schema_dict.get(column.old_name)
        if column_schema and column.keep:
            output_schema.append(
                FlowfileColumn.from_input(
                    column.new_name, column_schema.data_type, example_values=column_schema.example_values
                )
            )
    return output_schema


def get_schema_of_column(node_input_schema: list[FlowfileColumn], col_name: str) -> FlowfileColumn | None:
    for s in node_input_schema:
        if s.name == col_name:
            return s


class InvalidSetup(ValueError):
    """Error raised when pivot column has too many unique values."""

    pass


def get_output_data_type_pivot(schema: FlowfileColumn, agg_type: str) -> datatypes:
    if agg_type in ("count", "n_unique"):
        output_type = datatypes.Float64  # count is always float
    elif schema.generic_datatype() == "numeric":
        output_type = datatypes.Float64
    elif schema.generic_datatype() == "string":
        output_type = datatypes.Utf8
    elif schema.generic_datatype() == "date":
        output_type = datatypes.Datetime
    else:
        output_type = datatypes.Utf8
    return output_type


def pre_calculate_pivot_schema(
    node_input_schema: list[FlowfileColumn],
    pivot_input: transform_schema.PivotInput,
    output_fields: list[input_schema.MinimalFieldInfo] = None,
    input_lf: pl.LazyFrame = None,
) -> list[FlowfileColumn]:
    index_columns_schema = [
        get_schema_of_column(node_input_schema, index_col) for index_col in pivot_input.index_columns
    ]
    val_column_schema = get_schema_of_column(node_input_schema, pivot_input.value_col)
    if output_fields is not None and len(output_fields) > 0:
        return index_columns_schema + [
            FlowfileColumn(PlType(column_name=output_field.name, pl_datatype=output_field.data_type))
            for output_field in output_fields
        ]

    else:
        max_unique_vals = 200
        unique_vals = fetch_unique_values(
            input_lf.select(pivot_input.pivot_column)
            .unique()
            .sort(pivot_input.pivot_column)
            .limit(max_unique_vals)
            .cast(pl.String)
        )
        if len(unique_vals) >= max_unique_vals:
            main_logger.warning(
                "Pivot column has too many unique values. Please consider using a different column."
                f" Max unique values: {max_unique_vals}"
            )
        pl_output_fields = []
        for val in unique_vals:
            if len(pivot_input.aggregations) == 1:
                output_type = get_output_data_type_pivot(val_column_schema, pivot_input.aggregations[0])
                pl_output_fields.append(PlType(column_name=str(val), pl_datatype=output_type))
            else:
                for agg in pivot_input.aggregations:
                    output_type = get_output_data_type_pivot(val_column_schema, agg)
                    pl_output_fields.append(PlType(column_name=f"{val}_{agg}", pl_datatype=output_type))
        return index_columns_schema + [FlowfileColumn(pl_output_field) for pl_output_field in pl_output_fields]
