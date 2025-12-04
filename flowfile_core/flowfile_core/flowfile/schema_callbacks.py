
from typing import List

from polars import datatypes
import polars as pl

from pl_fuzzy_frame_match.output_column_name_utils import set_name_in_fuzzy_mappings
from pl_fuzzy_frame_match.pre_process import rename_fuzzy_right_mapping

from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import fetch_unique_values
from flowfile_core.configs.flow_logger import main_logger
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn, PlType
from flowfile_core.schemas import transform_schema
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.transform_schema import FuzzyMatchInputManager


def _ensure_all_columns_have_select(left_cols: List[str],
                                    right_cols: List[str],
                                    fuzzy_match_input: transform_schema.FuzzyMatchInputManager):
    """
    Ensure that all columns in the left and right FlowDataEngines are included in the fuzzy match input's select
     statements.
    Args:
        left_cols (List[str]): List of column names in the left FlowDataEngine.
        right_cols (List[str]): List of column names in the right FlowDataEngine.
        fuzzy_match_input (FuzzyMatchInput): Fuzzy match input configuration containing select statements.

    Returns:
        None
    """
    right_cols_in_select = {c.old_name for c in fuzzy_match_input.right_select.renames}
    left_cols_in_select = {c.old_name for c in fuzzy_match_input.left_select.renames}

    fuzzy_match_input.left_select.renames.extend(
        [transform_schema.SelectInput(col) for col in left_cols if col not in left_cols_in_select])
    fuzzy_match_input.right_select.renames.extend(
        [transform_schema.SelectInput(col) for col in right_cols if col not in right_cols_in_select]
    )


def _order_join_inputs_based_on_col_order(col_order: List[str], join_inputs: transform_schema.JoinInputsManager) -> None:
    """
    Ensure that the select columns in the fuzzy match input match the order of the incoming columns.
    This function modifies the join_inputs object in-place.

    Returns:
        None
    """
    select_map = {select.old_name: select for select in join_inputs.renames}
    ordered_renames = [select_map[col] for col in col_order if col in select_map]
    join_inputs.select_inputs.renames = ordered_renames


def calculate_fuzzy_match_schema(fm_input: transform_schema.FuzzyMatchInputManager,
                                 left_schema: List[FlowfileColumn],
                                 right_schema: List[FlowfileColumn]):
    _ensure_all_columns_have_select(left_cols=[col.column_name for col in left_schema],
                                    right_cols=[col.column_name for col in right_schema],
                                    fuzzy_match_input=fm_input)

    _order_join_inputs_based_on_col_order(col_order=[col.column_name for col in left_schema],
                                          join_inputs=fm_input.left_select)
    _order_join_inputs_based_on_col_order(col_order=[col.column_name for col in right_schema],
                                          join_inputs=fm_input.right_select)
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
            output_schema.append(FlowfileColumn.from_input(column.new_name, column_schema.data_type,
                                                           example_values=column_schema.example_values))
    for column in fm_input.right_select.renames:
        column_schema = right_schema_dict.get(column.old_name)
        if column_schema and (column.keep or column.join_key):
            output_schema.append(FlowfileColumn.from_input(column.new_name, column_schema.data_type,
                                                           example_values=column_schema.example_values))
    set_name_in_fuzzy_mappings(new_join_mapping)
    output_schema.extend([FlowfileColumn.from_input(fuzzy_mapping.output_column_name, 'Float64')
                          for fuzzy_mapping in new_join_mapping])
    return output_schema


def get_schema_of_column(node_input_schema: List[FlowfileColumn], col_name: str) -> FlowfileColumn|None:
    for s in node_input_schema:
        if s.name == col_name:
            return s


class InvalidSetup(ValueError):
    """Error raised when pivot column has too many unique values."""
    pass


def get_output_data_type_pivot(schema: FlowfileColumn, agg_type: str) -> datatypes:
    if agg_type in ('count', 'n_unique'):
        output_type = datatypes.Float64  # count is always float
    elif schema.generic_datatype() == 'numeric':
        output_type = datatypes.Float64
    elif schema.generic_datatype() == 'string':
        output_type = datatypes.Utf8
    elif schema.generic_datatype() == 'date':
        output_type = datatypes.Datetime
    else:
        output_type = datatypes.Utf8
    return output_type


def pre_calculate_pivot_schema(node_input_schema: List[FlowfileColumn],
                               pivot_input: transform_schema.PivotInput,
                               output_fields: List[input_schema.MinimalFieldInfo] = None,
                               input_lf: pl.LazyFrame = None) -> List[FlowfileColumn]:
    index_columns_schema = [get_schema_of_column(node_input_schema, index_col) for index_col in
                            pivot_input.index_columns]
    val_column_schema = get_schema_of_column(node_input_schema, pivot_input.value_col)
    if output_fields is not None and len(output_fields) > 0:
        return index_columns_schema+[FlowfileColumn(PlType(column_name=output_field.name,
                                                           pl_datatype=output_field.data_type)) for output_field in
                                     output_fields]

    else:
        max_unique_vals = 200
        unique_vals = fetch_unique_values(input_lf.select(pivot_input.pivot_column)
                                          .unique()
                                          .sort(pivot_input.pivot_column)
                                          .limit(max_unique_vals).cast(pl.String))
        if len(unique_vals) >= max_unique_vals:
            main_logger.warning('Pivot column has too many unique values. Please consider using a different column.'
                                f' Max unique values: {max_unique_vals}')
        pl_output_fields = []
        for val in unique_vals:
            if len(pivot_input.aggregations) == 1:
                output_type = get_output_data_type_pivot(val_column_schema, pivot_input.aggregations[0])
                pl_output_fields.append(PlType(column_name=str(val), pl_datatype=output_type))
            else:
                for agg in pivot_input.aggregations:
                    output_type = get_output_data_type_pivot(val_column_schema, agg)
                    pl_output_fields.append(PlType(column_name=f'{val}_{agg}', pl_datatype=output_type))
        return index_columns_schema + [FlowfileColumn(pl_output_field) for pl_output_field in pl_output_fields]
