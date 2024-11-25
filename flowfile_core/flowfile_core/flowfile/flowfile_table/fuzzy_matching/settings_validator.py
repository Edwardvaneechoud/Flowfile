
from typing import List
from flowfile_core.flowfile.flowfile_table.flow_file_column.main import FlowfileColumn, PlType
from flowfile_core.schemas import transform_schema
from flowfile_core.schemas import input_schema
from polars import datatypes
import polars as pl


def calculate_uniqueness(a: float, b: float) -> float:
    return ((pow(a + 0.5, 2) + pow(b + 0.5, 2)) / 2 - pow(0.5, 2)) + 0.5 * abs(a - b)


def finetune_fuzzy_mapping_settings(left_schema: List[FlowfileColumn],
                                    fuzzy_match_input: transform_schema.FuzzyMatchInput,
                                    right_schema: List[FlowfileColumn]) -> transform_schema.FuzzyMatchInput:
    for fuzzy_mapping in fuzzy_match_input.join_mapping:
        left_col_stat = next((col for col in left_schema if col.name == fuzzy_mapping.left_col), None)
        right_col_stat = next((col for col in right_schema if col.name == fuzzy_mapping.right_col), None)
        fuzzy_mapping.perc_unique = calculate_uniqueness(left_col_stat.perc_unique, right_col_stat.perc_unique)
    fuzzy_match_input.join_mapping.sort(key=lambda x: x.perc_unique, reverse=True)
    uniqueness_rate = sum(jm.perc_unique for jm in fuzzy_match_input.join_mapping)
    fuzzy_match_input.aggregate_output = uniqueness_rate < 1.2
    return fuzzy_match_input


def calculate_fuzzy_match_schema(fm_input: transform_schema.FuzzyMatchInput,
                                 left_schema: List[FlowfileColumn],
                                 right_schema: List[FlowfileColumn]):
    print('calculating fuzzy match schema')
    left_schema_dict, right_schema_dict = ({ls.name: ls for ls in left_schema}, {rs.name: rs for rs in right_schema})
    fm_input.auto_rename()

    output_schema = []
    for column in fm_input.left_select.renames:
        column_schema = left_schema_dict.get(column.old_name)
        if column_schema and column.keep:
            output_schema.append(FlowfileColumn.from_input(column.new_name, column_schema.data_type,
                                                           example_values=column_schema.example_values))
    for column in fm_input.right_select.renames:
        column_schema = right_schema_dict.get(column.old_name)
        if column_schema and column.keep:
            output_schema.append(FlowfileColumn.from_input(column.new_name, column_schema.data_type,
                                                           example_values=column_schema.example_values))

    for i, fm in enumerate(fm_input.join_mappings):
        output_schema.append(FlowfileColumn.from_input(f'fuzzy_score_{i}', 'Float64'))
    return output_schema


def get_schema_of_column(node_input_schema: List[FlowfileColumn], col_name: str) -> FlowfileColumn|None:
    for s in node_input_schema:
        if s.name == col_name:
            return s


def pre_calculate_pivot_schema(node_input_schema: List[FlowfileColumn],
                               pivot_input: transform_schema.PivotInput,
                               output_fields: List[input_schema.MinimalFieldInfo] = None,
                               input_lf: pl.LazyFrame = None) -> List[FlowfileColumn]:
    index_columns_schema = [get_schema_of_column(node_input_schema, index_col) for index_col in
                            pivot_input.index_columns]
    val_column_schema = get_schema_of_column(node_input_schema, pivot_input.value_col)
    if val_column_schema.generic_datatype() == 'numeric':
        output_type = datatypes.Float64
    elif val_column_schema.generic_datatype() == 'string':
        output_type = datatypes.Utf8
    elif val_column_schema.generic_datatype() == 'date':
        output_type = datatypes.Datetime
    else:
        output_type = datatypes.Utf8
    if output_fields is not None and len(output_fields) > 0:
        return index_columns_schema+[FlowfileColumn(PlType(Plcolumn_name=output_field.name,
                                                           pl_datatype=output_type)) for output_field in output_fields]

    else:
        pl_output_fields = []
        unique_vals = input_lf.select(pivot_input.pivot_column).unique().collect(streaming=True)[:, 0].cast(pl.String)
        for val in unique_vals:
            for agg in pivot_input.aggregations:
                pl_output_fields.append(PlType(column_name=f'{val}_{agg}', pl_datatype=output_type))
        return index_columns_schema + [FlowfileColumn(pl_output_field) for pl_output_field in pl_output_fields]
