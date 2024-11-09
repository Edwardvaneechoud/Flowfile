from flowfile_core.flowfile.flowfile_table.flowFilePolars import FlowFileTable, FlowFileColumn
from flowfile_core.schemas.analysis_schemas import graphic_walker_schemas as gw_schema
from typing import List


def get_semantic_type(data_type: str) -> str:
    """Determine the semanticType based on the data_type."""
    if data_type in ['Utf8', 'VARCHAR', 'CHAR', 'NVARCHAR', 'String']:
        return 'nominal'
    elif data_type in ['Int64', 'Float64', 'Int32', 'Float32', 'Int16', 'Float16', 'Decimal']:
        return 'quantitative'
    elif data_type in ['Datetime', 'Date']:
        return 'temporal'
    else:
        return 'nominal'  # Default case; adjust as necessary


def get_analytic_type(semantic_type: str) -> str:
    """Determine the analyticType based on the semanticType."""
    return 'measure' if semantic_type == 'quantitative' else 'dimension'


def convert_ff_column_to_gw_field(flow_file_column: FlowFileColumn) -> gw_schema.MutField:
    """
    Converts a FlowFileColumn instance into a GraphicWalkerField.

    Args:
    - flow_file_column: An instance of FlowFileColumn representing a column in the data schema.

    Returns:
    - A GraphicWalkerField instance with properties derived from the FlowFileColumn.
    """
    semantic_type = get_semantic_type(flow_file_column.data_type)

    # Determine the analytic type based on the semantic type
    analytic_type = get_analytic_type(semantic_type)

    # Create and return a new GraphicWalkerField instance
    return gw_schema.MutField(
        fid=flow_file_column.name,
        name=flow_file_column.name,
        basename=flow_file_column.name,
        key=flow_file_column.name,
        semanticType=semantic_type,
        analyticType=analytic_type
    )


def convert_ff_columns_to_gw_fields(ff_columns: List[FlowFileColumn]) -> [gw_schema.MutField]:
    return [convert_ff_column_to_gw_field(ff_column) for ff_column in ff_columns]


def get_initial_gf_data_from_ff(flow_file: FlowFileTable) -> gw_schema.DataModel:
    number_of_records = flow_file.get_number_of_records()
    if number_of_records > 100_000:
        flow_file = flow_file.get_sample(100_000, random=True)
    data = flow_file.to_pylist()
    fields = [convert_ff_column_to_gw_field(ff_column) for ff_column in flow_file.schema]
    return gw_schema.DataModel(fields=fields, data=data)


def get_gf_data_from_ff(flow_file: FlowFileTable, fields: List[gw_schema.MutField]) -> gw_schema.DataModel:
    data = flow_file.to_pylist()
    return gw_schema.DataModel(fields=fields, data=data)
