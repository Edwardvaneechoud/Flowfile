from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowfileColumn
from flowfile_core.schemas.analysis_schemas import graphic_walker_schemas as gw_schema


def get_semantic_type(datatype_group: str) -> str:
    """Map a readable dtype group onto a Graphic Walker semantic type.

    Must agree with ``polars_gw.get_fields``, which owns the field schema the
    browser actually charts against: numerics are quantitative, date-like
    columns temporal, everything else nominal.
    """
    if datatype_group == "Numeric":
        return "quantitative"
    elif datatype_group == "Date":
        return "temporal"
    else:
        return "nominal"


def get_analytic_type(semantic_type: str) -> gw_schema.AnalyticTypeLit:
    """Determine the analyticType based on the semanticType."""
    return "measure" if semantic_type == "quantitative" else "dimension"


def convert_ff_column_to_gw_field(flow_file_column: FlowfileColumn) -> gw_schema.MutField:
    """
    Converts a FlowfileColumn instance into a GraphicWalkerField.

    Args:
    - flow_file_column: An instance of FlowfileColumn representing a column in the data schema.

    Returns:
    - A GraphicWalkerField instance with properties derived from the FlowfileColumn.
    """
    semantic_type = get_semantic_type(flow_file_column.get_readable_datatype_group())

    analytic_type = get_analytic_type(semantic_type)

    return gw_schema.MutField(
        fid=flow_file_column.name,
        name=flow_file_column.name,
        basename=flow_file_column.name,
        key=flow_file_column.name,
        semanticType=semantic_type,
        analyticType=analytic_type,
    )


def convert_ff_columns_to_gw_fields(ff_columns: list[FlowfileColumn]) -> [gw_schema.MutField]:
    return [convert_ff_column_to_gw_field(ff_column) for ff_column in ff_columns]
