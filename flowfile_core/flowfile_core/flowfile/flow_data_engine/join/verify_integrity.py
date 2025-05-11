
from typing import List
from flowfile_core.schemas import transform_schema
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn


def verify_join_select_integrity(join_input: transform_schema.JoinInput | transform_schema.CrossJoinInput,
                                 left_columns: List[str],
                                 right_columns: List[str]):
    """
    Verify column availability for join selection and update availability flags.

    Args:
        join_input: Join configuration input containing column selections
        left_columns: List of available column names in left table
        right_columns: List of available column names in right table
    """
    for c in join_input.left_select.renames:
        if c.old_name not in left_columns:
            c.is_available = False
        else:
            c.is_available = True
    for c in join_input.right_select.renames:
        if c.old_name not in right_columns:
            c.is_available = False
        else:
            c.is_available = True


def verify_join_map_integrity(join_input: transform_schema.JoinInput,
                              left_columns: List[FlowfileColumn],
                              right_columns: List[FlowfileColumn]
                              ):
    """
    Verify data type compatibility for join mappings between tables.

    Args:
        join_input: Join configuration with mappings between columns
        left_columns: Schema columns from left table
        right_columns: Schema columns from right table
    Returns:
        bool: True if join mapping is valid, False otherwise
    """
    join_mappings = join_input.join_mapping
    left_column_dict = {lc.name: lc for lc in left_columns}
    right_column_dict = {rc.name: rc for rc in right_columns}
    for join_mapping in join_mappings:
        left_column_info: FlowfileColumn | None = left_column_dict.get(join_mapping.left_col)
        right_column_info: FlowfileColumn | None = right_column_dict.get(join_mapping.right_col)
        if not left_column_info or not right_column_info:
            return False
        if left_column_info.generic_datatype() != right_column_info.generic_datatype():
            return False
    return True