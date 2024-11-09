
from typing import List
from flowfile_core.schemas import transform_schema
from flowfile_core.flowfile.flowfile_table.flow_file_column.main import FlowFileColumn


def verify_join_select_integrity(join_input: transform_schema.JoinInput | transform_schema.CrossJoinInput,
                                 left_columns: List[str],
                                 right_columns: List[str]):
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
                              left_columns: List[FlowFileColumn],
                              right_columns: List[FlowFileColumn]
                              ):
    join_mappings = join_input.join_mapping
    left_column_dict = {lc.name: lc for lc in left_columns}
    right_column_dict = {rc.name: rc for rc in right_columns}
    for join_mapping in join_mappings:
        left_column_info: FlowFileColumn | None = left_column_dict.get(join_mapping.left_col)
        right_column_info: FlowFileColumn | None = right_column_dict.get(join_mapping.right_col)
        if not left_column_info or not right_column_info:
            return False
        if left_column_info.data_type != right_column_info.data_type:
            return False
    return True
