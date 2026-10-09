from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.schemas import transform_schema


def verify_join_select_integrity(
    join_input: transform_schema.JoinInput
    | transform_schema.CrossJoinInput
    | transform_schema.FuzzyMatchInput
    | transform_schema.JoinInputsManager,
    left_columns: list[str],
    right_columns: list[str],
):
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


def get_join_map_problems(
    join_input: transform_schema.JoinInput | transform_schema.FuzzyMatchInput | transform_schema.JoinInputManager,
    left_columns: list[FlowfileColumn],
    right_columns: list[FlowfileColumn],
) -> list[str]:
    """Collect human-readable reasons a join mapping is invalid.

    A join key referencing a column that no longer exists upstream, or a pair of
    keys with incompatible data types, each produces a message naming the column.

    Args:
        join_input: Join configuration with mappings between columns
        left_columns: Schema columns from left table
        right_columns: Schema columns from right table
    Returns:
        list[str]: One message per problem; empty when the mapping is valid.
    """
    problems: list[str] = []
    left_column_dict = {lc.name: lc for lc in left_columns}
    right_column_dict = {rc.name: rc for rc in right_columns}
    for join_mapping in join_input.join_mapping:
        left_column_info: FlowfileColumn | None = left_column_dict.get(join_mapping.left_col)
        right_column_info: FlowfileColumn | None = right_column_dict.get(join_mapping.right_col)
        if left_column_info is None:
            problems.append(f"left join key '{join_mapping.left_col}' no longer exists in the left input")
        if right_column_info is None:
            problems.append(f"right join key '{join_mapping.right_col}' no longer exists in the right input")
        if (
            left_column_info is not None
            and right_column_info is not None
            and left_column_info.generic_datatype() != right_column_info.generic_datatype()
        ):
            problems.append(
                f"join keys '{join_mapping.left_col}' ({left_column_info.generic_datatype()}) and "
                f"'{join_mapping.right_col}' ({right_column_info.generic_datatype()}) have incompatible types"
            )
    return problems


def get_shared_output_name_problems(rename_table: dict[str, str], side: str | None = None) -> list[str]:
    """One message per output name that more than one source column maps to.

    Args:
        rename_table: Source column → output name, as Polars' `.rename()` would receive it.
        side: Optional label ("left"/"right") naming the join side in the message.
    """
    sources_by_output: dict[str, list[str]] = {}
    for old_name, new_name in rename_table.items():
        sources_by_output.setdefault(new_name, []).append(old_name)
    label = f"{side} columns" if side else "columns"
    return [
        f"{label} {', '.join(repr(s) for s in sources)} share the output name '{new_name}'"
        for new_name, sources in sources_by_output.items()
        if len(sources) > 1
    ]


def _kept_output_names(side_manager: transform_schema.JoinInputsManager) -> set[str]:
    return {v.new_name for v in side_manager.select_inputs.renames if v.keep and v.is_available}


def get_duplicate_output_problems(
    manager: transform_schema.JoinInputManager | transform_schema.CrossJoinInputManager,
) -> list[str]:
    """Collect human-readable reasons the selected output names collide.

    Checked after any auto-rename: two columns on one side renamed to the same
    name, or a kept name present on both sides, would otherwise surface as a
    bare Polars duplicate-column error. Only available columns count, since an
    unavailable one is never selected.

    Returns:
        list[str]: One message per collision; empty when every output name is unique.
    """
    problems = get_shared_output_name_problems(manager.left_manager.get_rename_table(), "left")
    problems += get_shared_output_name_problems(manager.right_manager.get_rename_table(), "right")
    overlapping = sorted(_kept_output_names(manager.left_manager) & _kept_output_names(manager.right_manager))
    if overlapping:
        problems.append(f"columns {', '.join(repr(c) for c in overlapping)} are kept on both sides")
    return problems


def verify_join_map_integrity(
    join_input: transform_schema.JoinInput | transform_schema.FuzzyMatchInput | transform_schema.JoinInputManager,
    left_columns: list[FlowfileColumn],
    right_columns: list[FlowfileColumn],
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
    return len(get_join_map_problems(join_input, left_columns, right_columns)) == 0
