from flowfile_core.schemas.transform_schema import FuzzyMatchInput, SelectInput, JoinInputs, FuzzyMatchInputManager
from flowfile_core.flowfile.flow_data_engine.join import verify_join_select_integrity, verify_join_map_integrity
import polars as pl
from typing import TYPE_CHECKING, Tuple, List

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine


def _order_join_inputs_based_on_col_order(col_order: List[str], join_inputs: JoinInputs) -> None:
    """
    Ensure that the select columns in the fuzzy match input match the order of the incoming columns.
    This function modifies the join_inputs object in-place.

    Returns:
        None
    """
    select_map = {select.old_name: select for select in join_inputs.renames}
    ordered_renames = [select_map[col] for col in col_order if col in select_map]
    join_inputs.renames = ordered_renames


def _ensure_all_columns_have_select(left: "FlowDataEngine",
                                    right: "FlowDataEngine",
                                    fuzzy_match_input_manager: FuzzyMatchInputManager):
    """
    Ensure that all columns in the left and right FlowDataEngines are included in the fuzzy match input's select
     statements.
    Args:
        left (FlowDataEngine): Left FlowDataEngine
        right (FlowDataEngine): Right FlowDataEngine
        fuzzy_match_input_manager (FuzzyMatchInputManager): Fuzzy match input manager

    Returns:
        None
    """
    right_cols_in_select = {c.old_name for c in fuzzy_match_input_manager.right_select.renames}
    left_cols_in_select = {c.old_name for c in fuzzy_match_input_manager.left_select.renames}

    fuzzy_match_input_manager.left_select.renames.extend(
        [SelectInput(col) for col in left.columns if col not in left_cols_in_select])
    fuzzy_match_input_manager.right_select.renames.extend(
        [SelectInput(col) for col in right.columns if col not in right_cols_in_select]
    )


def prepare_for_fuzzy_match(left: "FlowDataEngine", right: "FlowDataEngine",
                            fuzzy_match_input_manager: FuzzyMatchInputManager) -> Tuple[pl.LazyFrame, pl.LazyFrame]:
    """
    Prepare two FlowDataEngines for fuzzy matching.

    Args:
        left: Left FlowDataEngine for fuzzy join
        right: Right FlowDataEngine for fuzzy join
        fuzzy_match_input: Parameters for fuzzy matching configuration
    Returns:
        Tuple[pl.LazyFrame, pl.LazyFrame]: Prepared left and right lazy frames
    """
    left.lazy = True
    right.lazy = True
    _ensure_all_columns_have_select(left, right, fuzzy_match_input_manager)
    _order_join_inputs_based_on_col_order(left.columns, fuzzy_match_input_manager.left_select.join_inputs)
    _order_join_inputs_based_on_col_order(right.columns, fuzzy_match_input_manager.right_select.join_inputs)
    verify_join_select_integrity(fuzzy_match_input_manager.fuzzy_input, left_columns=left.columns, right_columns=right.columns)
    if not verify_join_map_integrity(fuzzy_match_input_manager.fuzzy_input, left_columns=left.schema,
                                     right_columns=right.schema):
        raise Exception('Join is not valid by the data fields')

    fuzzy_match_input_manager.auto_rename()

    right_select = [v.old_name for v in fuzzy_match_input_manager.right_select.renames if
                    (v.keep or v.join_key) and v.is_available]
    left_select = [v.old_name for v in fuzzy_match_input_manager.left_select.renames if
                   (v.keep or v.join_key) and v.is_available]
    left_df: pl.LazyFrame | pl.DataFrame = left.data_frame.select(left_select).rename(
        fuzzy_match_input_manager.left_select.rename_table)
    right_df: pl.LazyFrame | pl.DataFrame = right.data_frame.select(right_select).rename(
        fuzzy_match_input_manager.right_select.rename_table)
    return left_df, right_df
