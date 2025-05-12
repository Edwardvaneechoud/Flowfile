from flowfile_core.schemas.transform_schema import FuzzyMatchInput
from flowfile_core.flowfile.flow_data_engine.join import verify_join_select_integrity, verify_join_map_integrity
import polars as pl
from typing import TYPE_CHECKING, Tuple

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine


def prepare_for_fuzzy_match(left: "FlowDataEngine", right: "FlowDataEngine",
                            fuzzy_match_input: FuzzyMatchInput) -> Tuple[pl.LazyFrame, pl.LazyFrame]:
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
    verify_join_select_integrity(fuzzy_match_input, left_columns=left.columns, right_columns=right.columns)
    if not verify_join_map_integrity(fuzzy_match_input, left_columns=left.schema, right_columns=right.schema):
        raise Exception('Join is not valid by the data fields')
    fuzzy_match_input = fuzzy_match_input
    fuzzy_match_input.auto_rename()
    right_select = [v.old_name for v in fuzzy_match_input.right_select.renames if
                    (v.keep or v.join_key) and v.is_available]
    left_select = [v.old_name for v in fuzzy_match_input.left_select.renames if
                   (v.keep or v.join_key) and v.is_available]
    left_df: pl.LazyFrame | pl.DataFrame = left.data_frame.select(left_select).rename(
        fuzzy_match_input.left_select.rename_table)
    right_df: pl.LazyFrame | pl.DataFrame = right.data_frame.select(right_select).rename(
        fuzzy_match_input.right_select.rename_table)
    return left_df, right_df
