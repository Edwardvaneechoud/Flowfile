from flowfile_core.configs import logger
from flowfile_core.schemas.transform_schema import FuzzyMatchInput
from flowfile_core.flowfile.flowfile_table.join import verify_join_select_integrity, verify_join_map_integrity
import polars as pl
from typing import Tuple
from flowfile_core.flowfile.flowfile_table.fuzzy_matching.settings_validator import finetune_fuzzy_mapping_settings


def prepare_for_fuzzy_match(left: "FlowFileTable", right: "FlowFileTable",
                            fuzzy_match_input: FuzzyMatchInput) -> Tuple[pl.LazyFrame, pl.LazyFrame]:
    left.calculate_schema()
    right.calculate_schema()
    left.lazy = True
    right.lazy = True
    verify_join_select_integrity(fuzzy_match_input, left_columns=left.columns, right_columns=right.columns)
    if not verify_join_map_integrity(fuzzy_match_input, left_columns=left.schema, right_columns=right.schema):
        raise Exception('Join is not valid by the data fields')
    fuzzy_match_input.auto_rename()
    finetune_fuzzy_mapping_settings(left_schema=left.schema, right_schema=right.schema, fuzzy_match_input=fuzzy_match_input)
    if fuzzy_match_input.aggregate_output and len(left) * len(right) > 1_000_000:
        left_df = left.data_frame.unique([jm.left_col for jm in fuzzy_match_input.join_mapping])
        right_df = right.data_frame.unique([jm.right_col for jm in fuzzy_match_input.join_mapping])
        logger.warning('The join fields are not unique enough, '
                       'resulting in many duplicates, therefore removing duplicates on the join field')
    else:
        left_df = left.data_frame
        right_df = right.data_frame
    right_select = [v.old_name for v in fuzzy_match_input.right_select.renames if (v.keep or v.join_key) and v.is_available]
    left_select = [v.old_name for v in fuzzy_match_input.left_select.renames if (v.keep or v.join_key) and v.is_available]
    left_df: pl.LazyFrame | pl.DataFrame = left_df.select(left_select).rename(fuzzy_match_input.left_select.rename_table)
    right_df: pl.LazyFrame | pl.DataFrame = right_df.select(right_select).rename(fuzzy_match_input.right_select.rename_table)
    return left_df, right_df
