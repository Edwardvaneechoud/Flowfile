# Standard library imports
from typing import Dict, Tuple, TypeVar

# Third-party imports
import polars as pl

from flowfile_core.schemas import (
    transform_schema as transform_schemas
)

T = TypeVar('T', pl.DataFrame, pl.LazyFrame)


def rename_df_table_for_join(left_df: T, right_df: T,  join_key_rename: transform_schemas.FullJoinKeyResponse) -> Tuple[T, T]:
    return (left_df.rename({r[0]: r[1] for r in join_key_rename.left.join_key_renames}),
            right_df.rename({r[0]: r[1] for r in join_key_rename.right.join_key_renames}))


def get_undo_rename_mapping_join(join_input: transform_schemas.JoinInputManager) -> Dict[str, str]:
    join_key_rename = join_input.get_join_key_renames(True)
    return {r[1]: r[0] for r in join_key_rename.right.join_key_renames + join_key_rename.left.join_key_renames}


def get_col_name_to_delete(col: transform_schemas.SelectInput, side: transform_schemas.SideLit):
    return col.new_name if not col.join_key else transform_schemas.construct_join_key_name(side, col.new_name)
