import polars as pl
from typing import List, Optional, Dict, Tuple
import tempfile

from flowfile_worker.polars_fuzzy_match.process import calculate_and_parse_fuzzy, process_fuzzy_frames
from flowfile_worker.polars_fuzzy_match.models import FuzzyMapping
from flowfile_worker.polars_fuzzy_match.utils import cache_polars_frame_to_temp
from flowfile_worker.polars_fuzzy_match.polars_sim_mock import PolarsSim
from logging import Logger
from flowfile_worker.utils import collect_lazy_frame

try:
    import polars_sim as ps
    HAS_POLARS_SIM = True
except ImportError:
    HAS_POLARS_SIM = False
    ps = PolarsSim()


def cross_join_large_files(left_fuzzy_frame: pl.LazyFrame,
                           right_fuzzy_frame: pl.LazyFrame,
                           left_col_name: str,
                           right_col_name: str
                           ) -> pl.LazyFrame:
    if not HAS_POLARS_SIM:
        raise Exception('The polars-sim library is required to perform this operation.')
    matches: pl.DataFrame = ps.join_sim(left=collect_lazy_frame(left_fuzzy_frame),
                                        right=collect_lazy_frame(right_fuzzy_frame),
                                        right_on=right_col_name,
                                        left_on=left_col_name,
                                        ntop=100,
                                        add_similarity=False)
    return matches.lazy()


def cross_join_small_files(left_df: pl.LazyFrame, right_df: pl.LazyFrame) -> pl.LazyFrame:
    return left_df.join(right_df, how='cross')


def cross_join_filter_existing_fuzzy_results(left_df: pl.LazyFrame, right_df: pl.LazyFrame,
                                             existing_matches: pl.LazyFrame,
                                             left_col_name: str, right_col_name: str):
    joined_df = left_df.join(existing_matches.select(['__left_index', '__right_index']),
                             on='__left_index').join(right_df, on='__right_index').select(left_col_name,
                                                                                          right_col_name).unique()
    left_df_agg = left_df.group_by(left_col_name).agg('__left_index')
    right_df_agg = right_df.group_by(right_col_name).agg('__right_index')
    return joined_df.join(left_df_agg, on=left_col_name).join(right_df_agg, on=right_col_name)


def cross_join_no_existing_fuzzy_results(left_df: pl.LazyFrame, right_df: pl.LazyFrame, left_col_name: str,
                                         right_col_name: str, temp_dir_ref: str):
    (left_fuzzy_frame,
     right_fuzzy_frame,
     left_col_name,
     right_col_name,
     len_left_df,
     len_right_df) = process_fuzzy_frames(left_df=left_df, right_df=right_df, left_col_name=left_col_name,
                                          right_col_name=right_col_name, temp_dir_ref=temp_dir_ref)
    cartesian_size = len_left_df * len_right_df
    max_size = 1_000_000_000_000 if HAS_POLARS_SIM else 10_000_000
    if cartesian_size > max_size:
        raise Exception('The cartesian product of the two dataframes is too large to process.')
    if cartesian_size > 10_000_000:
        cross_join_frame = cross_join_large_files(left_fuzzy_frame, right_fuzzy_frame, left_col_name=left_col_name,
                                                  right_col_name=right_col_name)
    else:
        cross_join_frame = cross_join_small_files(left_fuzzy_frame, right_fuzzy_frame)
    return cross_join_frame


def unique_df_large(_df: pl.DataFrame | pl.LazyFrame, cols: Optional[List[str]] = None) -> pl.DataFrame:
    if isinstance(_df, pl.LazyFrame):
        _df = collect_lazy_frame(_df)
    from tqdm import tqdm
    partition_col = cols[0] if cols is not None else _df.columns[0]
    other_cols = cols[1:] if cols is not None else _df.columns[1:]
    partitioned_df = _df.partition_by(partition_col)
    df = pl.concat([partition.unique(other_cols) for partition in tqdm(partitioned_df)])
    del partitioned_df, _df
    return df


def combine_matches(matching_dfs: List[pl.LazyFrame], temp_dir_ref: str):
    outcome_dfs = []
    for matching_df in matching_dfs:
        new_df = matching_df
        for other_matching_df in matching_dfs:
            if id(matching_df) != id(other_matching_df):
                new_df = new_df.join(other_matching_df.select('__left_index', '__right_index'),
                                     on=['__left_index', '__right_index'])
        outcome_dfs.append(cache_polars_frame_to_temp(new_df, temp_dir_ref))
    concat_mappings = pl.concat([df.select('__left_index', '__right_index') for df in outcome_dfs])
    count = collect_lazy_frame(concat_mappings.select(pl.len()))[0, 0]
    if count > 10_000_000:
        all_matching_indexes = unique_df_large(concat_mappings).lazy()
    else:
        all_matching_indexes = concat_mappings.unique(['__left_index', '__right_index'])
    for matching_df in outcome_dfs:
        all_matching_indexes = all_matching_indexes.join(matching_df, on=['__left_index', '__right_index'])
    return all_matching_indexes


def add_index_column(df: pl.LazyFrame, column_name: str, tempdir: str):
    return cache_polars_frame_to_temp(df.with_row_index(name=column_name), tempdir)


def process_fuzzy_mapping(
        fuzzy_map: FuzzyMapping,
        left_df: pl.LazyFrame,
        right_df: pl.LazyFrame,
        existing_matches: Optional[pl.LazyFrame],
        local_temp_dir_ref: str,
        i: int,
        flowfile_logger: Logger
) -> pl.LazyFrame:
    """
    Process a single fuzzy mapping to generate matching dataframes.

    Args:
        fuzzy_map: The fuzzy mapping configuration containing match columns and thresholds
        left_df: Left dataframe with index column
        right_df: Right dataframe with index column
        existing_matches: Previously computed matches (or None)
        local_temp_dir_ref: Temporary directory reference for caching interim results
        matching_dfs: List of matching dataframes (will be modified)
        i: Index of the current fuzzy mapping
        flowfile_logger: Logger instance for progress tracking

    Returns:
        tuple: A tuple containing:
            - updated existing_matches (Optional[pl.LazyFrame]): The latest matches for filtering
            - updated matching_dfs (List[pl.LazyFrame]): The list of all matching dataframes
    """
    # Determine join strategy based on existing matches
    if existing_matches is not None:
        flowfile_logger.info(f'Filtering existing fuzzy matches for {fuzzy_map.left_col} and {fuzzy_map.right_col}')
        cross_join_frame = cross_join_filter_existing_fuzzy_results(
            left_df=left_df,
            right_df=right_df,
            existing_matches=existing_matches,
            left_col_name=fuzzy_map.left_col,
            right_col_name=fuzzy_map.right_col
        )
    else:
        flowfile_logger.info(f'Performing fuzzy match for {fuzzy_map.left_col} and {fuzzy_map.right_col}')
        cross_join_frame = cross_join_no_existing_fuzzy_results(
            left_df=left_df,
            right_df=right_df,
            left_col_name=fuzzy_map.left_col,
            right_col_name=fuzzy_map.right_col,
            temp_dir_ref=local_temp_dir_ref
        )

    # Calculate fuzzy match scores
    flowfile_logger.info(f'Calculating fuzzy match for {fuzzy_map.left_col} and {fuzzy_map.right_col}')
    matching_df = calculate_and_parse_fuzzy(
        mapping_table=cross_join_frame,
        left_col_name=fuzzy_map.left_col,
        right_col_name=fuzzy_map.right_col,
        fuzzy_method=fuzzy_map.fuzzy_type,
        th_score=fuzzy_map.reversed_threshold_score
    )
    return matching_df.rename({'s': f'fuzzy_score_{i}'})


def fuzzy_match_dfs(
        left_df: pl.LazyFrame,
        right_df: pl.LazyFrame,
        fuzzy_maps: List[FuzzyMapping],
        flowfile_logger: Logger
) -> pl.DataFrame:
    """
    Perform fuzzy matching between two dataframes using multiple fuzzy mapping configurations.

    Args:
        left_df: Left dataframe to be matched
        right_df: Right dataframe to be matched
        fuzzy_maps: List of fuzzy mapping configurations
        flowfile_logger: Logger instance for tracking progress

    Returns:
        pl.DataFrame: The final matched dataframe with all fuzzy scores
    """
    matching_dfs: List[pl.LazyFrame] = []
    local_temp_dir = tempfile.TemporaryDirectory()
    local_temp_dir_ref = local_temp_dir.name

    # Add index columns to both dataframes
    left_df = add_index_column(left_df, '__left_index', local_temp_dir_ref)
    right_df = add_index_column(right_df, '__right_index', local_temp_dir_ref)

    existing_matches: Optional[pl.LazyFrame] = None

    # Process each fuzzy mapping
    for i, fuzzy_map in enumerate(fuzzy_maps):
        existing_matches = process_fuzzy_mapping(
            fuzzy_map=fuzzy_map,
            left_df=left_df,
            right_df=right_df,
            existing_matches=existing_matches,
            local_temp_dir_ref=local_temp_dir_ref,
            i=i,
            flowfile_logger=flowfile_logger
        )
        matching_dfs.append(existing_matches)

    # Combine all matches
    if len(matching_dfs) > 1:
        flowfile_logger.info('Combining fuzzy matches')
        all_mappings_df = combine_matches(matching_dfs, local_temp_dir_ref)
    else:
        flowfile_logger.info('Caching fuzzy matches')
        all_mappings_df = cache_polars_frame_to_temp(matching_dfs[0], local_temp_dir_ref)

    # Join matches with original dataframes
    flowfile_logger.info('Joining fuzzy matches with original dataframes')
    output_df = collect_lazy_frame(
        (left_df.join(all_mappings_df, on='__left_index')
         .join(right_df, on='__right_index')
         .drop('__right_index', '__left_index'))
    )

    # Clean up temporary files
    flowfile_logger.info('Cleaning up temporary files')
    local_temp_dir.cleanup()

    return output_df
