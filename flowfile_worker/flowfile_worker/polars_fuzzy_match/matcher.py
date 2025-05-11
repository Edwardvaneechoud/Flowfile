import polars as pl
from typing import List, Optional, Tuple
import tempfile
from logging import Logger

from flowfile_worker.polars_fuzzy_match.process import calculate_and_parse_fuzzy, process_fuzzy_frames
from flowfile_worker.polars_fuzzy_match.pre_process import pre_process_for_fuzzy_matching
from flowfile_worker.polars_fuzzy_match.models import FuzzyMapping
from flowfile_worker.polars_fuzzy_match.utils import cache_polars_frame_to_temp
from flowfile_worker.utils import collect_lazy_frame
import polars_simed as ps


HAS_POLARS_SIM = True


def ensure_left_is_larger(left_df: pl.DataFrame,
                          right_df: pl.DataFrame,
                          left_col_name: str,
                          right_col_name: str) -> tuple:
    """
    Ensures that the left dataframe is always the larger one.
    If the right dataframe is larger, swaps them.

    Args:
        left_df: The left dataframe
        right_df: The right dataframe
        left_col_name: Column name for the left dataframe
        right_col_name: Column name for the right dataframe

    Returns:
        tuple: (left_df, right_df, left_col_name, right_col_name)
    """
    left_frame_len = left_df.select(pl.len())[0, 0]
    right_frame_len = right_df.select(pl.len())[0, 0]

    # Swap dataframes if right is larger than left
    if right_frame_len > left_frame_len:
        return right_df, left_df, right_col_name, left_col_name

    return left_df, right_df, left_col_name, right_col_name


def split_dataframe(df: pl.DataFrame, max_chunk_size: int = 500_000) -> List[pl.DataFrame]:
    """
    Split a Polars DataFrame into multiple DataFrames with a maximum size.

    Args:
        df: The Polars DataFrame to split
        max_chunk_size: Maximum number of rows per chunk (default: 500,000)

    Returns:
        List of Polars DataFrames, each containing at most max_chunk_size rows
    """
    total_rows = df.select(pl.len())[0, 0]

    # If DataFrame is smaller than max_chunk_size, return it as is
    if total_rows <= max_chunk_size:
        return [df]

    # Calculate number of chunks needed
    num_chunks = (total_rows + max_chunk_size - 1) // max_chunk_size  # Ceiling division

    chunks = []
    for i in range(num_chunks):
        start_idx = i * max_chunk_size
        end_idx = min((i + 1) * max_chunk_size, total_rows)

        # Extract chunk using slice
        chunk = df.slice(start_idx, end_idx - start_idx)
        chunks.append(chunk)

    return chunks


def cross_join_large_files(left_fuzzy_frame: pl.LazyFrame,
                           right_fuzzy_frame: pl.LazyFrame,
                           left_col_name: str,
                           right_col_name: str,
                           flowfile_logger: Logger,
                           ) -> pl.LazyFrame:
    if not HAS_POLARS_SIM:
        raise Exception('The polars-sim library is required to perform this operation.')

    left_df = collect_lazy_frame(left_fuzzy_frame)
    right_df = collect_lazy_frame(right_fuzzy_frame)

    left_df, right_df, left_col_name, right_col_name = ensure_left_is_larger(
        left_df, right_df, left_col_name, right_col_name
    )
    left_chunks = split_dataframe(left_df, max_chunk_size=500_000)  # Reduced chunk size
    flowfile_logger.info(f"Splitting left dataframe into {len(left_chunks)} chunks.")
    df_matches = []

    # Process each chunk combination with error handling
    for i, left_chunk in enumerate(left_chunks):
        chunk_matches = ps.join_sim(
            left=left_chunk,
            right=right_df,
            left_on=left_col_name,
            right_on=right_col_name,
            top_n=100,
            add_similarity=False,
        )
        flowfile_logger.info(f"Processed chunk {int(i)} with {len(chunk_matches)} matches.")
        df_matches.append(chunk_matches)


    # Combine all matches
    if df_matches:
        return pl.concat(df_matches).lazy()
    else:
        columns = list(set(left_df.columns).union(set(right_df.columns)))
        return pl.DataFrame(schema={col: pl.Null for col in columns}).lazy()


def cross_join_small_files(left_df: pl.LazyFrame, right_df: pl.LazyFrame) -> pl.LazyFrame:
    return left_df.join(right_df, how='cross')


def cross_join_filter_existing_fuzzy_results(left_df: pl.LazyFrame, right_df: pl.LazyFrame,
                                             existing_matches: pl.LazyFrame,
                                             left_col_name: str, right_col_name: str):
    """
    Process and filter fuzzy matching results by joining dataframes using existing match indices.

    This function takes previously identified fuzzy matches (existing_matches) and performs
    a series of operations to create a refined dataset of matches between the left and right
    dataframes, preserving index relationships.

    Parameters:
    -----------
    left_df : pl.LazyFrame
        The left dataframe containing records to be matched.
    right_df : pl.LazyFrame
        The right dataframe containing records to be matched against.
    existing_matches : pl.LazyFrame
        A dataframe containing the indices of already identified matches between
        left_df and right_df, with columns '__left_index' and '__right_index'.
    left_col_name : str
        The column name from left_df to include in the result.
    right_col_name : str
        The column name from right_df to include in the result.

    Returns:
    --------
    pl.LazyFrame
        A dataframe containing the unique matches between left_df and right_df,
        with index information for both dataframes preserved. The resulting dataframe
        includes the specified columns from both dataframes along with their respective
        index aggregations.

    Notes:
    ------
    The function performs these operations:
    1. Join existing matches with both dataframes using their respective indices
    2. Select only the relevant columns and remove duplicates
    3. Create aggregations that preserve the relationship between values and their indices
    4. Join these aggregations back to create the final result set
    """
    joined_df = (existing_matches
                 .select(['__left_index', '__right_index'])
                 .join(left_df, on='__left_index')
                 .join(right_df, on='__right_index')
                 .select(left_col_name, right_col_name, '__left_index', '__right_index')
                 )
    return joined_df.group_by([left_col_name, right_col_name]).agg('__left_index', '__right_index')


def cross_join_no_existing_fuzzy_results(left_df: pl.LazyFrame, right_df: pl.LazyFrame, left_col_name: str,
                                         right_col_name: str, temp_dir_ref: str,
                                         flowfile_logger: Logger) -> pl.LazyFrame:
    """
    Generate fuzzy matching results by performing a cross join between dataframes.

    This function processes the input dataframes, determines the appropriate cross join method
    based on the size of the resulting cartesian product, and returns the cross-joined results
    for fuzzy matching when no existing matches are provided.

    Parameters:
    -----------
    left_df : pl.LazyFrame
        The left dataframe containing records to be matched.
    right_df : pl.LazyFrame
        The right dataframe containing records to be matched against.
    left_col_name : str
        The column name from left_df to use for fuzzy matching.
    right_col_name : str
        The column name from right_df to use for fuzzy matching.
    temp_dir_ref : str
        Reference to a temporary directory where intermediate results can be stored
        during processing of large dataframes.

    Returns:
    --------
    pl.LazyFrame
        A dataframe containing the cross join results of left_df and right_df,
        prepared for fuzzy matching operations.

    Notes:
    ------
    The function performs these operations:
    1. Processes input frames using the process_fuzzy_frames helper function
    2. Calculates the size of the cartesian product to determine processing approach
    3. Uses either cross_join_large_files or cross_join_small_files based on the size:
       - For cartesian products > 100M but < 1T (or 10M without polars-sim), uses large file method
       - For smaller products, uses the small file method
    4. Raises an exception if the cartesian product exceeds the maximum allowed size

    Raises:
    -------
    Exception
        If the cartesian product of the two dataframes exceeds the maximum allowed size
        (1 trillion with polars-sim, 100 million without).
    """
    (left_fuzzy_frame,
     right_fuzzy_frame,
     left_col_name,
     right_col_name,
     len_left_df,
     len_right_df) = process_fuzzy_frames(left_df=left_df, right_df=right_df, left_col_name=left_col_name,
                                          right_col_name=right_col_name, temp_dir_ref=temp_dir_ref)
    cartesian_size = len_left_df * len_right_df
    max_size = 100_000_000_000_000 if HAS_POLARS_SIM else 10_000_000
    if cartesian_size > max_size:
        flowfile_logger.error(f'The cartesian product of the two dataframes is too large to process: {cartesian_size}')
        raise Exception('The cartesian product of the two dataframes is too large to process.')
    if cartesian_size > 100_000_000:
        flowfile_logger.info('Performing approximate fuzzy match for large dataframes to reduce memory usage.')
        cross_join_frame = cross_join_large_files(left_fuzzy_frame, right_fuzzy_frame, left_col_name=left_col_name,
                                                  right_col_name=right_col_name, flowfile_logger=flowfile_logger)
    else:
        cross_join_frame = cross_join_small_files(left_fuzzy_frame, right_fuzzy_frame)
    return cross_join_frame


def unique_df_large(_df: pl.DataFrame | pl.LazyFrame, cols: Optional[List[str]] = None) -> pl.DataFrame:
    """
    Efficiently compute unique rows in large dataframes by partitioning.

    This function processes large dataframes by first partitioning them by a selected column,
    then finding unique combinations within each partition before recombining the results.
    This approach is more memory-efficient for large datasets than calling .unique() directly.

    Parameters:
    -----------
    _df : pl.DataFrame | pl.LazyFrame
        The input dataframe to process. Can be either a Polars DataFrame or LazyFrame.
    cols : Optional[List[str]]
        The list of columns to consider when finding unique rows. If None, all columns
        are used. The first column in this list is used as the partition column.

    Returns:
    --------
    pl.DataFrame
        A dataframe containing only the unique rows from the input dataframe,
        based on the specified columns.

    Notes:
    ------
    The function performs these operations:
    1. Converts LazyFrame to DataFrame if necessary
    2. Partitions the dataframe by the first column in cols (or the first column of the dataframe if cols is None)
    3. Applies the unique operation to each partition based on the remaining columns
    4. Concatenates the results back into a single dataframe
    5. Frees memory by deleting intermediate objects

    This implementation uses tqdm to provide a progress bar during processing,
    which is particularly helpful for large datasets where the operation may take time.
    """
    if isinstance(_df, pl.LazyFrame):
        _df = collect_lazy_frame(_df)
    from tqdm import tqdm
    partition_col = cols[0] if cols is not None else _df.columns[0]
    other_cols = cols[1:] if cols is not None else _df.columns[1:]
    partitioned_df = _df.partition_by(partition_col)
    df = pl.concat([partition.unique(other_cols) for partition in tqdm(partitioned_df)])
    del partitioned_df, _df
    return df


def combine_matches(matching_dfs: List[pl.LazyFrame]):
    all_matching_indexes = matching_dfs[-1].select('__left_index', '__right_index')
    for matching_df in matching_dfs:
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
        flowfile_logger: Logger,
        existing_number_of_matches: Optional[int] = None
) -> Tuple[pl.LazyFrame, int]:
    """
    Process a single fuzzy mapping to generate matching dataframes.

    Args:
        fuzzy_map: The fuzzy mapping configuration containing match columns and thresholds
        left_df: Left dataframe with index column
        right_df: Right dataframe with index column
        existing_matches: Previously computed matches (or None)
        local_temp_dir_ref: Temporary directory reference for caching interim results
        i: Index of the current fuzzy mapping
        flowfile_logger: Logger instance for progress tracking
        existing_number_of_matches: Number of existing matches (if available)

    Returns:
        Tuple[pl.LazyFrame, int]: The final matching dataframe and the number of matches
    """
    # Determine join strategy based on existing matches
    if existing_matches is not None:
        existing_matches = existing_matches.select('__left_index', '__right_index')
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
            temp_dir_ref=local_temp_dir_ref,
            flowfile_logger=flowfile_logger
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
    if existing_matches is not None:
        matching_df = matching_df.join(existing_matches, on=['__left_index', '__right_index'])
    matching_df = cache_polars_frame_to_temp(matching_df, local_temp_dir_ref)
    if existing_number_of_matches is None or existing_number_of_matches > 100_000_000:
        existing_number_of_matches = matching_df.select(pl.len()).collect()[0, 0]
    if existing_number_of_matches > 100_000_000:
        return unique_df_large(matching_df.rename({'s': f'fuzzy_score_{i}'})).lazy(), existing_number_of_matches
    else:
        return matching_df.rename({'s': f'fuzzy_score_{i}'}).unique(), existing_number_of_matches


def perform_all_fuzzy_matches(left_df: pl.LazyFrame,
                              right_df: pl.LazyFrame,
                              fuzzy_maps: List[FuzzyMapping],
                              flowfile_logger: Logger,
                              local_temp_dir_ref: str,
                              ) -> List[pl.LazyFrame]:
    matching_dfs = []
    existing_matches = None
    existing_number_of_matches = None
    for i, fuzzy_map in enumerate(fuzzy_maps):
        existing_matches, existing_number_of_matches = process_fuzzy_mapping(
            fuzzy_map=fuzzy_map,
            left_df=left_df,
            right_df=right_df,
            existing_matches=existing_matches,
            local_temp_dir_ref=local_temp_dir_ref,
            i=i,
            flowfile_logger=flowfile_logger,
            existing_number_of_matches=existing_number_of_matches
        )
        matching_dfs.append(existing_matches)
    return matching_dfs


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
    left_df, right_df, fuzzy_maps = pre_process_for_fuzzy_matching(left_df, right_df, fuzzy_maps, flowfile_logger)

    # Create a temporary directory for caching intermediate results
    local_temp_dir = tempfile.TemporaryDirectory()
    local_temp_dir_ref = local_temp_dir.name

    # Add index columns to both dataframes
    left_df = add_index_column(left_df, '__left_index', local_temp_dir_ref)
    right_df = add_index_column(right_df, '__right_index', local_temp_dir_ref)

    matching_dfs = perform_all_fuzzy_matches(left_df, right_df, fuzzy_maps, flowfile_logger, local_temp_dir_ref)

    # Combine all matches
    if len(matching_dfs) > 1:
        flowfile_logger.info('Combining fuzzy matches')
        all_matches_df = combine_matches(matching_dfs)
    else:
        flowfile_logger.info('Caching fuzzy matches')
        all_matches_df = cache_polars_frame_to_temp(matching_dfs[0], local_temp_dir_ref)

    # Join matches with original dataframes
    flowfile_logger.info('Joining fuzzy matches with original dataframes')
    output_df = collect_lazy_frame(
        (left_df.join(all_matches_df, on='__left_index')
         .join(right_df, on='__right_index')
         .drop('__right_index', '__left_index'))
    )

    # Clean up temporary files
    flowfile_logger.info('Cleaning up temporary files')
    local_temp_dir.cleanup()

    return output_df
