import polars as pl
import polars_distance as pld
from flowfile_worker.polars_fuzzy_match.utils import cache_polars_frame_to_temp
from flowfile_worker.utils import collect_lazy_frame
from flowfile_worker.polars_fuzzy_match.models import FuzzyTypeLiteral


def calculate_fuzzy_score(mapping_table: pl.LazyFrame, left_col_name: str, right_col_name: str,
                          fuzzy_method: FuzzyTypeLiteral, th_score: float) -> pl.LazyFrame:
    """
    Calculate fuzzy matching scores between columns in a LazyFrame.

    Args:
        mapping_table: The DataFrame containing columns to compare
        left_col_name: Name of the left column for comparison
        right_col_name: Name of the right column for comparison
        fuzzy_method: Type of fuzzy matching algorithm to use
        th_score: The threshold score for fuzzy matching

    Returns:
        A LazyFrame with fuzzy matching scores
    """
    mapping_table = mapping_table.with_columns(pl.col(left_col_name).str.to_lowercase().alias('left'),
                                               pl.col(right_col_name).str.to_lowercase().alias('right'))
    dist_col = pld.DistancePairWiseString(pl.col('left'))
    if fuzzy_method in ("jaro_winkler"):
        fm_method = getattr(dist_col, fuzzy_method)(pl.col('right')).alias('s')
    else:
        fm_method = getattr(dist_col, fuzzy_method)(pl.col('right'), normalized=True).alias('s')
    return (mapping_table.with_columns(fm_method).drop(['left', 'right']).filter(pl.col('s') <= th_score).
            with_columns((1-pl.col('s')).alias('s')))


def process_fuzzy_frames(left_df: pl.LazyFrame, right_df: pl.LazyFrame, left_col_name: str, right_col_name: str,
                         temp_dir_ref: str):
    """
    Process left and right data frames to create fuzzy frames,
    cache them temporarily, and adjust based on their lengths.

    Args:
    - left_df (pl.DataFrame): The left data frame.
    - right_df (pl.DataFrame): The right data frame.
    - fm (object): An object containing configuration such as the left column name.
    - temp_dir_ref (str): A reference to the temporary directory for caching frames.

    Returns:
    - Tuple[pl.DataFrame, pl.DataFrame, str, str]: Processed left and right fuzzy frames and their respective column names.
    """

    # Process left and right data frames
    left_fuzzy_frame = cache_polars_frame_to_temp(left_df.group_by(left_col_name).agg('__left_index').
                                                  filter(pl.col(left_col_name).is_not_null()), temp_dir_ref)
    right_fuzzy_frame = cache_polars_frame_to_temp(right_df.group_by(right_col_name).agg('__right_index').
                                                   filter(pl.col(right_col_name).is_not_null()), temp_dir_ref)
    # Calculate lengths of fuzzy frames
    len_left_df = collect_lazy_frame(left_fuzzy_frame.select(pl.len()))[0, 0]
    len_right_df = collect_lazy_frame(right_fuzzy_frame.select(pl.len()))[0, 0]

    # Decide which frame to use as left or right based on their lengths
    if len_left_df < len_right_df:
        # Swap the frames and column names if right frame is larger
        left_fuzzy_frame, right_fuzzy_frame = right_fuzzy_frame, left_fuzzy_frame
        left_col_name, right_col_name = right_col_name, left_col_name

    # Return the processed frames and column names
    return left_fuzzy_frame, right_fuzzy_frame, left_col_name, right_col_name, len_left_df, len_right_df


def calculate_and_parse_fuzzy(mapping_table: pl.LazyFrame, left_col_name: str, right_col_name: str,
                              fuzzy_method: FuzzyTypeLiteral, th_score: float) -> pl.LazyFrame:
    """
    Calculate fuzzy scores and parse/explode the results for further processing.

    Args:
        mapping_table: The DataFrame containing columns to compare
        left_col_name: Name of the left column for comparison
        right_col_name: Name of the right column for comparison
        fuzzy_method: Type of fuzzy matching algorithm to use
        th_score: Minimum similarity score threshold (0-1)

    Returns:
        A LazyFrame with exploded indices and fuzzy scores
    """
    return calculate_fuzzy_score(mapping_table, left_col_name, right_col_name, fuzzy_method, th_score).select(
        pl.col('s'), pl.col('__left_index'), pl.col('__right_index')).explode(pl.col('__left_index')).explode(
        pl.col('__right_index'))
