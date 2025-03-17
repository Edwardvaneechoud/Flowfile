from logging import Logger
from typing import List, Dict, Tuple

import polars as pl

from flowfile_worker.polars_fuzzy_match.models import FuzzyMapping
from flowfile_worker.utils import collect_lazy_frame


def get_approx_uniqueness(lf: pl.LazyFrame) -> Dict[str, int]:
    """
    Calculate the approximate number of unique values for each column in a LazyFrame.

    Args:
        lf (pl.LazyFrame): Input LazyFrame to analyze.

    Returns:
        Dict[str, int]: Dictionary mapping column names to their approximate unique value counts.

    Raises:
        Exception: If the uniqueness calculation fails (empty result).
    """
    uniqueness = lf.select(pl.all().approx_n_unique()).collect().to_dicts()
    if len(uniqueness) == 0:
        raise Exception('Approximate uniqueness calculation failed')
    return uniqueness[0]


def calculate_uniqueness(a: float, b: float) -> float:
    """
    Calculate a combined uniqueness score from two individual uniqueness ratios.

    The formula prioritizes columns with high combined uniqueness while accounting for
    differences between the two input values.

    Args:
        a (float): First uniqueness ratio, typically from the left dataframe.
        b (float): Second uniqueness ratio, typically from the right dataframe.

    Returns:
        float: Combined uniqueness score.
    """
    return ((pow(a + 0.5, 2) + pow(b + 0.5, 2)) / 2 - pow(0.5, 2)) + 0.5 * abs(a - b)


def calculate_df_len(df: pl.LazyFrame) -> int:
    """
    Calculate the number of rows in a LazyFrame.

    Args:
        df (pl.LazyFrame): Input LazyFrame.

    Returns:
        int: Number of rows in the LazyFrame.
    """
    return collect_lazy_frame(df.select(pl.len()))[0, 0]


def fill_perc_unique_in_fuzzy_maps(left_df: pl.LazyFrame, right_df: pl.LazyFrame, fuzzy_maps: List[FuzzyMapping],
                                   flowfile_logger: Logger, left_len: int, right_len: int) -> List[FuzzyMapping]:
    """
    Calculate and set uniqueness percentages for all fuzzy mapping columns.

    Computes the approximate unique value counts in both dataframes for the columns
    specified in fuzzy_maps, then calculates a combined uniqueness score for each mapping.

    Args:
        left_df (pl.LazyFrame): Left dataframe.
        right_df (pl.LazyFrame): Right dataframe.
        fuzzy_maps (List[FuzzyMapping]): List of fuzzy mappings between left and right columns.
        flowfile_logger (Logger): Logger for information output.
        left_len (int): Number of rows in the left dataframe.
        right_len (int): Number of rows in the right dataframe.

    Returns:
        List[FuzzyMapping]: Updated fuzzy mappings with calculated uniqueness percentages.
    """
    left_unique_values = get_approx_uniqueness(left_df.select(fuzzy_map.left_col for fuzzy_map in fuzzy_maps))
    right_unique_values = get_approx_uniqueness(right_df.select(fuzzy_map.right_col for fuzzy_map in fuzzy_maps))
    flowfile_logger.info(f'Left unique values: {left_unique_values}')
    flowfile_logger.info(f'Right unique values: {right_unique_values}')
    for fuzzy_map in fuzzy_maps:
        fuzzy_map.perc_unique = calculate_uniqueness(left_unique_values[fuzzy_map.left_col] / left_len,
                                                     right_unique_values[fuzzy_map.right_col] / right_len)
    return fuzzy_maps


def determine_order_of_fuzzy_maps(fuzzy_maps: List[FuzzyMapping]) -> List[FuzzyMapping]:
    """
    Sort fuzzy mappings by their uniqueness percentages in descending order.

    This ensures that columns with higher uniqueness are prioritized in the
    fuzzy matching process.

    Args:
        fuzzy_maps (List[FuzzyMapping]): List of fuzzy mappings between columns.

    Returns:
        List[FuzzyMapping]: Sorted list of fuzzy mappings by uniqueness (highest first).
    """
    return sorted(fuzzy_maps, key=lambda x: x.perc_unique, reverse=True)


def calculate_uniqueness_rate(fuzzy_maps: List[FuzzyMapping]) -> float:
    """
    Calculate the total uniqueness rate across all fuzzy mappings.

    Args:
        fuzzy_maps (List[FuzzyMapping]): List of fuzzy mappings with calculated uniqueness.

    Returns:
        float: Sum of uniqueness percentages across all mappings.
    """
    return sum(jm.perc_unique for jm in fuzzy_maps)


def determine_need_for_aggregation(uniqueness_rate: float, cartesian_join_number: int) -> bool:
    """
    Determine if aggregation is needed based on uniqueness and potential join size.

    Aggregation helps prevent explosive cartesian joins when matching columns
    have low uniqueness, which could lead to performance issues.

    Args:
        uniqueness_rate (float): Total uniqueness rate across fuzzy mappings.
        cartesian_join_number (int): Potential size of the cartesian join (left_len * right_len).

    Returns:
        bool: True if aggregation is needed, False otherwise.
    """
    return uniqueness_rate < 1.2 and cartesian_join_number > 1_000_000


def aggregate_output(left_df: pl.LazyFrame, right_df: pl.LazyFrame,
                     fuzzy_maps: List[FuzzyMapping]) -> Tuple[pl.LazyFrame, pl.LazyFrame]:
    """
    Deduplicate the dataframes based on the fuzzy mapping columns.

    This reduces the size of the join by removing duplicate rows when the
    uniqueness rate is low and the potential join size is large.

    Args:
        left_df (pl.LazyFrame): Left dataframe.
        right_df (pl.LazyFrame): Right dataframe.
        fuzzy_maps (List[FuzzyMapping]): List of fuzzy mappings between columns.

    Returns:
        Tuple[pl.LazyFrame, pl.LazyFrame]: Deduplicated left and right dataframes.
    """
    left_df = left_df.unique([fuzzy_map.left_col for fuzzy_map in fuzzy_maps])
    right_df = right_df.unique([fuzzy_map.right_col for fuzzy_map in fuzzy_maps])
    return left_df, right_df


def report_on_order_of_fuzzy_maps(fuzzy_maps: List[FuzzyMapping], flowfile_logger: Logger) -> None:
    """
    Log the order of fuzzy mappings based on uniqueness.
    Parameters
    ----------
    fuzzy_maps: List[FuzzyMapping]
    flowfile_logger: Logger

    -------
    """
    flowfile_logger.info('Fuzzy mappings sorted by uniqueness')
    for i, fuzzy_map in enumerate(fuzzy_maps):
        flowfile_logger.info(f'{i}. Fuzzy mapping: {fuzzy_map.left_col} -> {fuzzy_map.right_col} '
                             f'Uniqueness: {fuzzy_map.perc_unique}')


def pre_process_for_fuzzy_matching(left_df: pl.LazyFrame, right_df: pl.LazyFrame,
                                   fuzzy_maps: List[FuzzyMapping],
                                   flowfile_logger: Logger) -> Tuple[pl.LazyFrame, pl.LazyFrame, List[FuzzyMapping]]:
    """
    Preprocess dataframes and fuzzy mappings for optimal fuzzy matching.

    This function:
    1. Calculates dataframe sizes
    2. Calculates uniqueness percentages for each fuzzy mapping
    3. Sorts the fuzzy mappings by uniqueness
    4. Determines if aggregation is needed to prevent large cartesian joins
    5. Performs aggregation if necessary

    Args:
        left_df (pl.LazyFrame): Left dataframe.
        right_df (pl.LazyFrame): Right dataframe.
        fuzzy_maps (List[FuzzyMapping]): List of fuzzy mappings between columns.
        flowfile_logger (Logger): Logger for information output.

    Returns:
        Tuple[pl.LazyFrame, pl.LazyFrame, List[FuzzyMapping]]:
            - Potentially modified left dataframe
            - Potentially modified right dataframe
            - Sorted and updated fuzzy mappings
    """
    flowfile_logger.info('Optimizing data and settings for fuzzy matching')
    left_df_len = calculate_df_len(left_df)
    right_df_len = calculate_df_len(right_df)
    if left_df_len == 0 or right_df_len == 0:
        return left_df, right_df, fuzzy_maps
    fuzzy_maps = fill_perc_unique_in_fuzzy_maps(left_df, right_df, fuzzy_maps, flowfile_logger, left_df_len,
                                                right_df_len)
    fuzzy_maps = determine_order_of_fuzzy_maps(fuzzy_maps)
    report_on_order_of_fuzzy_maps(fuzzy_maps, flowfile_logger)

    uniqueness_rate = calculate_uniqueness_rate(fuzzy_maps)
    flowfile_logger.info(f'Uniqueness rate: {uniqueness_rate}')
    if determine_need_for_aggregation(uniqueness_rate, left_df_len * right_df_len):
        flowfile_logger.warning('The join fields are not unique enough, resulting in many duplicates, '
                                'therefore removing duplicates on the join field')
        left_df, right_df = aggregate_output(left_df, right_df, fuzzy_maps)
    flowfile_logger.info('Data and settings optimized for fuzzy matching')
    return left_df, right_df, fuzzy_maps
