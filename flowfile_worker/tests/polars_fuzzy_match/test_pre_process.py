import sys
import os
import tempfile
from typing import List

# Set up import paths
try:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    sys.path.append(os.path.dirname(os.path.abspath('flowfile_worker/tests/polars_fuzzy_match/test_matcher.py')))

from match_utils import (generate_small_fuzzy_test_data_left, generate_small_fuzzy_test_data_right,
                         create_test_data, create_fuzzy_maps)

import polars as pl
import pytest
import logging

from flowfile_worker.polars_fuzzy_match.process import process_fuzzy_frames

# Import functions to test
from flowfile_worker.polars_fuzzy_match.pre_process import (
    get_approx_uniqueness,
    calculate_uniqueness,
    calculate_df_len,
    fill_perc_unique_in_fuzzy_maps,
    determine_order_of_fuzzy_maps,
    calculate_uniqueness_rate,
    determine_need_for_aggregation,
    aggregate_output,
    pre_process_for_fuzzy_matching
)


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing."""
    data = {
        "name": ["John", "Alice", "Bob", "Charlie"],
        "age": [30, 25, 35, 40],
        "city": ["New York", "Boston", "Chicago", "Seattle"],
        'country': ['USA', 'USA', 'USA', 'Canada']
    }
    return pl.DataFrame(data).lazy()


@pytest.fixture
def flow_logger():
    return logging.getLogger('sample')

@pytest.fixture
def temp_directory():
    """Create a real temporary directory that will be cleaned up after the test."""
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Created temporary directory: {temp_dir}")
        yield temp_dir
    print("Temporary directory cleaned up")


def test_get_approx_uniqueness(sample_dataframe):
    uniqueness = get_approx_uniqueness(sample_dataframe)
    assert uniqueness == {'name': 4, 'age': 4, 'city': 4, 'country': 2}


def test_calculate_uniqueness():
    assert calculate_uniqueness(0.5, 0.5) == 0.75
    assert calculate_uniqueness(0.6, 0.8) == 1.3000000000000003
    assert calculate_uniqueness(0.6, 0.5) == 0.905
    assert calculate_uniqueness(0.1, 0.3) == 0.35


def test_calculate_df_len(sample_dataframe):
    assert calculate_df_len(sample_dataframe) == 4


def test_fill_perc_unique_in_fuzzy_maps(sample_dataframe, flow_logger):
    left_df, right_df, fuzzy_maps = create_test_data()
    left_df_len = calculate_df_len(left_df)
    right_df_len = calculate_df_len(right_df)
    fuzzy_maps = fill_perc_unique_in_fuzzy_maps(left_df, right_df, fuzzy_maps, flow_logger, left_df_len, right_df_len)
    fuzzy_map_1 = fuzzy_maps[0]
    fuzzy_map_2 = fuzzy_maps[1]
    fuzzy_map_3 = fuzzy_maps[2]
    assert fuzzy_map_3.perc_unique < 1
    assert fuzzy_map_2.perc_unique > 1
    assert fuzzy_map_1.perc_unique > 1


def test_determine_order_of_fuzzy_maps():
    fuzzy_maps = create_fuzzy_maps()

    fuzzy_maps = determine_order_of_fuzzy_maps(fuzzy_maps)
    assert fuzzy_maps[0].perc_unique > fuzzy_maps[1].perc_unique > fuzzy_maps[2].perc_unique


def test_calculate_uniqueness_rate():
    fuzzy_maps = create_fuzzy_maps()
    uniqueness_score_full = calculate_uniqueness_rate(fuzzy_maps)
    uniqueness_score_first_two = calculate_uniqueness_rate(fuzzy_maps[:2])
    uniqueness_score_last_one = calculate_uniqueness_rate(fuzzy_maps[2:])
    assert uniqueness_score_full > 1.2
    assert uniqueness_score_last_one < 1.2
    assert uniqueness_score_first_two < uniqueness_score_full
    assert uniqueness_score_last_one < uniqueness_score_full


def test_determine_need_for_aggregation():
    uniqueness_score = 0.5
    assert determine_need_for_aggregation(uniqueness_score, 1_0200_000) is True
    assert determine_need_for_aggregation(uniqueness_score, 1_000_000) is False


def test_aggregate_output():
    left_df, right_df, fuzzy_maps = create_test_data()
    left_df_unique, right_df_unique = aggregate_output(left_df, right_df, fuzzy_maps[2:])
    left_n_vals = left_df.select(fuzzy_maps[2].left_col).unique().select(pl.len()).collect()[0, 0]
    right_n_vals = right_df.select(fuzzy_maps[2].right_col).unique().select(pl.len()).collect()[0, 0]
    assert left_df_unique.select(pl.len()).collect()[0, 0] == left_n_vals
    assert right_df_unique.select(pl.len()).collect()[0, 0] == right_n_vals


def test_process_fuzzy_mapping_no_uniqueness(flow_logger):
    left_df, right_df, mapping = create_test_data(100000)
    mapping = mapping[2:]

    left_df_prep, right_df_prep, mapping = pre_process_for_fuzzy_matching(left_df, right_df, mapping, flow_logger)
    assert left_df_prep.collect().shape[0] == left_df.select(mapping[0].left_col).unique().collect().shape[0]
    assert right_df_prep.collect().shape[0] == right_df.select(mapping[0].right_col).unique().collect().shape[0]
    assert left_df_prep.collect().shape[0] < left_df.select(mapping[0].left_col).collect().shape[0]
    assert right_df_prep.collect().shape[0] < right_df.select(mapping[0].right_col).collect().shape[0]

def test_process_fuzzy_mapping_uniqueness(flow_logger):
    left_df, right_df, mapping = create_test_data(100000)
    left_df_prep, right_df_prep, mapping = pre_process_for_fuzzy_matching(left_df, right_df, mapping, flow_logger)
    assert left_df_prep.collect().shape[0] == left_df.select(pl.first()).collect().shape[0]
    assert right_df_prep.collect().shape[0] == right_df.select(pl.first()).collect().shape[0]

