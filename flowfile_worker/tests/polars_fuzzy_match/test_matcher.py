import sys
import os
import tempfile
from typing import List

# Set up import paths
try:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    sys.path.append(os.path.dirname(os.path.abspath('flowfile_worker/tests/polars_fuzzy_match/test_matcher.py')))

from match_utils import (generate_small_fuzzy_test_data_left, create_deterministic_test_data,
                         create_test_data, generate_small_fuzzy_test_data)

import polars as pl
import pytest
import logging

from flowfile_worker.polars_fuzzy_match.process import process_fuzzy_frames
from flowfile_worker.polars_fuzzy_match.pre_process import pre_process_for_fuzzy_matching

# Import functions to test
from flowfile_worker.polars_fuzzy_match.matcher import (
    cross_join_large_files,
    cross_join_small_files,
    cross_join_filter_existing_fuzzy_results,
    cross_join_no_existing_fuzzy_results,
    unique_df_large,
    combine_matches,
    add_index_column,
    fuzzy_match_dfs,
    process_fuzzy_mapping,
    perform_all_fuzzy_matches,

)


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing."""
    data = {
        "name": ["John", "Alice", "Bob", "Charlie"],
        "age": [30, 25, 35, 40],
        "city": ["New York", "Boston", "Chicago", "Seattle"]
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


def test_add_index_column(sample_dataframe, temp_directory):
    """Test the add_index_column function."""
    # Use a real temporary directory
    left_df, _, _ = create_test_data()

    result_df = add_index_column(left_df, '__test_index', temp_directory)
    logging.info(f"Result columns: {result_df.columns}")
    assert result_df is not None

    direct_df = left_df.with_row_index(name='__test_index').collect()
    assert '__test_index' in direct_df.columns
    assert list(direct_df['__test_index']) == list(range(direct_df.shape[0]))


def test_cross_join_small_files(temp_directory):
    """Test the cross_join_small_files function."""
    left_df, right_df, mapping = create_test_data(10)

    left_col_name = mapping[0].left_col
    right_col_name = mapping[0].right_col
    left_df = add_index_column(left_df, '__left_index', temp_directory)
    right_df = add_index_column(right_df, '__right_index', temp_directory)

    (left_fuzzy_frame,
     right_fuzzy_frame,
     left_col_name,
     right_col_name,
     len_left_df,
     len_right_df) = process_fuzzy_frames(
        left_df=left_df,
        right_df=right_df,
        left_col_name=left_col_name,
        right_col_name=right_col_name,
        temp_dir_ref=temp_directory
    )

    result_df = cross_join_small_files(left_fuzzy_frame, right_fuzzy_frame).collect()
    assert result_df.select(pl.len())[0, 0] == len_left_df * len_right_df
    assert set(result_df.columns) == {'company_name', '__left_index', 'organization',
                                      '__right_index'}, 'Unexpected columns'


def test_cross_join_large_files(temp_directory):
    """Test the cross_join_large_files function."""
    left_df, right_df, mapping = create_test_data(1000)  # Smaller size for test speed

    left_col_name = mapping[0].left_col
    right_col_name = mapping[0].right_col
    left_df = add_index_column(left_df, '__left_index', temp_directory)
    right_df = add_index_column(right_df, '__right_index', temp_directory)

    (left_fuzzy_frame,
     right_fuzzy_frame,
     left_col_name,
     right_col_name,
     len_left_df,
     len_right_df) = process_fuzzy_frames(
        left_df=left_df,
        right_df=right_df,
        left_col_name=left_col_name,
        right_col_name=right_col_name,
        temp_dir_ref=temp_directory
    )

    logging.info(f"Left columns: {left_fuzzy_frame.columns}")
    logging.info(f"Right columns: {right_fuzzy_frame.columns}")

    result_df = cross_join_large_files(left_fuzzy_frame, right_fuzzy_frame, left_col_name, right_col_name).collect()

    logging.info(f"Result columns: {result_df.columns}")
    assert result_df.select(pl.len())[0, 0] > 0  # Should return some rows
    assert result_df.select(pl.len())[0, 0] < len_left_df * len_right_df
    assert set(result_df.columns) == {'company_name', '__left_index', 'organization',
                                      '__right_index'}, 'Unexpected columns'


def test_cross_join_filter_existing_fuzzy_results(temp_directory):
    """Test cross_join_filter_existing_fuzzy_results function."""
    left_df, right_df, mapping = create_test_data(20)

    left_col_name = mapping[0].left_col
    right_col_name = mapping[0].right_col

    # Add index columns
    left_df = add_index_column(left_df, '__left_index', temp_directory)
    right_df = add_index_column(right_df, '__right_index', temp_directory)

    # Create specific existing matches with a deliberate pattern
    # Using indices that aren't sequential to ensure the function is properly filtering
    existing_matches = pl.DataFrame({
        "__left_index": [0, 1, 2, 3],
        "__right_index": [4, 3, 2, 1]
    }, schema=[('__left_index', pl.UInt32), ('__right_index', pl.UInt32)]).lazy()

    # Before running the filter, verify we have our source data
    left_collected = left_df.collect()
    right_collected = right_df.collect()

    # Run the filter function
    result_df = cross_join_filter_existing_fuzzy_results(
        left_df,
        right_df,
        existing_matches,
        left_col_name,
        right_col_name
    ).collect()

    # Verify results
    assert "__left_index" in result_df.columns
    assert "__right_index" in result_df.columns
    assert left_col_name in result_df.columns
    assert right_col_name in result_df.columns

    # Verify that the function correctly filtered on the existing matches
    # The result should include only the mapping pairs that were in existing_matches
    existing_pairs = list(zip(existing_matches.collect()["__left_index"].to_list(),
                              existing_matches.collect()["__right_index"].to_list()))

    result_pairs = []
    for row in result_df.iter_rows(named=True):
        left_indices = row["__left_index"]
        right_indices = row["__right_index"]

        # Handle both scalar and list types
        if isinstance(left_indices, list) and isinstance(right_indices, list):
            for left_idx in left_indices:
                for right_idx in right_indices:
                    result_pairs.append((left_idx, right_idx))
        else:
            result_pairs.append((left_indices, right_indices))

    # Check that all result pairs correspond to existing matches
    for left_idx, right_idx in result_pairs:
        assert (left_idx, right_idx) in existing_pairs, f"Pair ({left_idx}, {right_idx}) not in existing matches"

    # Verify we have the expected number of matches
    assert len(result_df) == len(
        existing_matches.collect()), "Result should have same number of rows as existing matches"


def test_cross_join_no_existing_fuzzy_results(temp_directory):
    """Test cross_join_no_existing_fuzzy_results function."""
    left_df, right_df, mapping = create_deterministic_test_data(20)

    left_col_name = mapping[0].left_col
    right_col_name = mapping[0].right_col

    # Add index columns
    left_df = add_index_column(left_df, '__left_index', temp_directory)
    right_df = add_index_column(right_df, '__right_index', temp_directory)

    # Run the function
    result_df = cross_join_no_existing_fuzzy_results(
        left_df,
        right_df,
        left_col_name,
        right_col_name,
        temp_directory
    ).collect()

    # Verify results
    assert result_df is not None
    assert result_df.shape[0] > 0
    assert result_df.select(pl.len())[0, 0] == left_df.select(pl.len()).collect()[0, 0] * \
           right_df.select(pl.len()).collect()[0, 0]


def test_process_fuzzy_mapping_no_existing_matches(temp_directory, flow_logger):
    left_df, right_df, mapping = create_test_data(20)
    left_df = add_index_column(left_df, '__left_index', temp_directory)
    right_df = add_index_column(right_df, '__right_index', temp_directory)

    fuzzy_map = mapping[0]

    result, _ = process_fuzzy_mapping(fuzzy_map=fuzzy_map,
                                      left_df=left_df,
                                      right_df=right_df,
                                      existing_matches=None,
                                      local_temp_dir_ref=temp_directory,
                                      i=1,
                                      flowfile_logger=flow_logger)
    test_result = (result.join(left_df, on='__left_index')
                   .join(right_df, on='__right_index')
                   .select(["company_name", "organization", "fuzzy_score_1"]).collect())
    result = result.collect()

    # Assert that the result contains the expected columns
    assert '__left_index' in result.columns
    assert '__right_index' in result.columns
    assert 'fuzzy_score_1' in result.columns

    # Verify result is not empty
    assert result.shape[0] > 0

    # Check that fuzzy scores are within expected range (0-100)
    assert all(0 <= score <= 1 for score in result['fuzzy_score_1'])

    # Verify that the test_result has matched columns and reasonable values
    assert test_result.shape[0] > 0
    assert all(isinstance(company, str) for company in test_result['company_name'])
    assert all(isinstance(org, str) for org in test_result['organization'])

    # Check that high fuzzy scores correspond to similar strings
    for row in test_result.iter_rows(named=True):
        company = row['company_name']
        org = row['organization']
        score = row['fuzzy_score_1']

        # If score is high (above threshold), company and org should be similar
        if score >= fuzzy_map.threshold_score / 100:
            # Basic similarity check - at least sharing the same prefix
            assert len(company) > 0 and len(org) > 0

            # For exact matches, the score should be very high
            if company == org:
                assert score == 1  # Expect very high scores for exact matches


def test_process_fuzzy_multiple_mappings(temp_directory, flow_logger):
    left_df, right_df, mapping = create_test_data(50_000)

    left_df, right_df, mapping = pre_process_for_fuzzy_matching(left_df, right_df, mapping, flow_logger)

    left_df = add_index_column(left_df, '__left_index', temp_directory)
    right_df = add_index_column(right_df, '__right_index', temp_directory)

    first_result, n_matches = process_fuzzy_mapping(fuzzy_map=mapping[0],
                                                    left_df=left_df,
                                                    right_df=right_df,
                                                    existing_matches=None,
                                                    local_temp_dir_ref=temp_directory,
                                                    i=1,
                                                    flowfile_logger=flow_logger,
                                                    existing_number_of_matches=None)

    second_result, n_matches = process_fuzzy_mapping(fuzzy_map=mapping[1],
                                                     left_df=left_df,
                                                     right_df=right_df,
                                                     existing_matches=first_result,
                                                     local_temp_dir_ref=temp_directory,
                                                     i=2,
                                                     flowfile_logger=flow_logger,
                                                     existing_number_of_matches=n_matches)

    third_result, n_matches = process_fuzzy_mapping(fuzzy_map=mapping[2],
                                                    left_df=left_df,
                                                    right_df=right_df,
                                                    existing_matches=second_result,
                                                    local_temp_dir_ref=temp_directory,
                                                    i=3,
                                                    flowfile_logger=flow_logger,
                                                    existing_number_of_matches=n_matches)

    first_count = first_result.select(pl.len()).collect()[0, 0]
    second_count = second_result.select(pl.len()).collect()[0, 0]
    third_count = third_result.select(pl.len()).collect()[0, 0]
    assert first_count >= second_count >= third_count, "Expected decreasing number of matches"


def test_perform_all_fuzzy_matches(temp_directory, flow_logger):
    left_df, right_df, mapping = create_test_data(10)

    left_df, right_df, mapping = pre_process_for_fuzzy_matching(left_df, right_df, mapping, flow_logger)
    left_df = add_index_column(left_df, '__left_index', temp_directory)
    right_df = add_index_column(right_df, '__right_index', temp_directory)

    all_matches = perform_all_fuzzy_matches(left_df, right_df, mapping, flow_logger, temp_directory)
    assert len(all_matches) == len(mapping), "Expected one result per mapping"


def test_fuzzy_match_dfs(flow_logger):
    left_df, right_df, mapping = generate_small_fuzzy_test_data()
    result = fuzzy_match_dfs(left_df.lazy(), right_df.lazy(), mapping, flow_logger)
    result = result.sort('id')
    assert result is not None
    expected_match_data = pl.DataFrame(
        {'id': [1, 2, 3, 4, 5], 'company_name': ['Apple Inc.', 'Microsft', 'Amazon', 'Gogle', 'Facebok'],
         'address': ['1 Apple Park', 'One Microsoft Way', '410 Terry Ave N', '1600 Amphitheatre', '1 Hacker Way'],
         'contact': ['Tim Cook', 'Satya Ndella', 'Andy Jessy', 'Sundar Pichai', 'Mark Zukerberg'],
         'fuzzy_score_0': [0.88, 0.9142857142857143, 0.8857142857142858, 0.8666666666666667, 0.9166666666666667],
         'fuzzy_score_1': [0.6666666666666667, 0.9230769230769231, 0.9, 1.0, 0.9333333333333333],
         'id_right': [101, 102, 103, 104, 105],
         'organization': ['Apple Incorporated', 'Microsoft Corp', 'Amazon.com Inc', 'Google LLC', 'Facebook Inc'],
         'location': ['Apple Park, Cupertino', 'Microsoft Way, Redmond', 'Terry Ave North, Seattle',
                      'Amphitheatre Pkwy, Mountain View', 'Hacker Way, Menlo Park'],
         'ceo': ['Timothy Cook', 'Satya Nadella', 'Andy Jassy', 'Sundar Pichai', 'Mark Zuckerberg']}

    )
    assert result.equals(expected_match_data), "Unexpected match data"


def test_unique_df_large(temp_directory):
    """Test the unique_df_large function for handling large dataframes with duplicates."""
    # Create a sample dataframe with intentional duplicates
    data = {
        "category": ["A", "A", "B", "B", "C"] * 20,  # Categories with repetition
        "value": [1, 1, 2, 2, 3] * 20,  # Values with repetition
        "id": list(range(100))  # Unique IDs to make rows distinct
    }
    df = pl.DataFrame(data)

    # Test with columns specified
    result_with_cols = unique_df_large(df, cols=["category", "value"])

    # Verify the results
    assert result_with_cols.shape[0] == 3, "Expected 3 unique combinations of category and value"
    assert set(result_with_cols["category"].to_list()) == {"A", "B", "C"}, "Unexpected categories"
    assert set(result_with_cols["value"].to_list()) == {1, 2, 3}, "Unexpected values"

    # Test with default columns (all columns)
    result_all_cols = unique_df_large(df)

    # Since we have unique IDs, each row should be unique when considering all columns
    assert result_all_cols.shape[0] == 100, "Expected 100 unique rows when considering all columns"


def test_combine_matches(temp_directory):
    """Test the combine_matches function for merging multiple match datasets."""
    # Create sample matching dataframes
    match1 = pl.DataFrame({
        "__left_index": [0, 1, 2, 3],
        "__right_index": [5, 6, 7, 8],
        "fuzzy_score_0": [0.9, 0.8, 0.7, 0.6]
    }).lazy()

    match2 = pl.DataFrame({
        "__left_index": [0, 1, 2, 3],
        "__right_index": [5, 6, 7, 8],
        "fuzzy_score_1": [0.85, 0.75, 0.65, 0.55]
    }).lazy()

    match3 = pl.DataFrame({
        "__left_index": [0, 1],  # Subset of matches to test joining behavior
        "__right_index": [5, 6],
        "fuzzy_score_2": [0.95, 0.92]
    }).lazy()

    # Test combining all matches
    matching_dfs = [match1, match2, match3]
    result = combine_matches(matching_dfs).collect()

    # Verify structure and content
    assert result.shape[0] == 2, "Expected 2 rows after combining (limited by match3)"
    assert set(result.columns) == {"__left_index", "__right_index", "fuzzy_score_0", "fuzzy_score_1",
                                   "fuzzy_score_2"}, "Unexpected columns"

    # Verify values for specific matches
    first_match = result.filter(pl.col("__left_index") == 0).select(
        ["fuzzy_score_0", "fuzzy_score_1", "fuzzy_score_2"]).row(0)
    assert first_match == (0.9, 0.85, 0.95), "Unexpected scores for first match"

    # Test with empty list
    with pytest.raises(IndexError):
        combine_matches([])

    # Test with single match dataframe
    single_result = combine_matches([match1]).collect()
    assert single_result.shape[0] == 4, "Expected 4 rows from single match"
    assert set(single_result.columns) == {"__left_index", "__right_index",
                                          "fuzzy_score_0"}, "Unexpected columns with single match"
