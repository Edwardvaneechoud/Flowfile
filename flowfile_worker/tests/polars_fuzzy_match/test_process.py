import sys
import os
from typing import List

# Set up import paths
try:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    sys.path.append(os.path.dirname(os.path.abspath('flowfile_worker/tests/polars_fuzzy_match/test_process.py')))

import polars as pl
import pytest
from match_utils import generate_small_fuzzy_test_data_left, generate_small_fuzzy_test_data_right
from flowfile_worker.polars_fuzzy_match.process import (
    calculate_fuzzy_score,
    calculate_and_parse_fuzzy,
    process_fuzzy_frames
)
from flowfile_worker.polars_fuzzy_match.models import FuzzyTypeLiteral

# Test configuration
FUZZY_TYPES: List[FuzzyTypeLiteral] = [
    'levenshtein', 'jaro', 'jaro_winkler',
    'hamming', 'damerau_levenshtein', 'indel'
]
THRESHOLD_VALUES: List[float] = [0.3, 0.5, 0.7, 0.9]


@pytest.fixture(autouse=True)
def no_caching(monkeypatch):
    """
    Disable caching for all tests by mocking cache-related functions.
    """

    def _cache_polars_frame_to_temp(df, tempdir=None):
        print("Global caching disabled")
        return df.lazy() if isinstance(df, pl.DataFrame) else df

    try:
        # Import and patch utils module
        import flowfile_worker.polars_fuzzy_match.utils
        monkeypatch.setattr(
            flowfile_worker.polars_fuzzy_match.utils,
            'cache_polars_frame_to_temp',
            _cache_polars_frame_to_temp
        )
        monkeypatch.setattr(
            flowfile_worker.polars_fuzzy_match.utils,
            'write_polars_frame',
            lambda df, path: True
        )
    except (ImportError, AttributeError) as e:
        print(f"Warning: Unable to patch caching functions: {e}")


# Test fixtures
@pytest.fixture
def small_test_data() -> pl.LazyFrame:
    """Create a small dataset for fuzzy matching tests."""
    test_data = {
        "left_name": ["John", "Johan", "Johannes", "Edward", "Edwin", "Smith", "Simpson", "Thompson"],
        "right_name": ["Johny", "Doris", "John", "Eduward", "Edwin", "Smyth", "Simson", "Thomson"]
    }
    return pl.LazyFrame(test_data)


@pytest.fixture
def test_data_with_indices() -> pl.LazyFrame:
    """Create test data with left_index and right_index columns."""
    test_data = {
        "left_name": ["John", "Johan", "Johannes", "Edward"],
        "right_name": ["Johny", "Doris", "John", "Eduward"],
        "__left_index": [[1, 2], [3, 4], [5, 6], [7, 8]],
        "__right_index": [[10, 20], [30, 40], [50, 60], [70, 80]]
    }
    return pl.LazyFrame(test_data)


@pytest.fixture
def test_data_empty_indices() -> pl.LazyFrame:
    """Create test data with some empty indices to test edge cases."""
    test_data = {
        "left_name": ["John", "Johan", "Johannes"],
        "right_name": ["Johny", "Doris", "John"],
        "__left_index": [[], [1, 2], [3]],
        "__right_index": [[10], [20, 30], []]
    }
    return pl.LazyFrame(test_data)


# Test functions
@pytest.mark.parametrize("fuzzy_type", FUZZY_TYPES)
@pytest.mark.parametrize("threshold", THRESHOLD_VALUES)
def test_calculate_fuzzy_score_all_types(small_test_data: pl.LazyFrame, fuzzy_type: FuzzyTypeLiteral, threshold: float):
    """Test all fuzzy matching algorithms with various thresholds."""
    result_df = calculate_fuzzy_score(
        small_test_data,
        "left_name",
        "right_name",
        fuzzy_type,
        threshold
    ).collect()

    # Basic validation
    assert not result_df.is_empty()
    assert "s" in result_df.columns

    # Validate score ranges
    score_col = result_df["s"]
    assert score_col.min() >= 0.0
    assert score_col.max() <= 1.0

    # Validate threshold filtering
    if threshold > 0:
        filtered_scores = score_col.filter(score_col >= threshold)
        assert filtered_scores.min() >= threshold


def test_calculate_and_parse_fuzzy(test_data_with_indices):
    """Test the calculate_and_parse_fuzzy function."""
    fuzzy_method: FuzzyTypeLiteral = "levenshtein"
    threshold = 0.2

    # Expected result
    expected_data = pl.DataFrame(
        {'s': [0.8, 0.8, 0.8, 0.8, 0.8571428571428572, 0.8571428571428572, 0.8571428571428572, 0.8571428571428572],
         '__left_index': [1, 1, 2, 2, 7, 7, 8, 8],
         '__right_index': [10, 20, 10, 20, 70, 80, 70, 80]}
    )

    # Execute function and collect results
    result_df = calculate_and_parse_fuzzy(
        test_data_with_indices,
        "left_name",
        "right_name",
        fuzzy_method,
        threshold
    ).collect()

    # Basic validation
    assert not result_df.is_empty()
    assert "s" in result_df.columns
    assert "__left_index" in result_df.columns
    assert "__right_index" in result_df.columns

    # Check explode worked properly
    for row in result_df.iter_rows(named=True):
        assert isinstance(row["__left_index"], (int, float))
        assert isinstance(row["__right_index"], (int, float))

    # Validate score ranges
    score_col = result_df["s"]
    assert score_col.min() >= 0.8
    assert score_col.max() <= 1.0
    assert all(score >= threshold for score in score_col)

    # Verify matches expected data
    assert len(result_df) >= 1
    assert result_df.equals(expected_data, null_equal=True)


def test_calculate_and_parse_fuzzy_empty_indices(test_data_empty_indices):
    """Test calculate_and_parse_fuzzy with some empty indices."""
    fuzzy_method: FuzzyTypeLiteral = "levenshtein"
    threshold = 0.2

    # Execute function and collect results
    result_df = calculate_and_parse_fuzzy(
        test_data_empty_indices,
        "left_name",
        "right_name",
        fuzzy_method,
        threshold
    ).collect()

    # Basic validation
    assert "s" in result_df.columns
    assert "__left_index" in result_df.columns
    assert "__right_index" in result_df.columns

    # Check score ranges for any remaining rows
    if not result_df.is_empty():
        score_col = result_df["s"]
        assert score_col.min() >= 0.8
        assert score_col.max() <= 1.0
        assert all(score >= threshold for score in score_col)


def test_process_fuzzy_frames(monkeypatch):
    """Test the process_fuzzy_frames function."""
    # Import and patch modules directly for this test
    from flowfile_worker.polars_fuzzy_match import process

    # Create and apply mock functions
    def mock_cache_polars_frame_to_temp(df, tempdir=None):
        print("Mock cache_polars_frame_to_temp called")
        return df

    def mock_collect_lazy_frame(df):
        print("Mock collect_lazy_frame called")
        return df.collect()

    monkeypatch.setattr(process, 'cache_polars_frame_to_temp', mock_cache_polars_frame_to_temp)
    monkeypatch.setattr(process, 'collect_lazy_frame', mock_collect_lazy_frame)

    # Prepare test data
    temp_dir_ref = "/temp"
    left_df = (generate_small_fuzzy_test_data_left().lazy()
               .with_columns(pl.col("id")
                             .map_elements(lambda x: [x], return_dtype=pl.List(pl.Int64))
                             .alias("__left_index")))
    right_df = (generate_small_fuzzy_test_data_right().lazy()
                .with_columns(pl.col("id")
                              .map_elements(lambda x: [x], return_dtype=pl.List(pl.Int64))
                              .alias("__right_index")))

    # Define column names
    left_col_name = "company_name"
    right_col_name = "organization"

    # Execute the function
    left_fuzzy_frame, right_fuzzy_frame, result_left_col, result_right_col, len_left, len_right = process_fuzzy_frames(
        left_df,
        right_df,
        left_col_name,
        right_col_name,
        temp_dir_ref
    )

    # Verify results
    assert left_fuzzy_frame is not None
    assert right_fuzzy_frame is not None

    # Collect and check content
    left_collected = left_fuzzy_frame.collect()
    right_collected = right_fuzzy_frame.collect()

    # Verify columns
    assert result_left_col in left_collected.columns
    assert "__left_index" in left_collected.columns or "__right_index" in left_collected.columns
    assert result_right_col in right_collected.columns
    assert "__left_index" in right_collected.columns or "__right_index" in right_collected.columns

    # Verify lengths
    assert len_left > 0
    assert len_right > 0

    # Verify swap logic
    if len_left < len_right:
        raise AssertionError("Expected left frame to be larger than or equal to right frame after possible swap")

    # Check that null values were filtered
    assert left_collected.filter(pl.col(result_left_col).is_null()).height == 0
    assert right_collected.filter(pl.col(result_right_col).is_null()).height == 0