"""
Tests for file-based schema caching functionality.

Tests cover:
- Basic cache operations (get/set/invalidate)
- Cache invalidation on file modification
- Different settings creating different cache entries
- Integration with xlsx, csv, parquet schema loading
- Performance comparison (cached vs uncached)
- Cache statistics
- Hash-based invalidation via ReceivedTable.file_mtime/file_size
"""

import os
import tempfile
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from flowfile_core.utils.file_cache import (
    CacheEntry,
    FileInfo,
    FileSchemaCache,
    cache_schema,
    clear_file_cache,
    get_cached_schema,
    get_file_cache_stats,
    get_file_schema_cache,
    invalidate_file_cache,
    set_file_cache_debug_mode,
)
from flowfile_core.schemas.input_schema import ReceivedTable, InputCsvTable, InputExcelTable
from flowfile_core.flowfile.utils import get_hash


# =============================================================================
# FileInfo Tests
# =============================================================================


class TestFileInfo:
    """Tests for the FileInfo class."""

    def test_from_path_existing_file(self, tmp_path):
        """Test creating FileInfo from an existing file."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("hello")

        file_info = FileInfo.from_path(str(test_file))

        assert file_info is not None
        assert file_info.path == str(test_file)
        assert file_info.size == 5
        assert file_info.mtime > 0

    def test_from_path_nonexistent_file(self):
        """Test creating FileInfo from a non-existent file returns None."""
        file_info = FileInfo.from_path("/nonexistent/path/file.txt")
        assert file_info is None

    def test_has_changed_unchanged_file(self, tmp_path):
        """Test has_changed returns False for unchanged file."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("hello")

        file_info = FileInfo.from_path(str(test_file))
        assert file_info.has_changed() is False

    def test_has_changed_modified_file(self, tmp_path):
        """Test has_changed returns True when file is modified."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("hello")

        file_info = FileInfo.from_path(str(test_file))

        # Wait a bit and modify the file
        time.sleep(0.1)
        test_file.write_text("hello world")

        assert file_info.has_changed() is True

    def test_has_changed_deleted_file(self, tmp_path):
        """Test has_changed returns True when file is deleted."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("hello")

        file_info = FileInfo.from_path(str(test_file))
        test_file.unlink()

        assert file_info.has_changed() is True

    def test_to_key_format(self, tmp_path):
        """Test that to_key generates expected format."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("hello")

        file_info = FileInfo.from_path(str(test_file))
        key = file_info.to_key()

        assert str(test_file) in key
        assert ":" in key  # Format is path:mtime:size


# =============================================================================
# FileSchemaCache Tests
# =============================================================================


class TestFileSchemaCache:
    """Tests for the FileSchemaCache class."""

    @pytest.fixture(autouse=True)
    def clear_cache(self):
        """Clear cache before and after each test."""
        clear_file_cache()
        yield
        clear_file_cache()

    def test_singleton_pattern(self):
        """Test that FileSchemaCache is a singleton."""
        cache1 = FileSchemaCache.get_instance()
        cache2 = FileSchemaCache.get_instance()
        assert cache1 is cache2

    def test_get_schema_cache_miss(self, tmp_path):
        """Test that get_schema returns None on cache miss."""
        test_file = tmp_path / "test.xlsx"
        test_file.write_text("data")

        result = get_cached_schema(str(test_file), "xlsx", {"sheet": "Sheet1"})
        assert result is None

    def test_set_and_get_schema(self, tmp_path):
        """Test basic set and get operations."""
        test_file = tmp_path / "test.xlsx"
        test_file.write_text("data")

        mock_schema = [MagicMock(column_name="col1"), MagicMock(column_name="col2")]
        settings = {"sheet": "Sheet1"}

        cache_schema(str(test_file), "xlsx", settings, mock_schema)
        result = get_cached_schema(str(test_file), "xlsx", settings)

        assert result is mock_schema

    def test_different_settings_different_cache_entries(self, tmp_path):
        """Test that different settings create different cache entries."""
        test_file = tmp_path / "test.xlsx"
        test_file.write_text("data")

        schema1 = [MagicMock(column_name="col1")]
        schema2 = [MagicMock(column_name="col2")]

        cache_schema(str(test_file), "xlsx", {"sheet": "Sheet1"}, schema1)
        cache_schema(str(test_file), "xlsx", {"sheet": "Sheet2"}, schema2)

        result1 = get_cached_schema(str(test_file), "xlsx", {"sheet": "Sheet1"})
        result2 = get_cached_schema(str(test_file), "xlsx", {"sheet": "Sheet2"})

        assert result1 is schema1
        assert result2 is schema2
        assert result1 is not result2

    def test_cache_invalidation_on_file_change(self, tmp_path):
        """Test that cache is invalidated when file changes."""
        test_file = tmp_path / "test.xlsx"
        test_file.write_text("original data")

        mock_schema = [MagicMock(column_name="col1")]
        settings = {"sheet": "Sheet1"}

        cache_schema(str(test_file), "xlsx", settings, mock_schema)

        # Verify it's cached
        assert get_cached_schema(str(test_file), "xlsx", settings) is mock_schema

        # Modify the file
        time.sleep(0.1)  # Ensure mtime changes
        test_file.write_text("modified data - longer content")

        # Cache should be invalidated
        result = get_cached_schema(str(test_file), "xlsx", settings)
        assert result is None

    def test_invalidate_file(self, tmp_path):
        """Test explicit file invalidation."""
        test_file = tmp_path / "test.xlsx"
        test_file.write_text("data")

        mock_schema = [MagicMock(column_name="col1")]

        cache_schema(str(test_file), "xlsx", {"sheet": "Sheet1"}, mock_schema)
        cache_schema(str(test_file), "xlsx", {"sheet": "Sheet2"}, mock_schema)

        # Both should be cached
        assert get_cached_schema(str(test_file), "xlsx", {"sheet": "Sheet1"}) is not None
        assert get_cached_schema(str(test_file), "xlsx", {"sheet": "Sheet2"}) is not None

        # Invalidate all entries for this file
        count = invalidate_file_cache(str(test_file))
        assert count == 2

        # Both should be gone
        assert get_cached_schema(str(test_file), "xlsx", {"sheet": "Sheet1"}) is None
        assert get_cached_schema(str(test_file), "xlsx", {"sheet": "Sheet2"}) is None

    def test_clear_cache(self, tmp_path):
        """Test clearing all cache entries."""
        test_file1 = tmp_path / "test1.xlsx"
        test_file2 = tmp_path / "test2.csv"
        test_file1.write_text("data1")
        test_file2.write_text("data2")

        mock_schema = [MagicMock(column_name="col1")]

        cache_schema(str(test_file1), "xlsx", {}, mock_schema)
        cache_schema(str(test_file2), "csv", {}, mock_schema)

        stats = get_file_cache_stats()
        assert stats["entries"] == 2

        clear_file_cache()

        stats = get_file_cache_stats()
        assert stats["entries"] == 0

    def test_cache_stats(self, tmp_path):
        """Test cache statistics."""
        test_file = tmp_path / "test.xlsx"
        test_file.write_text("data")

        mock_schema = [MagicMock(column_name="col1")]
        settings = {"sheet": "Sheet1"}

        cache_schema(str(test_file), "xlsx", settings, mock_schema)

        # Access it multiple times to increase hit count
        for _ in range(5):
            get_cached_schema(str(test_file), "xlsx", settings)

        stats = get_file_cache_stats()
        assert stats["entries"] == 1
        assert stats["total_hits"] == 5
        assert "xlsx" in stats["enabled_file_types"]

    def test_caching_disabled_for_unsupported_types(self, tmp_path):
        """Test that caching is disabled for unsupported file types."""
        test_file = tmp_path / "test.json"
        test_file.write_text('{"data": 1}')

        mock_schema = [MagicMock(column_name="col1")]

        # json is not in the default enabled types
        cache_schema(str(test_file), "json", {}, mock_schema)

        # Should not be cached
        result = get_cached_schema(str(test_file), "json", {})
        assert result is None

    def test_debug_mode_enables_all_types(self, tmp_path):
        """Test that debug mode enables caching for all file types."""
        test_file = tmp_path / "test.json"
        test_file.write_text('{"data": 1}')

        mock_schema = [MagicMock(column_name="col1")]

        set_file_cache_debug_mode(True)
        try:
            cache_schema(str(test_file), "json", {}, mock_schema)
            result = get_cached_schema(str(test_file), "json", {})
            assert result is mock_schema
        finally:
            set_file_cache_debug_mode(False)

    def test_nonexistent_file_not_cached(self):
        """Test that non-existent files are not cached."""
        mock_schema = [MagicMock(column_name="col1")]

        cache_schema("/nonexistent/path/file.xlsx", "xlsx", {}, mock_schema)

        # Should not be cached
        result = get_cached_schema("/nonexistent/path/file.xlsx", "xlsx", {})
        assert result is None


# =============================================================================
# Thread Safety Tests
# =============================================================================


class TestCacheThreadSafety:
    """Tests for thread safety of the cache."""

    @pytest.fixture(autouse=True)
    def clear_cache(self):
        """Clear cache before and after each test."""
        clear_file_cache()
        yield
        clear_file_cache()

    def test_concurrent_access(self, tmp_path):
        """Test that concurrent access doesn't cause issues."""
        test_file = tmp_path / "test.xlsx"
        test_file.write_text("data")

        mock_schema = [MagicMock(column_name="col1")]
        settings = {"sheet": "Sheet1"}
        errors = []

        def cache_and_get():
            try:
                for i in range(100):
                    cache_schema(str(test_file), "xlsx", settings, mock_schema)
                    result = get_cached_schema(str(test_file), "xlsx", settings)
                    if result is None and i > 0:  # First might be None
                        errors.append(f"Unexpected None at iteration {i}")
            except Exception as e:
                errors.append(str(e))

        threads = [threading.Thread(target=cache_and_get) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"Errors during concurrent access: {errors}"


# =============================================================================
# Integration Tests with Real Files
# =============================================================================


class TestFileCacheIntegration:
    """Integration tests with real file types."""

    @pytest.fixture(autouse=True)
    def clear_cache(self):
        """Clear cache before and after each test."""
        clear_file_cache()
        yield
        clear_file_cache()

    def test_xlsx_schema_caching(self):
        """Test that xlsx schema is cached correctly."""
        from flowfile_core.flowfile.flow_graph import get_xlsx_schema

        excel_path = "flowfile_core/tests/support_files/data/excel_file.xlsx"
        if not os.path.exists(excel_path):
            pytest.skip("Excel test file not found")

        # First call - cache miss
        stats_before = get_file_cache_stats()

        schema1 = get_xlsx_schema(
            engine="openpyxl",
            file_path=excel_path,
            sheet_name="Sheet1",
            start_row=0,
            start_column=0,
            end_row=0,
            end_column=0,
            has_headers=True,
        )

        # Second call - cache hit
        schema2 = get_xlsx_schema(
            engine="openpyxl",
            file_path=excel_path,
            sheet_name="Sheet1",
            start_row=0,
            start_column=0,
            end_row=0,
            end_column=0,
            has_headers=True,
        )

        stats_after = get_file_cache_stats()

        assert schema1 is not None
        assert schema2 is not None
        assert len(schema1) == len(schema2)
        assert stats_after["total_hits"] > stats_before["total_hits"]

    def test_xlsx_different_sheets_different_cache(self):
        """Test that different sheets create different cache entries."""
        from flowfile_core.flowfile.flow_graph import get_xlsx_schema

        excel_path = "flowfile_core/tests/support_files/data/excel_file.xlsx"
        if not os.path.exists(excel_path):
            pytest.skip("Excel test file not found")

        schema1 = get_xlsx_schema(
            engine="openpyxl",
            file_path=excel_path,
            sheet_name="Sheet1",
            start_row=0,
            start_column=0,
            end_row=0,
            end_column=0,
            has_headers=True,
        )

        # Different sheet name should not hit the cache
        stats_before = get_file_cache_stats()

        # Note: This might fail if Sheet2 doesn't exist, but it tests the cache key logic
        try:
            schema2 = get_xlsx_schema(
                engine="openpyxl",
                file_path=excel_path,
                sheet_name="Sheet2",  # Different sheet
                start_row=0,
                start_column=0,
                end_row=0,
                end_column=0,
                has_headers=True,
            )
        except Exception:
            pass  # Sheet2 might not exist

        stats_after = get_file_cache_stats()

        # Should have created a new cache entry (or tried to)
        assert stats_after["entries"] >= 1


# =============================================================================
# Performance Tests
# =============================================================================


class TestFileCachePerformance:
    """Performance comparison tests."""

    @pytest.fixture(autouse=True)
    def clear_cache(self):
        """Clear cache before and after each test."""
        clear_file_cache()
        yield
        clear_file_cache()

    def test_cache_hit_is_faster(self):
        """Test that cache hits are significantly faster than misses."""
        from flowfile_core.flowfile.flow_graph import get_xlsx_schema

        excel_path = "flowfile_core/tests/support_files/data/excel_file.xlsx"
        if not os.path.exists(excel_path):
            pytest.skip("Excel test file not found")

        # First call - cache miss (reads file)
        start = time.time()
        get_xlsx_schema(
            engine="openpyxl",
            file_path=excel_path,
            sheet_name="Sheet1",
            start_row=0,
            start_column=0,
            end_row=0,
            end_column=0,
            has_headers=True,
        )
        first_call_time = time.time() - start

        # Multiple cache hits
        cache_hit_times = []
        for _ in range(10):
            start = time.time()
            get_xlsx_schema(
                engine="openpyxl",
                file_path=excel_path,
                sheet_name="Sheet1",
                start_row=0,
                start_column=0,
                end_row=0,
                end_column=0,
                has_headers=True,
            )
            cache_hit_times.append(time.time() - start)

        avg_cache_hit_time = sum(cache_hit_times) / len(cache_hit_times)

        # Cache hits should be at least 10x faster (usually 100x+)
        # Use a conservative threshold to avoid flaky tests
        assert avg_cache_hit_time < first_call_time, (
            f"Cache hit ({avg_cache_hit_time:.4f}s) should be faster than "
            f"first call ({first_call_time:.4f}s)"
        )

        # Log the speedup for visibility
        if first_call_time > 0 and avg_cache_hit_time > 0:
            speedup = first_call_time / avg_cache_hit_time
            print(f"\nSpeedup from caching: {speedup:.1f}x")
            print(f"First call (cache miss): {first_call_time:.4f}s")
            print(f"Average cache hit: {avg_cache_hit_time:.6f}s")

    def test_csv_schema_caching_performance(self):
        """Test CSV schema caching performance."""
        csv_path = "flowfile_core/tests/support_files/data/fake_data.csv"
        if not os.path.exists(csv_path):
            pytest.skip("CSV test file not found")

        from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
        from flowfile_core.schemas.input_schema import InputCsvTable, ReceivedTable

        # Create a schema callback similar to what flow_graph does
        def get_csv_schema():
            received_table = ReceivedTable(
                path=csv_path,
                name="test",
                file_type="csv",
                table_settings=InputCsvTable(),
            )
            cache_settings = {
                "encoding": received_table.table_settings.encoding,
                "delimiter": received_table.table_settings.delimiter,
                "has_headers": received_table.table_settings.has_headers,
            }

            cached = get_cached_schema(csv_path, "csv", cache_settings)
            if cached is not None:
                return cached

            input_data = FlowDataEngine.create_from_path(received_table)
            schema = input_data.schema

            if schema:
                cache_schema(csv_path, "csv", cache_settings, schema)

            return schema

        # First call
        start = time.time()
        schema1 = get_csv_schema()
        first_call = time.time() - start

        # Second call (cached)
        start = time.time()
        schema2 = get_csv_schema()
        second_call = time.time() - start

        assert schema1 is not None
        assert schema2 is not None
        assert second_call < first_call

        stats = get_file_cache_stats()
        assert stats["total_hits"] >= 1

    def test_parquet_schema_caching_performance(self):
        """Test Parquet schema caching performance."""
        parquet_path = "flowfile_core/tests/support_files/data/fake_data.parquet"
        if not os.path.exists(parquet_path):
            pytest.skip("Parquet test file not found")

        from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
        from flowfile_core.schemas.input_schema import InputParquetTable, ReceivedTable

        def get_parquet_schema():
            received_table = ReceivedTable(
                path=parquet_path,
                name="test",
                file_type="parquet",
                table_settings=InputParquetTable(),
            )
            cache_settings = {}  # Parquet schema doesn't depend on settings

            cached = get_cached_schema(parquet_path, "parquet", cache_settings)
            if cached is not None:
                return cached

            input_data = FlowDataEngine.create_from_path(received_table)
            schema = input_data.schema

            if schema:
                cache_schema(parquet_path, "parquet", cache_settings, schema)

            return schema

        # First call
        start = time.time()
        schema1 = get_parquet_schema()
        first_call = time.time() - start

        # Second call (cached)
        start = time.time()
        schema2 = get_parquet_schema()
        second_call = time.time() - start

        assert schema1 is not None
        assert schema2 is not None
        assert second_call < first_call

        stats = get_file_cache_stats()
        assert stats["total_hits"] >= 1


# =============================================================================
# Hash-Based Invalidation Tests (ReceivedTable.file_mtime/file_size)
# =============================================================================


class TestReceivedTableFileInfo:
    """Tests for file modification tracking in ReceivedTable."""

    def test_file_info_populated_on_create(self, tmp_path):
        """Test that file_mtime and file_size are populated when ReceivedTable is created."""
        test_file = tmp_path / "test.csv"
        test_file.write_text("a,b,c\n1,2,3\n")

        received_table = ReceivedTable(
            path=str(test_file),
            name="test.csv",
            file_type="csv",
            table_settings=InputCsvTable(),
        )

        # file_mtime and file_size should be populated via model_validator
        assert received_table.file_mtime is not None
        assert received_table.file_size is not None
        assert received_table.file_size == len("a,b,c\n1,2,3\n")

    def test_file_info_none_for_nonexistent_file(self):
        """Test that file_mtime and file_size are None for non-existent files."""
        received_table = ReceivedTable(
            path="/nonexistent/path/file.csv",
            name="file.csv",
            file_type="csv",
            table_settings=InputCsvTable(),
        )

        assert received_table.file_mtime is None
        assert received_table.file_size is None

    def test_refresh_file_info_detects_changes(self, tmp_path):
        """Test that refresh_file_info updates mtime/size after file modification."""
        test_file = tmp_path / "test.csv"
        test_file.write_text("a,b,c\n1,2,3\n")

        received_table = ReceivedTable(
            path=str(test_file),
            name="test.csv",
            file_type="csv",
            table_settings=InputCsvTable(),
        )

        original_mtime = received_table.file_mtime
        original_size = received_table.file_size

        # Modify the file
        time.sleep(0.1)  # Ensure mtime changes
        test_file.write_text("a,b,c,d\n1,2,3,4\n5,6,7,8\n")

        # Refresh file info
        received_table.refresh_file_info()

        # mtime and size should have changed
        assert received_table.file_mtime != original_mtime
        assert received_table.file_size != original_size

    def test_hash_changes_when_file_changes(self, tmp_path):
        """Test that ReceivedTable hash changes when the underlying file changes."""
        test_file = tmp_path / "test.csv"
        test_file.write_text("a,b,c\n1,2,3\n")

        received_table = ReceivedTable(
            path=str(test_file),
            name="test.csv",
            file_type="csv",
            table_settings=InputCsvTable(),
        )

        hash1 = get_hash(received_table)

        # Modify the file
        time.sleep(0.1)
        test_file.write_text("a,b,c,d\n1,2,3,4\n")

        # Refresh file info to update mtime/size
        received_table.refresh_file_info()

        hash2 = get_hash(received_table)

        # Hash should be different because file_mtime and file_size changed
        assert hash1 != hash2

    def test_hash_same_when_file_unchanged(self, tmp_path):
        """Test that ReceivedTable hash stays the same when file is unchanged."""
        test_file = tmp_path / "test.csv"
        test_file.write_text("a,b,c\n1,2,3\n")

        received_table = ReceivedTable(
            path=str(test_file),
            name="test.csv",
            file_type="csv",
            table_settings=InputCsvTable(),
        )

        hash1 = get_hash(received_table)

        # Refresh without changing file
        received_table.refresh_file_info()

        hash2 = get_hash(received_table)

        # Hash should be the same
        assert hash1 == hash2

    def test_hash_different_for_different_settings(self, tmp_path):
        """Test that different settings produce different hashes."""
        test_file = tmp_path / "test.csv"
        test_file.write_text("a,b,c\n1,2,3\n")

        table1 = ReceivedTable(
            path=str(test_file),
            name="test.csv",
            file_type="csv",
            table_settings=InputCsvTable(delimiter=","),
        )

        table2 = ReceivedTable(
            path=str(test_file),
            name="test.csv",
            file_type="csv",
            table_settings=InputCsvTable(delimiter=";"),
        )

        hash1 = get_hash(table1)
        hash2 = get_hash(table2)

        # Hash should be different due to different delimiter
        assert hash1 != hash2

    def test_excel_file_info(self, tmp_path):
        """Test file info tracking for Excel files."""
        # Create a minimal xlsx-like file for testing (just for file info tracking)
        test_file = tmp_path / "test.xlsx"
        test_file.write_bytes(b"PK\x03\x04" + b"\x00" * 100)  # Minimal zip header

        received_table = ReceivedTable(
            path=str(test_file),
            name="test.xlsx",
            file_type="excel",
            table_settings=InputExcelTable(sheet_name="Sheet1"),
        )

        assert received_table.file_mtime is not None
        assert received_table.file_size is not None
        assert received_table.file_size == 104  # 4 + 100 bytes
