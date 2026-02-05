"""
Integration tests for shared file I/O between kernel containers and the host.

These tests require Docker to be available and the flowfile-kernel image to be built.
They use the ``kernel_manager`` fixture from conftest.py (session-scoped).
"""

import asyncio
import os
from pathlib import Path

import polars as pl
import pytest

from flowfile_core.kernel.models import ExecuteRequest, ExecuteResult

pytestmark = pytest.mark.kernel


def _run(coro):
    """Run an async coroutine from sync test code."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class TestSharedFilesIntegration:
    """Integration tests for shared file I/O via kernel execution."""

    def test_write_csv_from_kernel_read_from_host(self, kernel_manager):
        """Kernel writes a CSV to shared dir, host reads it back."""
        manager, kernel_id = kernel_manager
        shared_dir = Path(manager.shared_volume_path) / "user_files"
        shared_dir.mkdir(parents=True, exist_ok=True)

        code = """
import flowfile

data = "name,value\\nalice,1\\nbob,2\\n"
flowfile.write_shared("integration_test.csv", data)
result = flowfile.shared_path("integration_test.csv")
"""
        request = ExecuteRequest(
            code=code,
            node_id=1,
            flow_id=1,
            input_paths={},
            output_dir=str(shared_dir / "_output"),
        )
        result: ExecuteResult = _run(manager.execute(kernel_id, request))
        assert result.success, f"Kernel execution failed: {result.error}"

        # Read the file from the host side
        host_file = shared_dir / "integration_test.csv"
        assert host_file.exists(), f"File not found at {host_file}"
        content = host_file.read_text()
        assert "alice,1" in content
        assert "bob,2" in content

    def test_write_from_host_read_from_kernel(self, kernel_manager):
        """Host writes a file, kernel reads it back via shared_path."""
        manager, kernel_id = kernel_manager
        shared_dir = Path(manager.shared_volume_path) / "user_files"
        shared_dir.mkdir(parents=True, exist_ok=True)

        # Write from host side
        host_file = shared_dir / "host_written.txt"
        host_file.write_text("hello from host")

        code = """
import flowfile

content = flowfile.read_shared("host_written.txt")
assert content == "hello from host", f"Unexpected content: {content}"
"""
        request = ExecuteRequest(
            code=code,
            node_id=2,
            flow_id=1,
            input_paths={},
            output_dir=str(shared_dir / "_output"),
        )
        result: ExecuteResult = _run(manager.execute(kernel_id, request))
        assert result.success, f"Kernel execution failed: {result.error}"

    def test_shared_path_with_polars_write_csv(self, kernel_manager):
        """Kernel writes CSV via polars using shared_path, host reads it."""
        manager, kernel_id = kernel_manager
        shared_dir = Path(manager.shared_volume_path) / "user_files"
        shared_dir.mkdir(parents=True, exist_ok=True)

        code = """
import flowfile
import polars as pl

df = pl.DataFrame({"x": [10, 20, 30], "y": ["a", "b", "c"]})
df.write_csv(flowfile.shared_path("polars_test.csv"))
"""
        request = ExecuteRequest(
            code=code,
            node_id=3,
            flow_id=1,
            input_paths={},
            output_dir=str(shared_dir / "_output"),
        )
        result: ExecuteResult = _run(manager.execute(kernel_id, request))
        assert result.success, f"Kernel execution failed: {result.error}"

        host_file = shared_dir / "polars_test.csv"
        assert host_file.exists()
        df = pl.read_csv(str(host_file))
        assert df.shape == (3, 2)
        assert df["x"].to_list() == [10, 20, 30]

    def test_shared_path_with_polars_write_parquet(self, kernel_manager):
        """Kernel writes parquet via polars using shared_path, host reads it."""
        manager, kernel_id = kernel_manager
        shared_dir = Path(manager.shared_volume_path) / "user_files"
        shared_dir.mkdir(parents=True, exist_ok=True)

        code = """
import flowfile
import polars as pl

df = pl.DataFrame({"id": [1, 2, 3], "name": ["alice", "bob", "charlie"]})
df.write_parquet(flowfile.shared_path("polars_test.parquet"))
"""
        request = ExecuteRequest(
            code=code,
            node_id=4,
            flow_id=1,
            input_paths={},
            output_dir=str(shared_dir / "_output"),
        )
        result: ExecuteResult = _run(manager.execute(kernel_id, request))
        assert result.success, f"Kernel execution failed: {result.error}"

        host_file = shared_dir / "polars_test.parquet"
        assert host_file.exists()
        df = pl.read_parquet(str(host_file))
        assert df.shape == (3, 2)
        assert df["name"].to_list() == ["alice", "bob", "charlie"]

    def test_list_shared_from_kernel(self, kernel_manager):
        """Kernel lists files after writing them."""
        manager, kernel_id = kernel_manager
        shared_dir = Path(manager.shared_volume_path) / "user_files"
        shared_dir.mkdir(parents=True, exist_ok=True)

        code = """
import flowfile

flowfile.write_shared("list_a.txt", "aaa")
flowfile.write_shared("list_b.txt", "bbb")
files = flowfile.list_shared()
assert "list_a.txt" in files, f"list_a.txt not in {files}"
assert "list_b.txt" in files, f"list_b.txt not in {files}"
"""
        request = ExecuteRequest(
            code=code,
            node_id=5,
            flow_id=1,
            input_paths={},
            output_dir=str(shared_dir / "_output"),
        )
        result: ExecuteResult = _run(manager.execute(kernel_id, request))
        assert result.success, f"Kernel execution failed: {result.error}"

    def test_delete_shared_from_kernel(self, kernel_manager):
        """Kernel writes and then deletes a shared file."""
        manager, kernel_id = kernel_manager
        shared_dir = Path(manager.shared_volume_path) / "user_files"
        shared_dir.mkdir(parents=True, exist_ok=True)

        code = """
import flowfile

flowfile.write_shared("to_delete.txt", "temporary")
flowfile.delete_shared("to_delete.txt")

try:
    flowfile.read_shared("to_delete.txt")
    assert False, "Should have raised FileNotFoundError"
except FileNotFoundError:
    pass  # expected
"""
        request = ExecuteRequest(
            code=code,
            node_id=6,
            flow_id=1,
            input_paths={},
            output_dir=str(shared_dir / "_output"),
        )
        result: ExecuteResult = _run(manager.execute(kernel_id, request))
        assert result.success, f"Kernel execution failed: {result.error}"
