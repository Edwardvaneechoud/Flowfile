"""Integration tests for kernel-hosted Jedi code intelligence.

Requires Docker — the ``kernel_manager`` fixture builds the flowfile-kernel image
(which now bundles jedi/pyflakes) and starts a container. Exercises the full path:
execute a cell to seed a variable, then ask the kernel's /lsp/complete to surface it
through ``KernelManager.lsp_request`` (the same forward core's bridge route uses).
"""

import asyncio

import pytest

from flowfile_core.kernel.manager import KernelManager
from flowfile_core.kernel.models import ExecuteRequest

pytestmark = [pytest.mark.kernel, pytest.mark.lsp]

_FLOW_ID = 777


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _labels(manager: KernelManager, kernel_id: str, code: str, line: int, column: int) -> list[str]:
    payload = {"code": code, "line": line, "column": column, "flow_id": _FLOW_ID}
    result = _run(manager.lsp_request(kernel_id, "complete", payload))
    return [item["label"] for item in result.get("items", [])]


class TestKernelLsp:
    def test_complete_polars_module(self, kernel_manager: tuple[KernelManager, str]):
        """Real installed polars resolves through the seeded namespace."""
        manager, kernel_id = kernel_manager
        labels = _labels(manager, kernel_id, "pl.", 1, 3)
        assert "DataFrame" in labels
        assert "col" in labels

    def test_complete_flowfile_ctx(self, kernel_manager: tuple[KernelManager, str]):
        manager, kernel_id = kernel_manager
        labels = _labels(manager, kernel_id, "flowfile_ctx.", 1, 13)
        assert "read_input" in labels

    def test_complete_uses_live_namespace(self, kernel_manager: tuple[KernelManager, str]):
        """A variable bound by a prior executed cell is completable with real types."""
        manager, kernel_id = kernel_manager
        _run(
            manager.execute(
                kernel_id,
                ExecuteRequest(
                    node_id=1,
                    flow_id=_FLOW_ID,
                    code="import polars as pl\nlsp_df = pl.LazyFrame({'a': [1], 'b': [2]})",
                    input_paths={},
                    output_dir="/shared/test_lsp_ns",
                ),
            )
        )
        labels = _labels(manager, kernel_id, "lsp_df.", 1, 7)
        assert "select" in labels
        assert "filter" in labels

    def test_hover_returns_contents(self, kernel_manager: tuple[KernelManager, str]):
        manager, kernel_id = kernel_manager
        payload = {"code": "pl.col", "line": 1, "column": 6, "flow_id": _FLOW_ID}
        result = _run(manager.lsp_request(kernel_id, "hover", payload))
        assert result.get("contents")
