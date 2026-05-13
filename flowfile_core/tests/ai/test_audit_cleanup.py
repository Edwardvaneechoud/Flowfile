from __future__ import annotations

from typing import get_args

from flowfile_core.ai import audit
from flowfile_core.ai.agents.planner._internal import DEFAULT_MAX_DB_READER_OPS
from flowfile_core.ai.safety import RefusalReason


class TestAuditBudgetCleanup:
    """Verify cleanup changes."""

    def test_missing_diff_removed_from_refusal_reason(self) -> None:
        """``missing_diff`` must not appear in ``RefusalReason``."""
        assert "missing_diff" not in get_args(RefusalReason)

    def test_max_args_bytes_is_32k(self) -> None:
        assert audit.MAX_ARGS_BYTES == 32 * 1024

    def test_db_reader_budget_constant_exists(self) -> None:
        assert DEFAULT_MAX_DB_READER_OPS == 8
