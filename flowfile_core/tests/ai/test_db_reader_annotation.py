"""Tests for database_reader query change annotation

Verifies that when the AI agent modifies a ``database_reader`` node's query,
the staged diff is annotated as high-risk and a warning is emitted.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from flowfile_core.ai.tools.executor.handlers.update import _handle_update_node_settings
from flowfile_core.ai.tools.dry_run import DryRunCache
from flowfile_core.schemas import input_schema


def _db_reader_settings(
    query: str = "SELECT * FROM users",
    table_name: str | None = None,
    query_mode: str = "query",
) -> input_schema.NodeDatabaseReader:
    """Build a minimal ``NodeDatabaseReader`` with inline connection."""
    return input_schema.NodeDatabaseReader(
        flow_id=1,
        node_id=10,
        database_settings=input_schema.DatabaseSettings(
            connection_mode="inline",
            database_connection=input_schema.DatabaseConnection(
                database_type="postgresql",
                host="localhost",
                port=5432,
                database="testdb",
                username="user",
                password="pass",
            ),
            query=query,
            query_mode=query_mode,
            table_name=table_name,
        ),
    )


def _mock_flow_with_db_reader(settings: input_schema.NodeDatabaseReader) -> MagicMock:
    """Create a mock flow with a database_reader node."""
    node = MagicMock()
    node.node_type = "database_reader"
    node.setting_input = settings
    node.node_inputs.main_inputs = []
    node.node_inputs.right_input = None

    flow = MagicMock()
    flow.flow_id = 1
    flow.get_node.return_value = node
    return flow


class TestDatabaseReaderQueryAnnotation:
    """Tests for high-risk annotation on database_reader query changes."""

    def test_query_change_produces_high_risk_annotation(self) -> None:
        """Changing a database_reader's query flags the diff as high-risk."""
        old_settings = _db_reader_settings(query="SELECT * FROM users")
        flow = _mock_flow_with_db_reader(old_settings)

        new_settings_payload = old_settings.model_dump(mode="json")
        new_settings_payload["database_settings"]["query"] = "SELECT * FROM users WHERE role = 'admin'"

        result = _handle_update_node_settings(
            tool_name="flowfile.graph.update_node_settings",
            tool_args={"flow_id": 1, "node_id": 10, "settings": new_settings_payload},
            redacted_args={"flow_id": 1, "node_id": 10, "settings": new_settings_payload},
            flow=flow,
            session_id="test-session-todo5",
            user_id=1,
            mode="stage",
            dry_run_cache=DryRunCache(),
        )

        assert result.status == "staged", result.refusal_detail
        assert result.staged_node_payload is not None
        assert result.staged_node_payload["high_risk"] is True
        assert any("high-risk" in w for w in result.warnings)

    def test_same_query_no_annotation(self) -> None:
        """Updating a database_reader without changing query does not flag."""
        old_settings = _db_reader_settings(query="SELECT * FROM users")
        flow = _mock_flow_with_db_reader(old_settings)

        # Same query, no change
        new_settings_payload = old_settings.model_dump(mode="json")

        result = _handle_update_node_settings(
            tool_name="flowfile.graph.update_node_settings",
            tool_args={"flow_id": 1, "node_id": 10, "settings": new_settings_payload},
            redacted_args={"flow_id": 1, "node_id": 10, "settings": new_settings_payload},
            flow=flow,
            session_id="test-session-todo5",
            user_id=1,
            mode="stage",
            dry_run_cache=DryRunCache(),
        )

        assert result.status == "staged", result.refusal_detail
        assert result.staged_node_payload is not None
        assert result.staged_node_payload["high_risk"] is False
        assert not any("high-risk" in w for w in result.warnings)
