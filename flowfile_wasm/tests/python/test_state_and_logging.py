"""State-lifecycle and logging contract tests.

clear_all() is the bridge's flow-switch reset (flow-store.ts runs it before
every flow import/execution); it must empty every module-level dict in
engine.state or stale LazyFrames from the previous flow stay pinned. The
log_node decorator wraps every execute_* function and must stay transparent:
same name (the Pyodide namespace dump keys on it) and same result dicts.
"""
import logging

import engine
from engine import state


def _populate_all_state():
    engine.execute_read_csv(1, "a,b\n1,2\n", {})
    engine.fetch_preview(1)
    engine.propagate_schemas(
        {
            "order": [1, 2],
            "nodes": {
                "1": {"type": "read", "input_ids": [], "settings": {}},
                "2": {"type": "filter", "input_ids": [1], "settings": {}},
            },
        },
        {"1": [{"name": "a", "data_type": "Int64"}, {"name": "b", "data_type": "Int64"}]},
    )


def test_clear_all_empties_every_state_dict():
    _populate_all_state()
    dicts = (
        state._lazyframes,
        state._schemas,
        state._preview_cache,
        state._plan_hashes,
        state._schema_lazyframes,
        state._schema_schemas,
    )
    assert all(len(d) > 0 for d in dicts)

    engine.clear_all()
    assert all(len(d) == 0 for d in dicts)


def test_clear_node_drops_schema_caches_too():
    _populate_all_state()
    engine.clear_node(1)
    assert 1 not in state._lazyframes
    assert 1 not in state._schema_lazyframes
    assert 1 not in state._schema_schemas


def test_set_log_level_roundtrip():
    try:
        assert engine.set_log_level("DEBUG") == "DEBUG"
        assert engine.logger.level == logging.DEBUG
        assert engine.set_log_level("info") == "INFO"
    finally:
        engine.set_log_level("INFO")


def test_log_node_is_transparent():
    assert engine.execute_read_csv.__name__ == "execute_read_csv"
    result = engine.execute_read_csv(1, "a\n1\n", {})
    assert result == {"success": True, "schema": [{"name": "a", "data_type": "Int64"}], "has_data": True}


def test_log_node_warns_with_node_error_on_failure(caplog):
    engine.logger.propagate = True  # caplog listens on the root logger
    try:
        with caplog.at_level(logging.WARNING, logger="flowfile.engine"):
            result = engine.execute_filter(2, 99, {})
    finally:
        engine.logger.propagate = False

    assert result["success"] is False
    messages = [r.getMessage() for r in caplog.records]
    assert any("execute_filter node=2 failed" in m and "node #99" in m for m in messages)
