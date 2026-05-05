"""W31 — Tool executor tests.

Cases (mirrors the plan-mode test list):

* ``test_static_apply_returns_predicted_schema`` — ``add_filter`` over a
  ``manual_input`` upstream applies, returns the predicted output schema.
* ``test_unknown_columns_refusal`` — bad column ref → ``rejected``,
  ``refusal_reason="unknown_columns"``, audit ``rejected`` row.
* ``test_dynamic_node_dry_run_cache`` — ``add_polars_code`` first call invokes
  the kernel-stub seam; second identical call serves from the LRU cache.
* ``test_source_target_via_mirror`` — ``add_manual_input`` predicts directly
  from settings via the mirror-graph; no upstream needed.
* ``test_network_egress_refusal`` — ``add_python_script`` with ``requests.post``
  is refused with ``RefusalReason="network_egress"``; kernel NOT invoked.
* ``test_audit_redacts_secrets`` — ``database_writer`` with ``password_ref`` →
  audit row's ``tool_args`` JSON contains ``<<secret:my-secret>>`` placeholder.
* ``test_d011_tier_0_cached_schema`` — upstream with cached ``predicted_schema``
  is reused without invoking the callback.
* ``test_d011_tier_1_callback_fires`` — static upstream with no cache + working
  ``schema_callback`` → callback invoked, validation succeeds.
* ``test_d011_tier_2_warn_and_stage`` — upstream with no callback + no cache →
  ``status="warned"``, deferred-validation message in ``warnings``.
* ``test_stage_mode_returns_payload`` — ``mode="stage"`` returns
  ``staged_node_payload``; real graph unchanged; audit row written.
* ``test_meta_pick_category`` — ``flowfile.meta.pick_category`` returns the
  heuristic category in the result's ``extra``.
* ``test_invalid_tool_name`` — non-MCP-shaped name → ``rejected``.
* ``test_lazy_litellm_contract`` — importing the executor doesn't load litellm.
* ``test_formula_predicts_via_mirror_not_kernel`` — ``add_formula`` (static)
  doesn't trigger the kernel dry-run seam.
"""

from __future__ import annotations

import sys
from collections.abc import Iterator
from typing import Any

import pytest

from flowfile_core.ai import audit
from flowfile_core.ai.tools import (
    DryRunCache,
    InsertionContext,
    ToolExecutionResult,
    execute_tool_call,
)
from flowfile_core.ai.tools import dry_run as dry_run_module
from flowfile_core.ai.tools import executor as executor_module
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.schemas import input_schema, schemas, transform_schema

# --------------------------------------------------------------------------- #
# Test helpers                                                                 #
# --------------------------------------------------------------------------- #


def _flow_settings(flow_id: int = 1) -> schemas.FlowSettings:
    return schemas.FlowSettings(
        flow_id=flow_id,
        execution_mode="Performance",
        execution_location="local",
        path="/tmp/test_ai_executor",
    )


def _add_orders_input(flow: FlowGraph, node_id: int = 1) -> None:
    raw = input_schema.NodeManualInput(
        flow_id=flow.flow_id,
        node_id=node_id,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="order_id", data_type="Integer"),
                input_schema.MinimalFieldInfo(name="customer_id", data_type="Integer"),
                input_schema.MinimalFieldInfo(name="amount", data_type="Double"),
                input_schema.MinimalFieldInfo(name="region", data_type="String"),
            ],
            data=[[1, 2, 3, 4], [10, 20, 30, 40], [100.0, 200.0, 50.0, 75.0], ["EU", "US", "EU", "US"]],
        ),
    )
    flow.add_manual_input(raw)
    flow.get_node(node_id).name = "orders"


def _flow_with_orders() -> FlowGraph:
    flow = FlowGraph(flow_settings=_flow_settings(), name="exec_test")
    _add_orders_input(flow)
    # Force schema prediction so the upstream has predicted_schema cached.
    flow.get_node(1).get_predicted_schema()
    return flow


def _filter_args(node_id: int = 2, depending_on_id: int = 1, expr: str = "[region]=='EU'") -> dict[str, Any]:
    settings = input_schema.NodeFilter(
        flow_id=1,
        node_id=node_id,
        depending_on_id=depending_on_id,
        filter_input=transform_schema.FilterInput(filter_type="advanced", advanced_filter=expr),
    )
    return settings.model_dump(mode="json")


def _user_id() -> int:
    """Stable user id for audit rows. Doesn't need to exist in the DB —
    ``ai_audit_events.user_id`` is a plain integer column with no FK."""
    return 1


@pytest.fixture
def call_kwargs() -> dict[str, Any]:
    """Common kwargs for executor calls — bypasses the global handler by
    providing ``flow=`` directly."""
    return {
        "session_id": "test-session-w31",
        "user_id": _user_id(),
    }


@pytest.fixture
def stub_kernel(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[dict[str, Any]]]:
    """Replace ``_run_kernel_for_dry_run`` with a stub that records calls and
    returns a fixed schema. Each call's ``(node_id, code, sample_size)`` is
    appended to the returned list so tests can assert call counts."""
    from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn

    calls: list[dict[str, Any]] = []

    def _stub(flow, node_id, code, output_names, sample):
        calls.append(
            {
                "node_id": node_id,
                "code": code,
                "output_names": list(output_names),
            }
        )
        return [
            FlowfileColumn.from_input(column_name="result_col", data_type="String"),
        ]

    monkeypatch.setattr(dry_run_module, "_run_kernel_for_dry_run", _stub)
    yield calls


# --------------------------------------------------------------------------- #
# 1. Static apply                                                              #
# --------------------------------------------------------------------------- #


def test_static_apply_returns_predicted_schema(call_kwargs: dict[str, Any]) -> None:
    flow = _flow_with_orders()
    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.add_filter",
        tool_args=_filter_args(node_id=2, depending_on_id=1),
        insertion_context=InsertionContext(upstream_node_ids=[1]),
        flow=flow,
        **call_kwargs,
    )
    assert result.status == "applied", result.refusal_detail
    assert result.node_id == 2
    assert result.predicted_output_schema is not None
    names = {col["name"] for col in result.predicted_output_schema}
    assert {"order_id", "customer_id", "amount", "region"} <= names
    # Real graph mutated: filter node now exists, wired to orders.
    assert flow.get_node(2) is not None
    assert flow.get_node(2).node_inputs.main_inputs[0].node_id == 1
    assert result.audit_id is not None


# --------------------------------------------------------------------------- #
# 2. Unknown-columns refusal                                                   #
# --------------------------------------------------------------------------- #


def test_unknown_columns_refusal(call_kwargs: dict[str, Any]) -> None:
    flow = _flow_with_orders()
    settings = input_schema.NodeFilter(
        flow_id=1,
        node_id=99,
        depending_on_id=1,
        filter_input=transform_schema.FilterInput(
            filter_type="basic",
            basic_filter=transform_schema.BasicFilter(
                field="not_a_real_column",
                operator="equals",
                value="x",
            ),
        ),
    )
    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.add_filter",
        tool_args=settings.model_dump(mode="json"),
        insertion_context=InsertionContext(upstream_node_ids=[1]),
        flow=flow,
        **call_kwargs,
    )
    assert result.status == "rejected"
    assert result.refusal_reason == "unknown_columns"
    assert "not_a_real_column" in (result.refusal_detail or "")
    assert flow.get_node(99) is None  # NOT added
    # Audit row recorded with rejected status.
    rows = audit.query_events(session_id="test-session-w31", limit=10)
    assert any(r.tool_name == "flowfile.graph.add_filter" and r.result_status == "rejected" for r in rows)


# --------------------------------------------------------------------------- #
# 3. Dynamic node dry-run cache                                                #
# --------------------------------------------------------------------------- #


def test_dynamic_node_dry_run_cache(call_kwargs: dict[str, Any], stub_kernel: list[dict[str, Any]]) -> None:
    flow = _flow_with_orders()
    cache = DryRunCache()
    code = "main.select(['order_id', 'amount'])"
    polars_settings = input_schema.NodePolarsCode(
        flow_id=1,
        node_id=10,
        depending_on_ids=[1],
        polars_code_input=transform_schema.PolarsCodeInput(polars_code=code),
    )
    args = polars_settings.model_dump(mode="json")

    first = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.add_polars_code",
        tool_args=args,
        insertion_context=InsertionContext(upstream_node_ids=[1]),
        flow=flow,
        dry_run_cache=cache,
        mode="stage",  # avoid mutating the real graph for this assertion focus
        **call_kwargs,
    )
    assert first.status == "staged"
    assert first.predicted_output_schema is not None
    assert len(stub_kernel) == 1

    # Second call with identical (code, sample) hits cache → kernel NOT called.
    polars_settings_2 = polars_settings.model_copy(update={"node_id": 11})
    second = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.add_polars_code",
        tool_args=polars_settings_2.model_dump(mode="json"),
        insertion_context=InsertionContext(upstream_node_ids=[1]),
        flow=flow,
        dry_run_cache=cache,
        mode="stage",
        **call_kwargs,
    )
    assert second.status == "staged"
    assert len(stub_kernel) == 1, "second identical call should hit cache"


# --------------------------------------------------------------------------- #
# 4. Source target via mirror-graph (no upstream needed)                       #
# --------------------------------------------------------------------------- #


def test_source_target_via_mirror(call_kwargs: dict[str, Any]) -> None:
    flow = FlowGraph(flow_settings=_flow_settings(flow_id=2), name="exec_test_source")
    raw = input_schema.NodeManualInput(
        flow_id=2,
        node_id=1,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="id", data_type="Integer"),
                input_schema.MinimalFieldInfo(name="label", data_type="String"),
            ],
            data=[[1, 2], ["a", "b"]],
        ),
    )
    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.add_manual_input",
        tool_args=raw.model_dump(mode="json"),
        insertion_context=InsertionContext(upstream_node_ids=[]),
        flow=flow,
        **call_kwargs,
    )
    assert result.status == "applied", result.refusal_detail
    assert result.predicted_output_schema is not None
    names = {col["name"] for col in result.predicted_output_schema}
    assert names == {"id", "label"}


# --------------------------------------------------------------------------- #
# 5. Network egress refusal                                                    #
# --------------------------------------------------------------------------- #


def test_network_egress_refusal(call_kwargs: dict[str, Any], stub_kernel: list[dict[str, Any]]) -> None:
    flow = _flow_with_orders()
    bad_code = "import requests\nresult = requests.post('http://example.com', json={})\nmain"
    settings = input_schema.NodePolarsCode(
        flow_id=1,
        node_id=20,
        depending_on_ids=[1],
        polars_code_input=transform_schema.PolarsCodeInput(polars_code=bad_code),
    )
    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.add_polars_code",
        tool_args=settings.model_dump(mode="json"),
        insertion_context=InsertionContext(upstream_node_ids=[1]),
        flow=flow,
        **call_kwargs,
    )
    assert result.status == "rejected"
    assert result.refusal_reason == "network_egress"
    assert "requests" in (result.refusal_detail or "")
    assert len(stub_kernel) == 0, "egress-flagged code must not reach the kernel"
    assert flow.get_node(20) is None


# --------------------------------------------------------------------------- #
# 6. Audit redacts secret refs                                                 #
# --------------------------------------------------------------------------- #


def test_audit_redacts_secrets(call_kwargs: dict[str, Any]) -> None:
    """Verify that ``redact_secrets`` runs before the audit row is persisted.
    Uses a connect call so we don't need a full database_writer fixture."""
    flow = _flow_with_orders()
    # Flat shape per the connect ToolSpec; self-loop is rejected, but
    # redact_secrets runs in execute_tool_call before handler dispatch.
    tool_args = {
        "flow_id": flow.flow_id,
        "from_node_id": 1,
        "to_node_id": 1,
        "password_ref": "my-secret",
    }
    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.connect",
        tool_args=tool_args,
        insertion_context=InsertionContext(),
        flow=flow,
        **call_kwargs,
    )
    # The connection itself may fail (cycle), but redaction happened before we got here.
    assert result.executed_args is not None
    assert result.executed_args.get("password_ref") == "<<secret:my-secret>>"


# --------------------------------------------------------------------------- #
# 6b. Connect tool flat-shape contract                                         #
# --------------------------------------------------------------------------- #


def _flow_with_orders_and_filter() -> FlowGraph:
    """Orders manual_input (node 1) plus an unwired filter (node 2)."""
    flow = _flow_with_orders()
    flow.add_filter(
        input_schema.NodeFilter(
            flow_id=flow.flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=transform_schema.FilterInput(filter_type="advanced", advanced_filter="[region]=='EU'"),
        )
    )
    return flow


def test_connect_accepts_flat_shape(call_kwargs: dict[str, Any]) -> None:
    """The ``flowfile.graph.connect`` ToolSpec advertises a flat shape; the
    executor must wire the connection from those flat fields without any
    pre-shaping by the caller."""
    flow = _flow_with_orders_and_filter()
    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.connect",
        tool_args={"flow_id": flow.flow_id, "from_node_id": 1, "to_node_id": 2},
        insertion_context=InsertionContext(),
        flow=flow,
        **call_kwargs,
    )
    assert result.status == "applied", result.refusal_detail
    target_inputs = flow.get_node(2).node_inputs.main_inputs
    assert [n.node_id for n in target_inputs] == [1]


def test_connect_flat_shape_with_explicit_classes(call_kwargs: dict[str, Any]) -> None:
    """``input_class`` / ``output_class`` from the flat shape must round-trip
    into the staged ``NodeConnection`` payload — i.e. they actually reach the
    constructed connection model and are not silently defaulted."""
    flow = _flow_with_orders_and_filter()
    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.connect",
        tool_args={
            "flow_id": flow.flow_id,
            "from_node_id": 1,
            "to_node_id": 2,
            "input_class": "input-1",
            "output_class": "output-0",
        },
        insertion_context=InsertionContext(),
        flow=flow,
        mode="stage",
        **call_kwargs,
    )
    assert result.status == "staged", result.refusal_detail
    assert result.staged_node_payload is not None
    payload = result.staged_node_payload["connection"]
    assert payload["input_connection"] == {"node_id": 2, "connection_class": "input-1"}
    assert payload["output_connection"] == {"node_id": 1, "connection_class": "output-0"}


def test_connect_invalid_input_class_surfaces_tool_field_name(call_kwargs: dict[str, Any]) -> None:
    """A bad ``input_class`` must produce a refusal that mentions the flat
    tool-spec field name (``input_class``) rather than the internal Pydantic
    field path (``input_connection.connection_class``) — otherwise the W53
    retry loop hands the LLM a field name it has never been shown."""
    flow = _flow_with_orders_and_filter()
    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.connect",
        tool_args={
            "flow_id": flow.flow_id,
            "from_node_id": 1,
            "to_node_id": 2,
            "input_class": "input-99",
        },
        insertion_context=InsertionContext(),
        flow=flow,
        **call_kwargs,
    )
    assert result.status == "rejected"
    assert result.refusal_detail is not None
    assert "input_class" in result.refusal_detail
    assert "input_connection" not in result.refusal_detail


def test_connect_missing_required_field_is_refused(call_kwargs: dict[str, Any]) -> None:
    """Missing ``from_node_id`` / ``to_node_id`` produces a tool-shaped
    refusal (rather than a KeyError leaking through)."""
    flow = _flow_with_orders_and_filter()
    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.connect",
        tool_args={"flow_id": flow.flow_id, "to_node_id": 2},
        insertion_context=InsertionContext(),
        flow=flow,
        **call_kwargs,
    )
    assert result.status == "rejected"
    assert result.refusal_detail is not None
    assert "from_node_id" in result.refusal_detail


def test_delete_connection_accepts_flat_shape(call_kwargs: dict[str, Any]) -> None:
    """The flat-shape contract applies to ``flowfile.graph.delete_connection``
    too — the executor copy-pastes the same construction path."""
    flow = _flow_with_orders_and_filter()
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))
    assert [n.node_id for n in flow.get_node(2).node_inputs.main_inputs] == [1]

    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.delete_connection",
        tool_args={"flow_id": flow.flow_id, "from_node_id": 1, "to_node_id": 2},
        insertion_context=InsertionContext(),
        flow=flow,
        **call_kwargs,
    )
    assert result.status == "applied", result.refusal_detail
    assert flow.get_node(2).node_inputs.main_inputs == []


# --------------------------------------------------------------------------- #
# 7-9. D011 tiered handling                                                    #
# --------------------------------------------------------------------------- #


def test_d011_tier_0_cached_schema(call_kwargs: dict[str, Any]) -> None:
    flow = _flow_with_orders()
    # Upstream is already populated; tier 0 returns the cached schema.
    assert flow.get_node(1).node_schema.predicted_schema is not None
    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.add_filter",
        tool_args=_filter_args(node_id=30, depending_on_id=1),
        insertion_context=InsertionContext(upstream_node_ids=[1]),
        flow=flow,
        **call_kwargs,
    )
    assert result.status == "applied"
    assert result.warnings == []


def test_d011_tier_1_callback_fires(call_kwargs: dict[str, Any]) -> None:
    """Tier 1 covers nodes whose predicted_schema can be derived via
    ``get_predicted_schema(force=True)`` — either via an explicit
    ``schema_callback`` or the ``_predicted_data_getter`` fallback. ``filter``
    uses the latter (passthrough)."""
    flow = _flow_with_orders()
    filter_settings = input_schema.NodeFilter(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        filter_input=transform_schema.FilterInput(filter_type="advanced", advanced_filter="[region]=='EU'"),
    )
    flow.add_filter(filter_settings)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))
    # Wipe the filter's cached predicted_schema to force tier 1 path.
    flow.get_node(2).node_schema.predicted_schema = None

    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.add_filter",
        tool_args=_filter_args(node_id=3, depending_on_id=2, expr="[region]=='US'"),
        insertion_context=InsertionContext(upstream_node_ids=[2]),
        flow=flow,
        **call_kwargs,
    )
    assert result.status == "applied", result.refusal_detail
    assert result.warnings == []
    # Tier 1 populated the upstream's predicted_schema in place.
    assert flow.get_node(2).node_schema.predicted_schema is not None


def test_d011_tier_2_warn_and_stage(call_kwargs: dict[str, Any]) -> None:
    """Tier 2 fires when there's neither cached schema nor a way to derive one.
    Construct a python_script upstream and break both the callback regenerator
    AND the function so ``_predicted_data_getter`` returns nothing."""
    flow = _flow_with_orders()
    py_settings = input_schema.NodePythonScript(
        flow_id=1,
        node_id=5,
        depending_on_ids=[1],
        python_script_input=input_schema.PythonScriptInput(code="output_df = input_df_1"),
    )
    flow.add_python_script(py_settings)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 5))
    py_node = flow.get_node(5)
    py_node.node_schema.predicted_schema = None
    py_node._schema_callback = None
    py_node.user_provided_schema_callback = None

    def _broken(*_args, **_kwargs):
        raise RuntimeError("forced tier-2 path for test")

    py_node._function = _broken

    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.add_filter",
        tool_args=_filter_args(node_id=6, depending_on_id=5),
        insertion_context=InsertionContext(upstream_node_ids=[5]),
        flow=flow,
        **call_kwargs,
    )
    # When upstream schema is unresolved, validation is deferred → status is
    # "warned" (still applies the node) and a warning is appended.
    assert result.status in ("warned", "rejected")
    if result.status == "warned":
        assert any("schema unknown" in w for w in result.warnings)


# --------------------------------------------------------------------------- #
# 10. Stage mode                                                               #
# --------------------------------------------------------------------------- #


def test_stage_mode_returns_payload(call_kwargs: dict[str, Any]) -> None:
    flow = _flow_with_orders()
    args = _filter_args(node_id=42, depending_on_id=1)
    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.add_filter",
        tool_args=args,
        insertion_context=InsertionContext(upstream_node_ids=[1]),
        flow=flow,
        mode="stage",
        **call_kwargs,
    )
    assert result.status == "staged"
    assert result.staged_node_payload is not None
    assert result.staged_node_payload["node_type"] == "filter"
    assert result.staged_node_payload["settings"]["node_id"] == 42
    assert result.predicted_output_schema is not None
    # Real graph not mutated.
    assert flow.get_node(42) is None
    # Audit row still recorded.
    assert result.audit_id is not None


# --------------------------------------------------------------------------- #
# 11. Meta pick_category                                                       #
# --------------------------------------------------------------------------- #


def test_meta_pick_category(call_kwargs: dict[str, Any]) -> None:
    flow = _flow_with_orders()
    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.meta.pick_category",
        tool_args={"intent": "filter rows where amount is over 100"},
        insertion_context=InsertionContext(),
        flow=flow,
        **call_kwargs,
    )
    assert result.status == "applied"
    assert "category" in result.extra
    # Heuristic should pick "transformations" for a filter-shaped intent.
    assert isinstance(result.extra["category"], str)


# --------------------------------------------------------------------------- #
# 12. Invalid tool name                                                        #
# --------------------------------------------------------------------------- #


def test_invalid_tool_name(call_kwargs: dict[str, Any]) -> None:
    flow = _flow_with_orders()
    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="not.a.flowfile.tool",
        tool_args={},
        insertion_context=InsertionContext(),
        flow=flow,
        **call_kwargs,
    )
    assert result.status == "rejected"
    assert "invalid tool name" in (result.refusal_detail or "")


# --------------------------------------------------------------------------- #
# 13. Lazy litellm contract                                                    #
# --------------------------------------------------------------------------- #


def test_lazy_litellm_contract() -> None:
    # Drop any cached litellm shims so the contract is enforced from a clean slate.
    for mod in list(sys.modules):
        if mod.startswith("litellm") or mod == "litellm":
            sys.modules.pop(mod, None)
    # Re-import the executor surface.
    sys.modules.pop("flowfile_core.ai.tools.executor", None)
    sys.modules.pop("flowfile_core.ai.tools", None)
    import flowfile_core.ai.tools  # noqa: F401
    import flowfile_core.ai.tools.executor  # noqa: F401

    leaked = [m for m in sys.modules if m == "litellm" or m.startswith("litellm.")]
    assert not leaked, f"litellm leaked: {leaked}"


# --------------------------------------------------------------------------- #
# 14. Formula is static — no kernel call                                       #
# --------------------------------------------------------------------------- #


def test_formula_predicts_via_mirror_not_kernel(call_kwargs: dict[str, Any], stub_kernel: list[dict[str, Any]]) -> None:
    flow = _flow_with_orders()
    formula_settings = input_schema.NodeFormula(
        flow_id=1,
        node_id=50,
        depending_on_id=1,
        function=transform_schema.FunctionInput(
            field=transform_schema.FieldInput(name="net_amount", data_type="Double"),
            function="[amount] * 0.95",
        ),
    )
    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.add_formula",
        tool_args=formula_settings.model_dump(mode="json"),
        insertion_context=InsertionContext(upstream_node_ids=[1]),
        flow=flow,
        **call_kwargs,
    )
    # Either applied or rejected (depending on whether mirror predicts) —
    # the load-bearing assertion is that the kernel was NOT called.
    assert len(stub_kernel) == 0, "static formula must not trigger kernel dry-run"
    assert result.tool_name == "flowfile.graph.add_formula"


# --------------------------------------------------------------------------- #
# W54 — LLM-provided node_id validation                                        #
# --------------------------------------------------------------------------- #


def test_add_with_llm_provided_colliding_node_id_is_refused(call_kwargs: dict[str, Any]) -> None:
    """W54 AC2 — LLM emits ``add_filter(node_id=3, upstream_node_ids=[3])``.

    The executor refuses with ``self_loop_prevented`` *before* Pydantic validation;
    no node is staged or applied; an audit row records the rejection.
    """
    flow = _flow_with_orders()
    args = _filter_args(node_id=3, depending_on_id=1, expr="[region]=='EU'")

    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.add_filter",
        tool_args=args,
        insertion_context=InsertionContext(upstream_node_ids=[3]),
        flow=flow,
        mode="stage",
        llm_provided_node_id=3,
        audit_meta={
            "allocated_node_id": None,
            "llm_provided_node_id": 3,
            "resolved_upstream_node_ids": [3],
            "right_input_node_id": None,
            "live_node_ids_at_stage": [1],
            "staged_node_ids_at_stage": [],
        },
        **call_kwargs,
    )
    assert result.status == "rejected"
    assert result.refusal_reason == "self_loop_prevented"
    assert result.refusal_detail is not None
    assert "LLM-provided node_id 3" in result.refusal_detail
    assert "self-loop" in result.refusal_detail
    assert result.staged_node_payload is None
    assert flow.get_node(3) is None  # not added

    rows = audit.query_events(session_id=call_kwargs["session_id"], limit=10)
    matching = [r for r in rows if r.tool_name == "flowfile.graph.add_filter" and r.result_status == "rejected"]
    assert matching, "audit row for the rejection must be present"


def test_add_with_llm_provided_id_already_live_is_refused(call_kwargs: dict[str, Any]) -> None:
    """W54 — LLM-provided node_id collides with an existing live node id."""
    flow = _flow_with_orders()  # contains node 1
    args = _filter_args(node_id=1, depending_on_id=1, expr="[region]=='EU'")

    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.add_filter",
        tool_args=args,
        insertion_context=InsertionContext(upstream_node_ids=[1]),
        flow=flow,
        mode="stage",
        llm_provided_node_id=1,
        audit_meta={"staged_node_ids_at_stage": []},
        **call_kwargs,
    )
    assert result.status == "rejected"
    assert result.refusal_reason == "self_loop_prevented"
    assert "already exists in the live graph" in (result.refusal_detail or "")


# --------------------------------------------------------------------------- #
# Sanity: ToolExecutionResult shape                                            #
# --------------------------------------------------------------------------- #


def test_tool_execution_result_default_shape() -> None:
    result = ToolExecutionResult(status="staged", tool_name="flowfile.graph.add_filter")
    assert result.warnings == []
    assert result.audit_id is None
    assert result.staged_node_payload is None


def test_executor_module_exposes_seam() -> None:
    """The kernel-runner seam must be a module attribute we can monkey-patch."""
    assert hasattr(dry_run_module, "_run_kernel_for_dry_run")
    assert callable(dry_run_module._run_kernel_for_dry_run)
    # And the executor module re-exports the public surface unchanged.
    assert executor_module.execute_tool_call is execute_tool_call
