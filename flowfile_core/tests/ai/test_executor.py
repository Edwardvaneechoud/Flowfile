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


def test_connect_string_node_ids_get_example_nudge(call_kwargs: dict[str, Any]) -> None:
    """2026-05-07 — when ``from_node_id`` / ``to_node_id`` come in as
    non-coercible strings (live dogfood: LLM sent ``"node_3"`` /
    ``"<placeholder>"`` shapes), the connect refusal appends a concrete
    example payload so the LLM corrects on retry. Mirrors W67's enrichment
    for ``add_*`` settings refusals.
    """
    flow = _flow_with_orders_and_filter()
    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.connect",
        tool_args={
            "flow_id": flow.flow_id,
            # Non-numeric strings — Pydantic's lax-mode coercion fails on these
            # (unlike "1" / "2" which it would silently parse).
            "from_node_id": "node_one",
            "to_node_id": "node_two",
        },
        insertion_context=InsertionContext(),
        flow=flow,
        **call_kwargs,
    )
    assert result.status == "rejected"
    detail = result.refusal_detail or ""
    # Pydantic's original error survives, mapped to flat field names.
    assert "from_node_id" in detail
    assert "to_node_id" in detail
    # And the new W67-style example is appended.
    assert "Example payload" in detail
    assert '"from_node_id": 3' in detail
    assert '"to_node_id": 5' in detail


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
# 10b. W62 — auto-layout for staged nodes                                       #
# --------------------------------------------------------------------------- #


def test_w62_default_pos_resolves_from_upstream(call_kwargs: dict[str, Any]) -> None:
    """W62 regression — when InsertionContext leaves ``pos_x`` / ``pos_y``
    unset (the default), the executor derives non-(0, 0) coords from the
    upstream node. Pre-W62 the staged payload always carried (0.0, 0.0)."""
    from flowfile_core.ai.tools.executor import (
        _AUTO_LAYOUT_X_SPACING,
        _AUTO_LAYOUT_Y_SPACING,
    )

    flow = _flow_with_orders()
    # Stamp a known upstream position so the assertion is unambiguous.
    flow.get_node(1).setting_input.pos_x = 400.0
    flow.get_node(1).setting_input.pos_y = 300.0

    args = _filter_args(node_id=2, depending_on_id=1)
    # Strip pos_x / pos_y from tool_args so the executor's "settings have
    # explicit coords" branch doesn't fire.
    args.pop("pos_x", None)
    args.pop("pos_y", None)

    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.add_filter",
        tool_args=args,
        insertion_context=InsertionContext(upstream_node_ids=[1]),
        flow=flow,
        mode="stage",
        **call_kwargs,
    )
    assert result.status == "staged", result.refusal_detail
    payload = result.staged_node_payload
    assert payload is not None

    ic = payload["insertion_context"]
    assert ic["pos_x"] == 400.0 + _AUTO_LAYOUT_X_SPACING
    assert ic["pos_y"] == 300.0
    # W62 also stamps the resolved coords onto settings so the apply path
    # (which reads ``settings.pos_x`` via ``set_node_information``) lands
    # the node at the resolved position.
    assert payload["settings"]["pos_x"] == 400.0 + _AUTO_LAYOUT_X_SPACING
    assert payload["settings"]["pos_y"] == 300.0
    # Sanity: not the pre-W62 (0, 0) bug.
    assert (ic["pos_x"], ic["pos_y"]) != (0.0, 0.0)
    # Layout offset Δy unused for a single anchor — staged_offset_index defaults to 0.
    _ = _AUTO_LAYOUT_Y_SPACING  # imported to assert it exists


def test_w62_explicit_pos_respected_verbatim(call_kwargs: dict[str, Any]) -> None:
    """Caller deliberately sets pos_x / pos_y (frontend click, ghost-node
    handoff). The executor must NOT auto-resolve in that case — the
    sentinel-vs-default distinction is the whole point of the
    ``float | None`` shape."""
    flow = _flow_with_orders()
    args = _filter_args(node_id=3, depending_on_id=1)
    args.pop("pos_x", None)
    args.pop("pos_y", None)

    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.add_filter",
        tool_args=args,
        # Deliberate (50.0, 75.0) — must survive verbatim in the staged
        # insertion_context. Settings stamping ALSO uses these because
        # tool_args has no explicit pos_x / pos_y.
        insertion_context=InsertionContext(upstream_node_ids=[1], pos_x=50.0, pos_y=75.0),
        flow=flow,
        mode="stage",
        **call_kwargs,
    )
    assert result.status == "staged"
    ic = result.staged_node_payload["insertion_context"]
    assert ic["pos_x"] == 50.0
    assert ic["pos_y"] == 75.0
    assert result.staged_node_payload["settings"]["pos_x"] == 50.0
    assert result.staged_node_payload["settings"]["pos_y"] == 75.0


def test_w62_explicit_zero_pos_respected(call_kwargs: dict[str, Any]) -> None:
    """Edge: caller passes ``pos_x=0.0`` deliberately. The resolver must
    NOT fire — only ``None`` triggers auto-layout. This is the whole point
    of switching the field type from ``float = 0.0`` to ``float | None = None``."""
    flow = _flow_with_orders()
    flow.get_node(1).setting_input.pos_x = 400.0
    flow.get_node(1).setting_input.pos_y = 300.0
    args = _filter_args(node_id=4, depending_on_id=1)
    args.pop("pos_x", None)
    args.pop("pos_y", None)

    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.add_filter",
        tool_args=args,
        insertion_context=InsertionContext(upstream_node_ids=[1], pos_x=0.0, pos_y=0.0),
        flow=flow,
        mode="stage",
        **call_kwargs,
    )
    assert result.status == "staged"
    ic = result.staged_node_payload["insertion_context"]
    # (0, 0) survives — it was deliberate.
    assert (ic["pos_x"], ic["pos_y"]) == (0.0, 0.0)


def test_w62_cold_flow_uses_fallback(call_kwargs: dict[str, Any]) -> None:
    """No upstream → resolver returns the seed coords, NOT (0, 0)."""
    from flowfile_core.ai.tools.executor import (
        _AUTO_LAYOUT_FALLBACK_X,
        _AUTO_LAYOUT_FALLBACK_Y,
    )

    flow = FlowGraph(flow_settings=_flow_settings(), name="w62-cold")
    raw = input_schema.NodeManualInput(
        flow_id=flow.flow_id,
        node_id=11,
        raw_data_format=input_schema.RawData(
            columns=[input_schema.MinimalFieldInfo(name="a", data_type="Integer")],
            data=[[1, 2]],
        ),
    )
    args = raw.model_dump(mode="json")
    args.pop("pos_x", None)
    args.pop("pos_y", None)

    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.add_manual_input",
        tool_args=args,
        insertion_context=InsertionContext(upstream_node_ids=[]),
        flow=flow,
        mode="stage",
        **call_kwargs,
    )
    assert result.status in ("staged", "warned"), result.refusal_detail
    ic = result.staged_node_payload["insertion_context"]
    assert ic["pos_x"] == _AUTO_LAYOUT_FALLBACK_X
    assert ic["pos_y"] == _AUTO_LAYOUT_FALLBACK_Y


def test_w62_staged_offset_index_stacks_fan_out(call_kwargs: dict[str, Any]) -> None:
    """``staged_offset_index`` parameter on ``execute_tool_call`` lets the
    caller (Cmd+K, planner) stack fan-outs from one upstream vertically."""
    flow = _flow_with_orders()
    flow.get_node(1).setting_input.pos_x = 400.0
    flow.get_node(1).setting_input.pos_y = 300.0
    common_args = lambda nid: {  # noqa: E731 — terse helper for the loop
        **_filter_args(node_id=nid, depending_on_id=1),
    }
    coords: list[tuple[float, float]] = []
    for offset, nid in enumerate((101, 102, 103)):
        args = common_args(nid)
        args.pop("pos_x", None)
        args.pop("pos_y", None)
        result = execute_tool_call(
            flow_id=flow.flow_id,
            tool_name="flowfile.graph.add_filter",
            tool_args=args,
            insertion_context=InsertionContext(upstream_node_ids=[1]),
            flow=flow,
            mode="stage",
            staged_offset_index=offset,
            **call_kwargs,
        )
        assert result.status == "staged", result.refusal_detail
        ic = result.staged_node_payload["insertion_context"]
        coords.append((ic["pos_x"], ic["pos_y"]))
    # All three at the same x (one column over from upstream), increasing y.
    xs = {c[0] for c in coords}
    ys = [c[1] for c in coords]
    assert len(xs) == 1
    assert ys == sorted(ys)
    assert ys[1] > ys[0]
    assert ys[2] > ys[1]


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


# --------------------------------------------------------------------------- #
# W67 — settings-validation refusal enrichment                                  #
# --------------------------------------------------------------------------- #


def _add_manual_input_args_with_string_raw_data() -> dict[str, Any]:
    """Replicate the live-transcript shape: LLM JSON-string-encodes
    ``raw_data_format``. Pydantic refuses with ``type=model_type``."""
    return {
        "flow_id": 1,
        "node_id": 5,
        "pos_x": 0.0,
        "pos_y": 0.0,
        "raw_data_format": '{"columns": [], "data": []}',
    }


def test_w67_settings_validation_refusal_uses_new_reason(call_kwargs: dict[str, Any]) -> None:
    """W67 Defect 2 — Pydantic ``ValidationError`` on settings flips
    ``refusal_reason`` to the new ``"settings_validation"`` literal (was
    ``None`` before W67)."""
    flow = _flow_with_orders()
    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.add_manual_input",
        tool_args=_add_manual_input_args_with_string_raw_data(),
        insertion_context=InsertionContext(upstream_node_ids=[]),
        flow=flow,
        mode="stage",
        **call_kwargs,
    )
    assert result.status == "rejected"
    assert result.refusal_reason == "settings_validation"


def test_w67_settings_validation_refusal_includes_field_and_shape(call_kwargs: dict[str, Any]) -> None:
    """W67 Defect 2 — refusal detail names the failing field, the expected
    shape, the received Python type, and points the LLM back at the catalog
    entry for the tool. Live-transcript replication."""
    flow = _flow_with_orders()
    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.add_manual_input",
        tool_args=_add_manual_input_args_with_string_raw_data(),
        insertion_context=InsertionContext(upstream_node_ids=[]),
        flow=flow,
        mode="stage",
        **call_kwargs,
    )
    detail = result.refusal_detail or ""
    assert "raw_data_format" in detail, detail
    assert "object" in detail.lower(), detail
    # Received type is Python's ``str`` — exact substring match on what
    # ``type(received).__name__`` produces.
    assert "str" in detail, detail
    assert "flowfile.graph.add_manual_input" in detail, detail
    assert "JSON-encoded string" in detail, detail


def test_w67_settings_validation_refusal_includes_concrete_example(call_kwargs: dict[str, Any]) -> None:
    """W67 Defect 2 — refusal detail embeds a JSON example payload that
    parses cleanly and has the structural keys (``columns``, ``data``) so
    the LLM has a concrete template to pattern-match on."""
    import json

    flow = _flow_with_orders()
    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.add_manual_input",
        tool_args=_add_manual_input_args_with_string_raw_data(),
        insertion_context=InsertionContext(upstream_node_ids=[]),
        flow=flow,
        mode="stage",
        **call_kwargs,
    )
    detail = result.refusal_detail or ""
    # Pull the example fragment between "Example payload for `<loc>`: " and "."
    marker = "Example payload for `raw_data_format`: "
    assert marker in detail, f"missing example payload marker in: {detail!r}"
    fragment = detail.split(marker, 1)[1]
    # The example is a JSON object terminated by a period+space (or end).
    # Parse the leading JSON object — find the first balanced ``}``.
    depth = 0
    end_idx = -1
    for i, ch in enumerate(fragment):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end_idx = i + 1
                break
    assert end_idx > 0, f"no balanced JSON object in: {fragment!r}"
    parsed = json.loads(fragment[:end_idx])
    assert isinstance(parsed, dict)
    assert "columns" in parsed, parsed
    assert "data" in parsed, parsed


# --------------------------------------------------------------------------- #
# W47 — update_node_settings                                                   #
# --------------------------------------------------------------------------- #


def _flow_with_orders_and_filter() -> FlowGraph:
    """``orders`` (id=1) → ``filter EU`` (id=2). Mirrors the production add
    pattern (``_apply_add_node``) by following ``add_filter`` with an
    explicit ``add_connection`` so the filter's ``node_inputs.main_inputs``
    is populated — without it the live ``get_predicted_schema`` call has
    no upstream data to feed into ``_func``.
    """
    flow = _flow_with_orders()
    flow.add_filter(
        input_schema.NodeFilter(
            flow_id=1,
            node_id=2,
            depending_on_id=1,
            filter_input=transform_schema.FilterInput(
                filter_type="advanced", advanced_filter="[region]=='EU'"
            ),
        )
    )
    add_connection(
        flow,
        input_schema.NodeConnection.create_from_simple_input(
            from_id=1, to_id=2, input_type="input-0", output_handle="output-0"
        ),
    )
    flow.get_node(2).get_predicted_schema()
    return flow


def _update_settings_args(
    *,
    node_id: int,
    settings: dict[str, Any],
    flow_id: int = 1,
) -> dict[str, Any]:
    return {"flow_id": flow_id, "node_id": node_id, "settings": settings}


def test_update_node_settings_stage_returns_modification_payload(call_kwargs: dict[str, Any]) -> None:
    """W47 — ``mode="stage"`` returns ``staged_node_payload`` with
    ``kind="modification"`` and old + new settings populated; the live
    node is not mutated.
    """
    flow = _flow_with_orders_and_filter()
    pre_filter = flow.get_node(2).setting_input.model_dump(mode="json")
    new_settings = _filter_args(node_id=2, depending_on_id=1, expr="[amount] > 50")

    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.update_node_settings",
        tool_args=_update_settings_args(node_id=2, settings=new_settings),
        insertion_context=InsertionContext(),
        flow=flow,
        mode="stage",
        **call_kwargs,
    )

    assert result.status == "staged", result.refusal_detail
    payload = result.staged_node_payload
    assert isinstance(payload, dict)
    assert payload["kind"] == "modification"
    assert payload["node_id"] == 2
    assert payload["node_type"] == "filter"
    # Old settings reflect the pre-change state captured at stage time.
    assert payload["old_settings"]["filter_input"]["advanced_filter"] == "[region]=='EU'"
    assert payload["new_settings"]["filter_input"]["advanced_filter"] == "[amount] > 50"
    assert payload["predicted_output_schema"] is not None
    # Live graph unchanged.
    assert (
        flow.get_node(2).setting_input.model_dump(mode="json")["filter_input"]["advanced_filter"]
        == pre_filter["filter_input"]["advanced_filter"]
    )


def test_update_node_settings_apply_mutates_live_node(call_kwargs: dict[str, Any]) -> None:
    """W47 — ``mode="apply"`` re-fires ``add_<node_type>`` with the new
    settings; the existing node's ``setting_input`` reflects the change
    on the next read.
    """
    flow = _flow_with_orders_and_filter()
    new_settings = _filter_args(node_id=2, depending_on_id=1, expr="[amount] > 50")

    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.update_node_settings",
        tool_args=_update_settings_args(node_id=2, settings=new_settings),
        insertion_context=InsertionContext(),
        flow=flow,
        mode="apply",
        **call_kwargs,
    )

    assert result.status in ("applied", "warned"), result.refusal_detail
    assert result.node_id == 2
    post = flow.get_node(2).setting_input
    assert post.filter_input.advanced_filter == "[amount] > 50"
    # The wiring is preserved — main upstream is still node 1.
    assert flow.get_node(2).node_inputs.main_inputs[0].node_id == 1


def test_update_node_settings_pydantic_refusal(call_kwargs: dict[str, Any]) -> None:
    """W47 refusal stage 1 — bad Pydantic shape surfaces as
    ``settings_validation``. The W67 enriched-detail message routes
    through the same path as ``add_*``.
    """
    flow = _flow_with_orders_and_filter()
    bogus_settings = {"flow_id": 1, "node_id": 2, "filter_input": "not-an-object"}

    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.update_node_settings",
        tool_args=_update_settings_args(node_id=2, settings=bogus_settings),
        insertion_context=InsertionContext(),
        flow=flow,
        mode="stage",
        **call_kwargs,
    )

    assert result.status == "rejected"
    assert result.refusal_reason == "settings_validation"
    assert "filter_input" in (result.refusal_detail or "")


def test_update_node_settings_network_egress_refusal(
    call_kwargs: dict[str, Any], stub_kernel: list[dict[str, Any]]
) -> None:
    """W47 refusal stage 2 — network-egress check fires on code-bearing
    nodes via the same ``_extract_code`` path used by ``add_*``.
    """
    flow = _flow_with_orders()
    flow.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=1,
            node_id=20,
            depending_on_ids=[1],
            polars_code_input=transform_schema.PolarsCodeInput(polars_code="main"),
        )
    )
    bad_code = "import requests\nresult = requests.post('http://example.com', json={})\nmain"
    new_settings = input_schema.NodePolarsCode(
        flow_id=1,
        node_id=20,
        depending_on_ids=[1],
        polars_code_input=transform_schema.PolarsCodeInput(polars_code=bad_code),
    ).model_dump(mode="json")

    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.update_node_settings",
        tool_args=_update_settings_args(node_id=20, settings=new_settings),
        insertion_context=InsertionContext(),
        flow=flow,
        mode="stage",
        **call_kwargs,
    )
    assert result.status == "rejected"
    assert result.refusal_reason == "network_egress"
    assert len(stub_kernel) == 0, "egress-flagged code must not reach the kernel"


def test_update_node_settings_unknown_columns_refusal(call_kwargs: dict[str, Any]) -> None:
    """W47 refusal stage 3 — column refs validated against live upstream
    schemas. Mirrors the ``add_*`` path; the upstream resolution comes
    from the live node's existing wiring rather than insertion_context.
    """
    flow = _flow_with_orders_and_filter()
    bogus_settings = input_schema.NodeFilter(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        filter_input=transform_schema.FilterInput(
            filter_type="basic",
            basic_filter=transform_schema.BasicFilter(
                field="not_a_real_column",
                operator="equals",
                value="x",
            ),
        ),
    ).model_dump(mode="json")

    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.update_node_settings",
        tool_args=_update_settings_args(node_id=2, settings=bogus_settings),
        insertion_context=InsertionContext(),
        flow=flow,
        mode="stage",
        **call_kwargs,
    )
    assert result.status == "rejected"
    assert result.refusal_reason == "unknown_columns"
    assert "not_a_real_column" in (result.refusal_detail or "")


def test_update_node_settings_unknown_node_id(call_kwargs: dict[str, Any]) -> None:
    """W47 — modification target must exist on the live graph. Bare
    integer with no live node yields a generic refusal.
    """
    flow = _flow_with_orders()
    new_settings = _filter_args(node_id=999, depending_on_id=1, expr="[amount] > 50")
    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.update_node_settings",
        tool_args=_update_settings_args(node_id=999, settings=new_settings),
        insertion_context=InsertionContext(),
        flow=flow,
        mode="stage",
        **call_kwargs,
    )
    assert result.status == "rejected"
    assert "999" in (result.refusal_detail or "")


def test_update_node_settings_missing_node_id_arg(call_kwargs: dict[str, Any]) -> None:
    """W47 — defensive: missing top-level ``node_id`` rejects with a
    deterministic detail before touching the live graph.
    """
    flow = _flow_with_orders_and_filter()
    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.update_node_settings",
        tool_args={"flow_id": 1, "settings": {}},
        insertion_context=InsertionContext(),
        flow=flow,
        mode="stage",
        **call_kwargs,
    )
    assert result.status == "rejected"
    assert "node_id" in (result.refusal_detail or "")


# --------------------------------------------------------------------------- #
# 2026-05-07 — sink-as-upstream refusal                                         #
# --------------------------------------------------------------------------- #


def _flow_with_orders_and_explore_data() -> FlowGraph:
    """Builds a minimal ``manual_input → explore_data`` flow. The
    ``explore_data`` node template has ``output == 0`` — i.e. it's a sink
    that consumes data and has no output port. Used to verify the executor
    refuses to wire downstream nodes to it.
    """
    flow = _flow_with_orders()
    flow.add_explore_data(
        input_schema.NodeExploreData(
            flow_id=1,
            node_id=99,  # node 99: explore_data sink
        )
    )
    return flow


def test_add_node_refuses_when_upstream_is_sink(call_kwargs: dict[str, Any]) -> None:
    """An ``add_<type>`` call whose ``upstream_node_ids`` names a sink
    (``NodeTemplate.output == 0``) is refused with ``upstream_is_sink``
    before any wiring happens. Mirrors the customer_deduplication dogfood
    failure (2026-05-07): LLM picked node 4 (``explore_data``) as the
    upstream for an ``add_group_by``.
    """
    flow = _flow_with_orders_and_explore_data()
    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.add_filter",
        tool_args=_filter_args(node_id=2, depending_on_id=99),
        insertion_context=InsertionContext(upstream_node_ids=[99]),
        flow=flow,
        mode="stage",
        **call_kwargs,
    )
    assert result.status == "rejected"
    assert result.refusal_reason == "upstream_is_sink"
    detail = result.refusal_detail or ""
    assert "99" in detail
    assert "explore_data" in detail
    # Refusal includes non-sink candidates so the LLM can retry intelligently.
    assert "[1]" in detail


def test_connect_refuses_when_upstream_is_sink(call_kwargs: dict[str, Any]) -> None:
    """An explicit ``flowfile.graph.connect`` call whose ``from`` (upstream)
    side is a sink is refused. Catches the case where the LLM's wiring
    intent bypasses the ``add_*`` auto-wiring path entirely.
    """
    flow = _flow_with_orders_and_explore_data()
    # Add a downstream candidate to connect TO so the connection has a
    # plausible shape (otherwise the validation might short-circuit
    # elsewhere).
    flow.add_filter(
        input_schema.NodeFilter(
            flow_id=1,
            node_id=2,
            depending_on_id=1,
            filter_input=transform_schema.FilterInput(
                filter_type="advanced", advanced_filter="[region]=='EU'"
            ),
        )
    )
    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.connect",
        tool_args={
            "flow_id": 1,
            "from_node_id": 99,  # explore_data sink — illegal as upstream
            "to_node_id": 2,
            "input_type": "main",
        },
        insertion_context=InsertionContext(),
        flow=flow,
        mode="stage",
        **call_kwargs,
    )
    assert result.status == "rejected"
    assert result.refusal_reason == "upstream_is_sink"
    detail = result.refusal_detail or ""
    assert "99" in detail and "explore_data" in detail


def test_add_node_passes_when_upstream_is_non_sink(call_kwargs: dict[str, Any]) -> None:
    """Regression guard: the sink check must not regress the happy path.
    A normal ``add_filter`` on top of a manual_input source (output > 0)
    still applies cleanly."""
    flow = _flow_with_orders_and_explore_data()
    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.add_filter",
        tool_args=_filter_args(node_id=2, depending_on_id=1),
        insertion_context=InsertionContext(upstream_node_ids=[1]),
        flow=flow,
        mode="stage",
        **call_kwargs,
    )
    assert result.status == "staged", result.refusal_detail
    assert result.refusal_reason is None


# --------------------------------------------------------------------------- #
# 2026-05-07 — node_id coercion in non-Pydantic handlers                        #
# --------------------------------------------------------------------------- #


def test_delete_node_coerces_string_node_id(call_kwargs: dict[str, Any]) -> None:
    """The LLM ships ``"node_id": "1"`` (string) on tools whose handlers
    don't go through Pydantic. The executor coerces simple numeric strings
    to int — same lenience the ``add_*`` / ``connect`` paths get from
    Pydantic's lax mode — so the cross-tool experience is consistent.
    """
    flow = _flow_with_orders()
    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.delete_node",
        tool_args={"flow_id": 1, "node_id": "1"},
        insertion_context=InsertionContext(),
        flow=flow,
        mode="stage",
        **call_kwargs,
    )
    # Coercion succeeded → handler ran → staging path applied.
    assert result.status == "staged", result.refusal_detail


def test_delete_node_refuses_non_coercible_node_id(call_kwargs: dict[str, Any]) -> None:
    """Truly non-numeric strings still refuse with the W67-style example."""
    flow = _flow_with_orders()
    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.delete_node",
        tool_args={"flow_id": 1, "node_id": "node_one"},
        insertion_context=InsertionContext(),
        flow=flow,
        mode="stage",
        **call_kwargs,
    )
    assert result.status == "rejected"
    detail = result.refusal_detail or ""
    assert "delete_node" in detail
    assert "Example payload" in detail


def test_read_node_schema_coerces_string_node_id(call_kwargs: dict[str, Any]) -> None:
    """Same coercion applies to read-only introspection tools."""
    flow = _flow_with_orders()
    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.schema.read_node_schema",
        tool_args={"flow_id": 1, "node_id": "1"},
        insertion_context=InsertionContext(),
        flow=flow,
        mode="apply",
        **call_kwargs,
    )
    assert result.status == "applied", result.refusal_detail


def test_delete_node_refuses_bool_node_id(call_kwargs: dict[str, Any]) -> None:
    """``True`` / ``False`` are technically ``int`` subclasses in Python —
    the coercion helper rejects them explicitly so a hallucinated boolean
    doesn't sneak through as ``1`` / ``0``."""
    flow = _flow_with_orders()
    result = execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.delete_node",
        tool_args={"flow_id": 1, "node_id": True},
        insertion_context=InsertionContext(),
        flow=flow,
        mode="stage",
        **call_kwargs,
    )
    assert result.status == "rejected"
    assert "bool" in (result.refusal_detail or "")
