"""Tests for one-shot local-model flow generation.

Covers JSON extraction, the topological insertion planner, and end-to-end
staging against a real (empty) ``FlowGraph`` — no network, no model.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from flowfile_core.ai import diff
from flowfile_core.ai.local_model import oneshot
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.schemas import schemas


@pytest.fixture(autouse=True)
def _reset_diff_store() -> Iterator[None]:
    diff.clear_for_tests()
    yield
    diff.clear_for_tests()


def _empty_flow(flow_id: int = 1) -> FlowGraph:
    return FlowGraph(
        flow_settings=schemas.FlowSettings(
            flow_id=flow_id,
            execution_mode="Performance",
            execution_location="local",
            path="/tmp/test_oneshot",
        ),
        name="oneshot_test",
    )


# extract_flow_json                                                           #


def test_extract_flow_json_direct():
    spec = oneshot.extract_flow_json('{"nodes":[{"id":"n1","type":"read"}],"edges":[]}')
    assert spec["nodes"][0]["type"] == "read"


def test_extract_flow_json_fenced():
    text = 'Here you go:\n```json\n{"nodes":[{"id":"a","type":"filter"}],"edges":[]}\n```\nDone.'
    spec = oneshot.extract_flow_json(text)
    assert spec["nodes"][0]["type"] == "filter"


def test_extract_flow_json_fenced_yaml():
    text = "```yaml\nnodes:\n  - id: a\n    type: read\nedges: []\n```"
    spec = oneshot.extract_flow_json(text)
    assert spec["nodes"][0]["type"] == "read"


def test_extract_flow_json_prose_wrapped_braces():
    # No fence, object embedded in prose — brace-span scan peels it out.
    text = 'Sure, here it is: {"nodes":[{"id":"a","type":"read"}],"edges":[]} — enjoy!'
    spec = oneshot.extract_flow_json(text)
    assert spec["nodes"][0]["type"] == "read"


def test_extract_flow_json_raw_yaml():
    text = "nodes:\n  - {id: a, type: read}\nedges: []"
    spec = oneshot.extract_flow_json(text)
    assert spec["nodes"][0]["type"] == "read"


def test_extract_flow_json_strips_think_block():
    # A thinking model may still emit <think>…</think> (the server runs with
    # thinking off, but not every template honours it); its prose can carry
    # braces that would otherwise win the widest-brace scan.
    text = (
        "<think>\nThe user wants {something}. I will output {\"nodes\": ...}\n</think>\n"
        '{"nodes":[{"id":"a","type":"read"}],"edges":[]}'
    )
    spec = oneshot.extract_flow_json(text)
    assert spec["nodes"][0]["type"] == "read"
    assert spec["edges"] == []


def test_extract_flow_json_only_think_block_is_empty():
    with pytest.raises(oneshot.OneShotError):
        oneshot.extract_flow_json("<think>nothing useful</think>")


# extract_answer                                                              #


def test_extract_answer_reads_the_escape_hatch_object():
    assert oneshot.extract_answer('{"answer": "I build flows from a description."}') == (
        "I build flows from a description."
    )
    assert oneshot.extract_answer('Sure:\n```json\n{"answer": "Hi!"}\n```') == "Hi!"
    assert oneshot.extract_answer('<think>hmm</think>{"answer": "Hi!"}') == "Hi!"


def test_extract_answer_treats_plain_prose_as_the_answer():
    prose = "I'm Flowfile's local flow generator. Describe a pipeline and I'll build it."
    assert oneshot.extract_answer(prose) == prose


def test_extract_answer_is_none_for_flow_attempts():
    assert oneshot.extract_answer('{"nodes":[{"id":"a","type":"read"}],"edges":[]}') is None
    # A brace span that is neither a flow nor an answer stays a parse error.
    assert oneshot.extract_answer('{"foo": "bar"}') is None
    assert oneshot.extract_answer("here is a broken one: {nodes: [") is None
    assert oneshot.extract_answer('{"answer": ""}') is None
    assert oneshot.extract_answer("") is None


def test_extract_flow_json_rejects_garbage():
    with pytest.raises(oneshot.OneShotError):
        oneshot.extract_flow_json("sorry, I cannot help with that")


def test_extract_flow_json_rejects_empty():
    with pytest.raises(oneshot.OneShotError):
        oneshot.extract_flow_json("")


# _plan_insertions                                                            #


def test_plan_insertions_linear_toposort_and_ids():
    spec = {
        "nodes": [
            {"id": "c", "type": "sort", "settings": {}},
            {"id": "a", "type": "read", "settings": {}},
            {"id": "b", "type": "filter", "settings": {}},
        ],
        "edges": [{"source": "a", "target": "b"}, {"source": "b", "target": "c"}],
    }
    by_type = {p.node_type: p for p in oneshot._plan_insertions(spec, start_id=5)}
    assert by_type["read"].node_id == 5
    assert by_type["filter"].node_id == 6
    assert by_type["sort"].node_id == 7
    assert by_type["read"].upstream_ids == []
    assert by_type["filter"].upstream_ids == [5]
    assert by_type["sort"].upstream_ids == [6]
    assert by_type["filter"].pos_x > by_type["read"].pos_x


def test_plan_insertions_join_splits_right_input():
    spec = {
        "nodes": [
            {"id": "l", "type": "read", "settings": {}},
            {"id": "r", "type": "read", "settings": {}},
            {"id": "j", "type": "join", "settings": {}},
        ],
        "edges": [{"source": "l", "target": "j"}, {"source": "r", "target": "j"}],
    }
    j = next(p for p in oneshot._plan_insertions(spec, start_id=1) if p.node_type == "join")
    assert len(j.upstream_ids) == 1
    assert j.right_input_id is not None
    assert j.right_input_id != j.upstream_ids[0]


# _stage_flow (integration against a real flow)                               #


def test_stage_flow_builds_diff():
    flow = _empty_flow()
    spec = {
        "nodes": [
            {
                "id": "a",
                "type": "manual_input",
                "settings": {
                    "raw_data_format": {
                        "columns": [
                            {"name": "status", "data_type": "String"},
                            {"name": "amount", "data_type": "Int64"},
                        ],
                        "data": [["paid", "open"], [100, 50]],
                    }
                },
            },
            {
                "id": "b",
                "type": "filter",
                "settings": {"filter_input": {"mode": "advanced", "advanced_filter": "[status] = 'paid'"}},
            },
            {"id": "c", "type": "sort", "settings": {"sort_input": [{"column": "amount", "how": "desc"}]}},
        ],
        "edges": [{"source": "a", "target": "b"}, {"source": "b", "target": "c"}],
    }
    result = oneshot._stage_flow(flow=flow, flow_id=1, user_id=1, spec=spec)
    assert result["op_count"] == 3, result["warnings"]
    graph_diff = diff.get_diff(result["diff_id"])
    assert graph_diff is not None
    assert [a.node_type for a in graph_diff.additions] == ["manual_input", "filter", "sort"]
    # The full diff payload rides along so the frontend can drive its
    # existing diff-review panel without a separate GET.
    payload = result["diff_payload"]
    assert payload["diff_id"] == result["diff_id"]
    assert payload["flow_id"] == 1
    assert [a["node_type"] for a in payload["additions"]] == ["manual_input", "filter", "sort"]


def test_stage_flow_skips_writer_nodes():
    flow = _empty_flow()
    spec = {
        "nodes": [
            {
                "id": "a",
                "type": "manual_input",
                "settings": {
                    "raw_data_format": {"columns": [{"name": "x", "data_type": "Int64"}], "data": [[1, 2, 3]]}
                },
            },
            {
                "id": "z",
                "type": "output",
                "settings": {
                    "output_settings": {
                        "name": "out.csv",
                        "directory": ".",
                        "file_type": "csv",
                        "write_mode": "overwrite",
                        "table_settings": {"file_type": "csv"},
                    }
                },
            },
        ],
        "edges": [{"source": "a", "target": "z"}],
    }
    result = oneshot._stage_flow(flow=flow, flow_id=1, user_id=1, spec=spec)
    created_types = [c["type"] for c in result["created"]]
    assert "manual_input" in created_types
    assert "output" not in created_types
    assert any("output" in w for w in result["warnings"])


# _build_simple_diff (light path — no executor / no validation at build)      #


def test_build_simple_diff_applies_to_real_flow():
    flow = _empty_flow()
    spec = {
        "nodes": [
            {
                "id": "a",
                "type": "manual_input",
                "settings": {
                    "raw_data_format": {"columns": [{"name": "x", "data_type": "Int64"}], "data": [[1, 2, 3]]}
                },
            },
            {
                "id": "b",
                "type": "filter",
                "settings": {"filter_input": {"mode": "advanced", "advanced_filter": "[x] > 1"}},
            },
        ],
        "edges": [{"source": "a", "target": "b"}],
    }
    result = oneshot._build_simple_diff(flow=flow, flow_id=1, spec=spec)
    assert result["op_count"] == 2, result["warnings"]
    assert result["rationale"] == "Generated flow (simple mode)"
    graph_diff = diff.get_diff(result["diff_id"])
    assert graph_diff is not None
    # Wiring rides on insertion_context (no separate connection ops).
    assert graph_diff.connections_added == []
    assert graph_diff.additions[1].insertion_context.upstream_node_ids == [
        graph_diff.additions[0].settings["node_id"]
    ]
    # The diff applies cleanly onto a real FlowGraph (no kernel/dry-run).
    applied = diff.apply_diff(flow, graph_diff)
    assert sorted(applied.applied_node_ids) == sorted(int(n.node_id) for n in flow.nodes)
    assert len(flow.nodes) == 2


def test_build_simple_diff_drops_writers_and_unknowns():
    flow = _empty_flow()
    spec = {
        "nodes": [
            {
                "id": "a",
                "type": "manual_input",
                "settings": {
                    "raw_data_format": {"columns": [{"name": "x", "data_type": "Int64"}], "data": [[1]]}
                },
            },
            {"id": "z", "type": "output", "settings": {}},
            {"id": "q", "type": "not_a_real_node", "settings": {}},
        ],
        "edges": [{"source": "a", "target": "z"}, {"source": "a", "target": "q"}],
    }
    result = oneshot._build_simple_diff(flow=flow, flow_id=1, spec=spec)
    created = [c["type"] for c in result["created"]]
    assert created == ["manual_input"]
    assert any("output" in w for w in result["warnings"])
    assert any("not_a_real_node" in w for w in result["warnings"])


def test_build_simple_diff_no_usable_nodes_raises():
    flow = _empty_flow()
    spec = {"nodes": [{"id": "z", "type": "output", "settings": {}}], "edges": []}
    with pytest.raises(oneshot.OneShotError):
        oneshot._build_simple_diff(flow=flow, flow_id=1, spec=spec)


# generate_flow end-to-end (stub provider — no network, no model)             #


def test_generate_flow_simple_with_stub_provider():
    """``generate_flow(mode="simple")`` parses a provider's JSON response and
    registers a GraphDiff — exercised with a duck-typed provider so no model or
    network is involved."""
    import asyncio

    flow = _empty_flow()

    class _Resp:
        content = (
            '{"nodes": [{"id": "a", "type": "manual_input", "settings": '
            '{"raw_data_format": {"columns": [{"name": "x", "data_type": "Int64"}], '
            '"data": [[1, 2]]}}}], "edges": []}'
        )

    class _StubProvider:
        async def chat(self, messages, **kwargs):  # type: ignore[no-untyped-def]
            return _Resp()

    result = asyncio.run(
        oneshot.generate_flow(
            provider=_StubProvider(),
            flow=flow,
            flow_id=1,
            user_id=1,
            user_request="make a one-row table",
            mode="simple",
        )
    )
    assert result["op_count"] == 1
    assert result["created"][0]["type"] == "manual_input"
    assert result["rationale"] == "Generated flow (simple mode)"
    graph_diff = diff.get_diff(result["diff_id"])
    assert graph_diff is not None
    assert [a.node_type for a in graph_diff.additions] == ["manual_input"]


def _run_generate(content: str) -> dict:
    import asyncio

    flow = _empty_flow()

    class _Resp:
        pass

    _Resp.content = content

    class _StubProvider:
        async def chat(self, messages, **kwargs):  # type: ignore[no-untyped-def]
            return _Resp()

    return asyncio.run(
        oneshot.generate_flow(
            provider=_StubProvider(),
            flow=flow,
            flow_id=1,
            user_id=1,
            user_request="what is this flow?",
            mode="simple",
        )
    )


def test_generate_flow_returns_the_models_answer_instead_of_failing():
    """A question in Simple build gets the model's reply back as ``answer``
    (nothing staged) rather than a "could not parse" 422."""
    result = _run_generate('{"answer": "I build flows from a description; ask in Chat mode about this one."}')
    assert result["answer"].startswith("I build flows")
    assert result["diff_id"] is None
    assert result["op_count"] == 0
    assert result["created"] == []


def test_generate_flow_prose_reply_becomes_the_answer():
    result = _run_generate("I'm Flowfile's local flow generator. Describe a pipeline to build.")
    assert result["answer"].startswith("I'm Flowfile's")
    assert result["diff_id"] is None


def test_generate_flow_broken_json_still_raises():
    with pytest.raises(oneshot.OneShotError):
        _run_generate('{"foo": "bar"}')
