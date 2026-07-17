"""Route-level tests for GET /flow/node_available_artifacts.

Exercises ``get_node_available_artifacts`` (routes.py) end to end, in-process
(no Docker / kernel): node lookup, the kernel-resolution ladder (query param →
``setting_input.kernel_id`` → ``python_script_input.kernel_id`` → the
registry manifest's ``default_kernel_id``) and the declared-publishes merge
(upstream user-defined kernel nodes' manifest ``publishes`` unioned with
run-observed artifacts). The pure merge helper is unit-tested separately in
``test_artifact_context``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from flowfile_core import flow_file_handler
from flowfile_core.configs import node_store
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.node_designer.state import (
    ArtifactDecl,
    EnvironmentState,
    NodeManifest,
)
from flowfile_core.flowfile.user_defined.registry import LoadedNode
from flowfile_core.flowfile.user_defined.registry import registry as user_defined_registry
from flowfile_core.routes.routes import get_node_available_artifacts
from flowfile_core.schemas import input_schema, schemas


@pytest.fixture
def flow_factory():
    """Build flows and register them in the singleton handler the route reads."""
    created: list[int] = []

    def _make(flow_id: int) -> FlowGraph:
        handler = FlowfileHandler()
        handler.register_flow(
            schemas.FlowSettings(
                flow_id=flow_id,
                name="artifacts_route_test",
                path=".",
                execution_mode="Development",
            )
        )
        flow = handler.get_flow(flow_id)
        flow_file_handler._flows[flow_id] = flow
        created.append(flow_id)
        return flow

    yield _make
    for fid in created:
        flow_file_handler._flows.pop(fid, None)


@pytest.fixture
def register_manifest():
    """Inject synthetic user-defined registry entries + node templates; restore on teardown."""
    saved_entries = dict(user_defined_registry._entries)
    saved_node_dict = dict(node_store.node_dict)

    def _register(
        node_key: str,
        *,
        kind: str = "kernel",
        default_kernel_id: str | None = None,
        publishes: list[tuple[str, str | None]] | None = None,
    ) -> LoadedNode:
        manifest = NodeManifest(
            class_name=node_key.title().replace("_", ""),
            node_name=node_key,
            environment=EnvironmentState(kind=kind, default_kernel_id=default_kernel_id),
            publishes=[ArtifactDecl(name=name, type=type_) for name, type_ in (publishes or [])],
        )
        entry = LoadedNode(
            node_key=node_key,
            file_path=Path(f"/virtual/{node_key}.py"),
            file_name=f"{node_key}.py",
            source_text="",
            source_hash="deadbeef",
            mtime=0.0,
            manifest=manifest,
        )
        user_defined_registry._entries[str(entry.file_path)] = entry
        # A template must exist in node_dict for the placeholder FlowNode to build.
        node_store.register_missing_node_template(node_key)
        return entry

    yield _register
    user_defined_registry._entries.clear()
    user_defined_registry._entries.update(saved_entries)
    node_store.node_dict.clear()
    node_store.node_dict.update(saved_node_dict)


# ------------------------------------------------------------------
# graph-building helpers
# ------------------------------------------------------------------


def _add_manual_input(flow: FlowGraph, node_id: int) -> None:
    flow.add_node_promise(input_schema.NodePromise(flow_id=flow.flow_id, node_id=node_id, node_type="manual_input"))
    flow.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow.flow_id,
            node_id=node_id,
            raw_data_format=input_schema.RawData.from_pylist([{"a": 1}, {"a": 2}]),
        )
    )


def _add_python_script(
    flow: FlowGraph, node_id: int, *, kernel_id: str | None = None, upstream_ids: tuple[int, ...] = ()
) -> None:
    flow.add_node_promise(input_schema.NodePromise(flow_id=flow.flow_id, node_id=node_id, node_type="python_script"))
    flow.add_python_script(
        input_schema.NodePythonScript(
            flow_id=flow.flow_id,
            node_id=node_id,
            depending_on_ids=list(upstream_ids),
            python_script_input=input_schema.PythonScriptInput(kernel_id=kernel_id),
        )
    )
    for uid in upstream_ids:
        add_connection(flow, input_schema.NodeConnection.create_from_simple_input(uid, node_id))


def _add_custom_promise(flow: FlowGraph, node_id: int, node_type: str, *, upstream_ids: tuple[int, ...] = ()) -> None:
    """A custom node placed as a bare placeholder (route reads node_type + setting_input only)."""
    flow.add_node_promise(input_schema.NodePromise(flow_id=flow.flow_id, node_id=node_id, node_type=node_type))
    for uid in upstream_ids:
        add_connection(flow, input_schema.NodeConnection.create_from_simple_input(uid, node_id))


# ------------------------------------------------------------------
# 1. Ladder: python_script_input.kernel_id resolves, observed returned
# ------------------------------------------------------------------


def test_kernel_from_python_script_input_returns_observed(flow_factory):
    flow = flow_factory(9101)
    _add_manual_input(flow, 1)
    _add_python_script(flow, 2, upstream_ids=(1,))  # publisher (no explicit kernel)
    _add_python_script(flow, 3, kernel_id="k1", upstream_ids=(2,))  # target
    flow.artifact_context.record_published(
        2, "k1", [{"name": "scaler", "type_name": "StandardScaler", "module": "sklearn.preprocessing"}]
    )

    resp = get_node_available_artifacts(flow_id=9101, node_id=3, kernel_id=None)

    assert len(resp["artifacts"]) == 1
    art = resp["artifacts"][0]
    assert art["name"] == "scaler"
    assert art["status"] == "published"
    assert art["type_name"] == "StandardScaler"
    assert art["source_node_id"] == 2
    assert art["kernel_id"] == "k1"


# ------------------------------------------------------------------
# 2. Ladder: manifest default_kernel_id fallback resolves the kernel
# ------------------------------------------------------------------


def test_kernel_from_manifest_default_fallback(flow_factory, register_manifest):
    register_manifest("udf_target", kind="kernel", default_kernel_id="kdata")
    flow = flow_factory(9102)
    _add_manual_input(flow, 1)
    _add_python_script(flow, 2, upstream_ids=(1,))  # publisher
    _add_custom_promise(flow, 3, "udf_target", upstream_ids=(2,))  # target, no explicit binding
    flow.artifact_context.record_published(2, "kdata", [{"name": "prep", "type_name": "dict"}])

    resp = get_node_available_artifacts(flow_id=9102, node_id=3, kernel_id=None)

    names = {a["name"]: a for a in resp["artifacts"]}
    assert set(names) == {"prep"}
    assert names["prep"]["status"] == "published"
    assert names["prep"]["kernel_id"] == "kdata"


# ------------------------------------------------------------------
# 3. Declared merge: upstream manifest publishes, nothing observed
# ------------------------------------------------------------------


def test_declared_publishes_merged_when_no_run(flow_factory, register_manifest):
    register_manifest(
        "xgb_train",
        kind="kernel",
        default_kernel_id="k1",
        publishes=[("model", "xgboost.core.Booster"), ("metrics", None)],
    )
    flow = flow_factory(9103)
    _add_manual_input(flow, 1)
    _add_custom_promise(flow, 2, "xgb_train", upstream_ids=(1,))
    _add_python_script(flow, 3, upstream_ids=(2,))  # target, kernel via query param

    resp = get_node_available_artifacts(flow_id=9103, node_id=3, kernel_id="k1")

    by_name = {a["name"]: a for a in resp["artifacts"]}
    assert set(by_name) == {"model", "metrics"}

    model = by_name["model"]
    assert model["status"] == "declared"
    assert model["type_name"] == "Booster"
    assert model["module"] == "xgboost.core"
    assert model["created_at"] is None
    assert model["size_bytes"] == 0
    assert model["source_node_id"] == 2

    metrics = by_name["metrics"]
    assert metrics["status"] == "declared"
    assert metrics["type_name"] == ""
    assert metrics["module"] == ""
    assert metrics["created_at"] is None


# ------------------------------------------------------------------
# 4. Observed wins on name conflict with a declared publish
# ------------------------------------------------------------------


def test_observed_wins_over_declared(flow_factory, register_manifest):
    register_manifest(
        "xgb_train",
        kind="kernel",
        default_kernel_id="k1",
        publishes=[("model", "xgboost.core.Booster"), ("metrics", None)],
    )
    flow = flow_factory(9104)
    _add_manual_input(flow, 1)
    _add_custom_promise(flow, 2, "xgb_train", upstream_ids=(1,))
    _add_python_script(flow, 3, upstream_ids=(2,))
    flow.artifact_context.record_published(
        2, "k1", [{"name": "model", "type_name": "ObservedBooster", "module": "xgboost.core"}]
    )

    resp = get_node_available_artifacts(flow_id=9104, node_id=3, kernel_id="k1")

    models = [a for a in resp["artifacts"] if a["name"] == "model"]
    assert len(models) == 1
    assert models[0]["status"] == "published"
    assert models[0]["type_name"] == "ObservedBooster"
    assert models[0]["created_at"] is not None
    # the un-observed declared publish still rides along
    assert {a["name"] for a in resp["artifacts"]} == {"model", "metrics"}


# ------------------------------------------------------------------
# 5. Upstream bound to a different kernel: its declared publishes excluded
# ------------------------------------------------------------------


def test_upstream_on_different_kernel_excluded(flow_factory, register_manifest):
    register_manifest(
        "xgb_train_k2",
        kind="kernel",
        default_kernel_id="k2",
        publishes=[("model", "xgboost.core.Booster")],
    )
    flow = flow_factory(9105)
    _add_manual_input(flow, 1)
    _add_custom_promise(flow, 2, "xgb_train_k2", upstream_ids=(1,))
    _add_python_script(flow, 3, upstream_ids=(2,))

    resp = get_node_available_artifacts(flow_id=9105, node_id=3, kernel_id="k1")

    assert resp == {"artifacts": []}


# ------------------------------------------------------------------
# 6. Unresolvable kernel: no param, no bindings, no manifest → empty
# ------------------------------------------------------------------


def test_unresolvable_kernel_returns_empty(flow_factory):
    flow = flow_factory(9106)
    _add_manual_input(flow, 1)
    _add_python_script(flow, 2, upstream_ids=(1,))  # no kernel binding

    resp = get_node_available_artifacts(flow_id=9106, node_id=2, kernel_id=None)

    assert resp == {"artifacts": []}
