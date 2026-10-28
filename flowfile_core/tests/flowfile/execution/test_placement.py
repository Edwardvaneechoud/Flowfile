"""A run whose node needs what its backend does not declare is refused before any node starts.

With the compute protocol flag on, ``run_graph`` places the nodes it will run, each on the run's backend, the model
nodes included, and compares what each needs with the ``Capabilities`` that backend declares; a model that crosses the
artifact store needs the store's scheme, and a Run Flow node is refused for what a node the flow it runs would run
needs, while one whose flow cannot be built fails when it runs. A node's settings are read with its flow's parameters
in them, so a path on a parameter is placed by where it points; a node that refers to a parameter its flow does not
define is not refused but fails when it runs, and one whose settings cannot be checked with the parameters in them is
refused with the error. A refused node is a failed row on the run report
naming the backend and the reason; no node runs and the backend is sent no work. A backend that cannot be asked for
its capabilities is not in the map placement is given: its nodes are not refused and meet its typed error when they
run. The backends here are core's own under another id, declaring what each test hands them, so no worker is needed.
With the flag off nothing is placed. A backend that reaches storage with its own identity (``credential_resolution``
``ambient``) is refused a cloud or catalog node for each stored connection it names of any method but ``env_vars`` and
``managed_identity``, which a plan leaves to the executor's environment.

A row's ``compute_error`` says what kind of failure it is: ``"Refusal"`` on a refused row, the class name of the typed
compute error that failed a node on that node's row, in both flag states, and ``None`` for any other failure.
"""

import importlib.metadata
import threading
import uuid
from types import SimpleNamespace
from pathlib import Path

import polars as pl
import pytest
from pydantic import SecretStr
from sqlalchemy.exc import OperationalError

from flowfile_core.artifacts import reset_storage_backend
from flowfile_core.configs import settings
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager import db_connections
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.embedded import EmbeddedWorkerBackend
from flowfile_core.flowfile.execution.errors import ComputeUnavailable, VersionMismatch
from flowfile_core.flowfile.execution.placement import (
    MixedPlacementPolicy,
    Placement,
    Refusal,
    SingleBackendPolicy,
    _catalog_target,
    place,
    remedy_for,
)
from flowfile_core.flowfile.execution.protocol import OperationKind
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.flow_node.flow_node import schema_prefetch_blocked
from flowfile_core.flowfile.flow_node.requirements import requirements_for
from flowfile_core.schemas import input_schema, schemas, transform_schema
from flowfile_core.schemas.cloud_storage_schemas import CloudStorageReadSettings, FullCloudStorageConnection
from shared.compute_protocol import Capabilities
from tests import test_run_flow_node
from tests.conftest import protocol_states
from tests.flowfile.conftest import add_test_catalog_writer, create_test_namespace
from tests.flowfile.execution.conftest import serve
from tests.flowfile.execution.graph_helpers import (
    add_filter_on,
    add_manual_input,
    add_promise,
    code_flow,
    connect,
    make_graph,
)
from tests.test_catalog_cloud_virtual import _create_cloud_schema, _ensure_connection

flag = protocol_states()

BACKEND_ID = "declaring"
FLOW_REMEDY = "To run it elsewhere, set the flow's Execution location under Flow Settings."
FLOW_STORE_REMEDY = (
    "To run it elsewhere, set the flow's Execution location under Flow Settings, or configure an exchange store "
    '(Settings → Execution → Backends), which moves a node its backend cannot run to the default backend "local" on '
    "its own."
)
OWN_REMEDY = "To run it elsewhere, set its Compute backend under General Settings."
AUTO_REMEDY = (
    "To run it elsewhere, set its Compute backend under General Settings, "
    "or the flow's Execution location under Flow Settings."
)
OWN_WITHOUT_STORE_REMEDY = (
    "To run it elsewhere, set the flow's Execution location under Flow Settings; "
    "a node's own Compute backend applies only when an exchange store is configured (Settings → Execution → Backends)."
)
LOCAL_DISK = 'namespace "TestCat.TestSch" is on local disk, not object storage'


def everything() -> Capabilities:
    """What the worker embedded in this process declares: the nineteen operations, ``file`` and object storage,
    every plugin."""
    return EmbeddedWorkerBackend().capabilities()


def ambient() -> Capabilities:
    """``everything()`` on a compute that reaches storage with its own identity, as a cluster does."""
    return everything().model_copy(update={"credential_resolution": "ambient"})


def without(capabilities: Capabilities, *, operation=None, scheme=None, plugin=None) -> Capabilities:
    """``capabilities`` less one operation, one scheme from both scheme sets, or one plugin."""
    update = {}
    if operation is not None:
        update["operations"] = capabilities.operations - {operation}
    if scheme is not None:
        update["read_schemes"] = capabilities.read_schemes - {scheme}
        update["write_schemes"] = capabilities.write_schemes - {scheme}
    if plugin is not None:
        update["plugins"] = {name: version for name, version in capabilities.plugins.items() if name != plugin}
    return capabilities.model_copy(update=update)


class DeclaringBackend(EmbeddedWorkerBackend):
    """The worker embedded in this process under another id, declaring the capabilities it is given.

    Given an error instead, it is unreachable: ``capabilities()``, ``submit`` and ``run_op`` raise it. ``work`` records
    each ``submit`` and ``run_op`` it is sent but the handshake's probe jobs; ``asked`` counts its ``capabilities()``
    calls.
    """

    def __init__(self, backend_id: str, declared: Capabilities | Exception):
        super().__init__()
        self.id = backend_id
        self.declared = declared
        self.asked = 0
        self.work: list[str] = []
        self._work_lock = threading.Lock()

    def capabilities(self) -> Capabilities:
        self.asked += 1
        if isinstance(self.declared, Exception):
            raise self.declared
        return self.declared

    def submit(self, plan, inputs, hints, result_id, *, flow_id, node_id):
        if not result_id.startswith("handshake-"):
            with self._work_lock:
                self.work.append(f"submit {node_id}")
        if isinstance(self.declared, Exception):
            raise self.declared
        return super().submit(plan, inputs, hints, result_id, flow_id=flow_id, node_id=node_id)

    def run_op(self, op, payload):
        with self._work_lock:
            self.work.append(f"run_op {op.value}")
        if isinstance(self.declared, Exception):
            raise self.declared
        return super().run_op(op, payload)


@pytest.fixture
def declare(monkeypatch):
    """``declare(declared, backend_id)`` registers a ``DeclaringBackend`` under the id and returns it.

    ``declaring`` is the default backend, the one a ``"remote"`` flow runs on.
    """
    registry.reset()
    handshake.reset_handshake()
    monkeypatch.setenv("FLOWFILE_COMPUTE_BACKEND", BACKEND_ID)

    def register(declared: Capabilities | Exception, backend_id: str = BACKEND_ID) -> DeclaringBackend:
        backend = DeclaringBackend(backend_id, declared)
        registry.register_backend(backend_id, lambda: backend)
        return backend

    yield register
    registry.reset()
    handshake.reset_handshake()


def fuzzy_flow(flow_id: int) -> FlowGraph:
    """Manual inputs 1 and 2 → fuzzy match 3 on ``name``."""
    graph = make_graph(flow_id)
    add_manual_input(graph, 1, [{"name": "eduward"}, {"name": "edward"}, {"name": "courtney"}])
    add_manual_input(graph, 2, [{"name": "edward"}, {"name": "courtney"}])
    add_promise(graph, 3, "fuzzy_match")
    graph.add_fuzzy_match(
        input_schema.NodeFuzzyMatch.model_validate(
            {
                "flow_id": flow_id,
                "node_id": 3,
                "depending_on_ids": [1, 2],
                "join_input": {
                    "join_mapping": [
                        {"left_col": "name", "right_col": "name", "threshold_score": 75, "fuzzy_type": "levenshtein"}
                    ],
                    "left_select": {"renames": [{"old_name": "name", "new_name": "name", "join_key": True}]},
                    "right_select": {"renames": [{"old_name": "name", "new_name": "name", "join_key": True}]},
                    "how": "inner",
                },
            }
        )
    )
    connect(graph, 1, 3)
    connect(graph, 2, 3, input_type="right")
    return graph


def model_flow(
    flow_id: int, execution_location: str = "local", *, publish: str | None = None, catalog: str | None = None
) -> FlowGraph:
    """Manual input 1 → Train Model 2 → Apply Model 3, scoring with node 2's model into ``pred``.

    With ``publish`` Train Model publishes its model under that name; with ``catalog`` Apply Model scores with the
    catalog model of that name instead.
    """
    graph = make_graph(flow_id, execution_location=execution_location)
    add_manual_input(graph, 1, [{"x": float(i), "y": 2.0 * i} for i in range(10)])
    add_promise(graph, 2, "train_model")
    train_input = input_schema.TrainModelSettings(target_column="y", feature_columns=["x"])
    if publish is not None:
        train_input = train_input.model_copy(update={"publish_to_catalog": True, "model_name": publish})
    graph.add_train_model(
        input_schema.NodeTrainModel(flow_id=flow_id, node_id=2, depending_on_id=1, train_input=train_input)
    )
    connect(graph, 1, 2)
    add_promise(graph, 3, "apply_model")
    apply_input = input_schema.ApplyModelSettings(source="upstream", upstream_node_id=2, output_column="pred")
    if catalog is not None:
        apply_input = apply_input.model_copy(update={"source": "catalog", "model_name": catalog})
    graph.add_apply_model(
        input_schema.NodeApplyModel(flow_id=flow_id, node_id=3, depending_on_id=2, apply_input=apply_input)
    )
    connect(graph, 2, 3)
    return graph


def run_flow_graph(
    flow_id: int,
    registration_id: int,
    bindings: list[input_schema.RunFlowParameterBinding] | None = None,
    parameters: list[schemas.FlowParameter] | None = None,
) -> FlowGraph:
    """Manual input 1 → Run Flow node 2 over the registered flow, no slots, its parameters bound by ``bindings``.

    ``parameters`` are the flow's own, which a constant binding ``${name}`` forwards.
    """
    graph = make_graph(flow_id)
    graph.flow_settings.parameters = parameters or []
    add_manual_input(graph, 1, [{"id": 1}])
    add_promise(graph, 2, "run_flow")
    graph.add_run_flow(
        input_schema.NodeRunFlow(
            flow_id=graph.flow_id,
            node_id=2,
            user_id=1,
            flow_reference=input_schema.SubflowReference(registration_id=registration_id),
            input_slots=[],
            output_slots=[],
            parameter_bindings=bindings or [],
        )
    )
    connect(graph, 1, 2)
    return graph


def add_python_script_on_kernel(graph: FlowGraph, node_id: int, depending_on_id: int) -> None:
    add_promise(graph, node_id, "python_script")
    graph.add_python_script(
        input_schema.NodePythonScript(
            flow_id=graph.flow_id,
            node_id=node_id,
            depending_on_ids=[depending_on_id],
            python_script_input=input_schema.PythonScriptInput(code="print(1)", kernel_id="k"),
        )
    )
    connect(graph, depending_on_id, node_id)


def register_gated_child(
    tmp_path: Path,
    flow_id: int,
    name: str,
    saved: str,
    operator: str,
    value: str,
    *,
    parameter: str = "mode",
    parameter_type: str = "string",
) -> int:
    """Manual input 1 → gate 2 on ``<parameter> <operator> <value>`` → Python Script 3 on kernel ``k``, as ``name``.

    ``parameter`` is the flow's parameter of ``parameter_type``, saved as ``saved``; answers the registration id.
    """
    child = make_graph(flow_id)
    child.flow_settings.parameters = [schemas.FlowParameter(name=parameter, default_value=saved, type=parameter_type)]
    add_manual_input(child, 1, [{"id": 1}])
    add_promise(child, 2, "gate")
    child.add_gate(
        input_schema.NodeGate(
            flow_id=child.flow_id,
            node_id=2,
            depending_on_id=1,
            gate_input=transform_schema.GateInput(parameter=parameter, operator=operator, value=value),
        )
    )
    connect(child, 1, 2)
    add_python_script_on_kernel(child, 3, depending_on_id=2)
    path = tmp_path / f"{name}.yaml"
    child.save_flow(str(path))
    child.close_flow()
    return test_run_flow_node.register_flow_file(path, name)


def assert_refused_before_any_node(
    graph: FlowGraph, run_info, backend: DeclaringBackend, refused: dict[int, list[str]]
):
    """The run failed with exactly ``refused`` (node id -> phrases its error holds), nothing ran or holds the run, and
    its flow log holds the refusal and no completion."""
    errors = {result.node_id: result.error for result in run_info.node_step_result}
    assert errors.keys() == refused.keys(), errors
    for node_id, phrases in refused.items():
        assert errors[node_id].startswith(f'cannot run on backend "{backend.id}": '), errors[node_id]
        for phrase in phrases:
            assert phrase in errors[node_id], errors[node_id]
    assert all(result.success is False for result in run_info.node_step_result)
    assert all(result.compute_error == "Refusal" for result in run_info.node_step_result)
    assert run_info.success is False
    assert run_info.nodes_completed == 0
    assert run_info.is_running is False
    assert graph.flow_settings.is_running is False
    assert backend.work == []
    flow_log = Path(graph.flow_logger.log_file_path).read_text()
    assert "no node was started" in flow_log, flow_log
    assert "Flow completed!" not in flow_log, flow_log


def test_a_catalog_write_to_a_local_disk_namespace_is_refused_on_a_backend_without_local_files(
    protocol_on, declare, catalog_clean_state
):
    backend = declare(without(everything(), scheme="file"))
    graph = make_graph(8401)
    add_manual_input(graph, 1, [{"id": 1, "name": "ada"}])
    add_test_catalog_writer(graph, node_id=2, depending_on_id=1, table_name="dim", namespace_id=create_test_namespace())

    run_info = graph.run_graph()

    assert_refused_before_any_node(graph, run_info, backend, {2: [LOCAL_DISK]})


def test_a_fuzzy_match_is_refused_on_a_backend_without_the_fuzzy_join_operation(protocol_on, declare):
    backend = declare(without(everything(), operation="fuzzy_join"))
    graph = fuzzy_flow(8402)

    run_info = graph.run_graph()

    assert_refused_before_any_node(graph, run_info, backend, {3: ["it needs the fuzzy_join operation"]})


@pytest.mark.parametrize(
    "flow_id, node_type, code",
    [(8452, "polars_code", "output_df = input_df"), (8453, "sql_query", "SELECT * FROM input_1")],
    ids=["polars_code", "sql_query"],
)
def test_a_user_code_node_is_refused_on_a_backend_without_its_operation(protocol_on, declare, flow_id, node_type, code):
    backend = declare(without(everything(), operation=node_type))
    graph = code_flow(flow_id, node_type, code, [{"id": 1, "name": "ada"}])

    run_info = graph.run_graph()

    assert_refused_before_any_node(graph, run_info, backend, {2: [f"it needs the {node_type} operation"]})


def test_a_fuzzy_match_is_refused_on_a_backend_without_its_plugin(protocol_on, declare):
    backend = declare(without(everything(), plugin="polars_simed"))
    graph = fuzzy_flow(8403)

    run_info = graph.run_graph()

    assert_refused_before_any_node(
        graph, run_info, backend, {3: ["it needs polars_simed (not installed)"]}
    )


def test_each_formula_node_is_refused_on_a_backend_without_the_formula_library_and_a_select_still_runs(
    declare, flag
):
    """Every formula-family node needs the formula library, whatever its formulas compile to, so a backend without it
    refuses each of them and places a select; with the flag off nothing is placed."""
    assert everything().plugins["polars_expr_transformer"] == importlib.metadata.version("polars_expr_transformer")
    backend = declare(without(everything(), plugin="polars_expr_transformer"))
    graph = make_graph(8470 + flag)
    add_manual_input(graph, 1, [{"a": 1}, {"a": 2}])
    add_promise(graph, 2, "formula")
    graph.add_formula(
        input_schema.NodeFormula(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            function=transform_schema.FunctionInput(field=transform_schema.FieldInput(name="b"), function="[a] + 1"),
        )
    )
    connect(graph, 1, 2)
    add_promise(graph, 3, "multi_field_formula")
    graph.add_multi_field_formula(
        input_schema.NodeMultiFieldFormula(
            flow_id=graph.flow_id,
            node_id=3,
            depending_on_id=1,
            multi_field_formula_input=transform_schema.MultiFieldFormulaInput(
                formula="[_CurrentField_] + 1", selection_mode="list", selected_columns=["a"]
            ),
        )
    )
    connect(graph, 1, 3)
    add_filter_on(graph, 4, 1, "[a] > 1")
    add_promise(graph, 5, "gate")
    graph.add_gate(
        input_schema.NodeGate(
            flow_id=graph.flow_id,
            node_id=5,
            depending_on_id=1,
            gate_input=transform_schema.GateInput(condition_source="formula", formula="[a] > 1"),
        )
    )
    connect(graph, 1, 5)
    add_promise(graph, 6, "dynamic_rename")
    graph.add_dynamic_rename(
        input_schema.NodeDynamicRename(
            flow_id=graph.flow_id,
            node_id=6,
            depending_on_id=1,
            dynamic_rename_input=transform_schema.DynamicRenameInput(
                rename_mode="formula", formula="uppercase([column_name])"
            ),
        )
    )
    connect(graph, 1, 6)
    add_promise(graph, 7, "select")
    graph.add_select(
        input_schema.NodeSelect(
            flow_id=graph.flow_id,
            node_id=7,
            depending_on_id=1,
            select_input=[transform_schema.SelectInput(old_name="a")],
        )
    )
    connect(graph, 1, 7)

    run_info = graph.run_graph()

    if not flag:
        assert run_info.success is True, [result.error for result in run_info.node_step_result]
        assert backend.asked == 0
        assert graph.get_node(2).get_resulting_data().collect()["b"].to_list() == [2, 3]
        return
    assert_refused_before_any_node(
        graph,
        run_info,
        backend,
        {n: ["it needs polars_expr_transformer (not installed)"] for n in range(2, 7)},
    )
    for node_id in range(2, 7):
        graph.delete_node(node_id)
    ran = graph.run_graph()

    assert ran.success is True, [result.error for result in ran.node_step_result]
    assert graph.get_node(7).get_resulting_data().collect().to_dicts() == [{"a": 1}, {"a": 2}]


def test_a_python_script_node_is_refused_on_a_backend_off_cores_host(protocol_on, declare):
    backend = declare(without(everything(), scheme="file"))
    graph = make_graph(8404)
    add_manual_input(graph, 1, [{"id": 1}])
    add_promise(graph, 2, "python_script")
    graph.add_python_script(
        input_schema.NodePythonScript(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_ids=[1],
            python_script_input=input_schema.PythonScriptInput(code="print(1)", kernel_id="k"),
        )
    )
    connect(graph, 1, 2)

    run_info = graph.run_graph()

    assert_refused_before_any_node(
        graph, run_info, backend, {2: ["it runs on a notebook kernel, which reads its inputs from core's disk"]}
    )


def test_a_run_flow_node_is_refused_for_what_a_node_of_its_child_flow_needs(protocol_on, declare, tmp_path):
    backend = declare(without(everything(), scheme="file"))
    child = make_graph(8422)
    add_manual_input(child, 1, [{"id": 1}])
    add_python_script_on_kernel(child, 2, depending_on_id=1)
    path = tmp_path / "kernel_child.yaml"
    child.save_flow(str(path))
    child.close_flow()
    graph = run_flow_graph(8421, test_run_flow_node.register_flow_file(path, "kernel_child"))

    run_info = graph.run_graph()

    assert_refused_before_any_node(
        graph,
        run_info,
        backend,
        {2: ['node 2 of the flow it runs ("kernel_child")', "it runs on a notebook kernel"]},
    )
    assert backend.asked == 1


@pytest.mark.parametrize(
    ("saved_mode", "bindings"),
    [
        ("plain", None),
        (
            "kernel",
            [input_schema.RunFlowParameterBinding(parameter_name="mode", source="constant", constant_value="plain")],
        ),
        (
            "kernel",
            [input_schema.RunFlowParameterBinding(parameter_name="mode", source="column", column_name="id")],
        ),
    ],
    ids=["saved_default", "constant_binding", "column_binding"],
)
def test_a_run_flow_node_is_not_refused_for_a_child_node_a_parameter_gate_closes(
    protocol_on, declare, tmp_path, monkeypatch, saved_mode, bindings
):
    backend = declare(without(everything(), scheme="file"))
    registration_id = register_gated_child(tmp_path, 8423, "gated_child", saved_mode, "equals", "kernel")
    graph = run_flow_graph(8424, registration_id, bindings)
    child_runs = []
    run_graph = FlowGraph.run_graph

    def recording_child_runs(self, **kwargs):
        run_info = run_graph(self, **kwargs)
        if self._subflow_depth:
            child_runs.append(run_info)
        return run_info

    monkeypatch.setattr(FlowGraph, "run_graph", recording_child_runs)

    run_info = graph.run_graph()

    assert run_info.success is True, [result.error for result in run_info.node_step_result]
    assert [result.compute_error for result in run_info.node_step_result] == [None, None]
    [child_run] = child_runs
    assert {result.node_id: result.skipped for result in child_run.node_step_result} == {1: False, 2: False, 3: True}
    assert backend.asked == 1


def test_a_run_flow_node_decides_its_child_s_gates_on_the_parent_parameters_it_forwards(protocol_on, declare, tmp_path):
    backend = declare(without(everything(), scheme="file"))
    registration_id = register_gated_child(tmp_path, 8426, "forward_string_child", "kernel", "not_equals", "plain")
    forward = [input_schema.RunFlowParameterBinding(parameter_name="mode", source="constant", constant_value="${mode}")]
    opening = run_flow_graph(
        8427, registration_id, forward, [schemas.FlowParameter(name="mode", default_value="kernel", type="string")]
    )
    closing = run_flow_graph(
        8428, registration_id, forward, [schemas.FlowParameter(name="mode", default_value="plain", type="string")]
    )

    opened = opening.run_graph()

    assert_refused_before_any_node(
        opening,
        opened,
        backend,
        {2: ['node 3 of the flow it runs ("forward_string_child")', "it runs on a notebook kernel"]},
    )

    closed = closing.run_graph()

    assert closed.success is True, [result.error for result in closed.node_step_result]
    assert [result.compute_error for result in closed.node_step_result] == [None, None]


def test_a_run_flow_node_forwarding_an_integer_parameter_is_refused_for_what_its_child_needs(
    protocol_on, declare, tmp_path, caplog
):
    backend = declare(without(everything(), scheme="file"))
    child = make_graph(8433)
    child.flow_settings.parameters = [schemas.FlowParameter(name="n", default_value="0", type="integer")]
    add_manual_input(child, 1, [{"id": 1}])
    add_python_script_on_kernel(child, 2, depending_on_id=1)
    path = tmp_path / "forward_integer_child.yaml"
    child.save_flow(str(path))
    child.close_flow()
    graph = run_flow_graph(
        8434,
        test_run_flow_node.register_flow_file(path, "forward_integer_child"),
        [input_schema.RunFlowParameterBinding(parameter_name="n", source="constant", constant_value="${n}")],
        [schemas.FlowParameter(name="n", default_value="1", type="integer")],
    )

    run_info = graph.run_graph()

    assert_refused_before_any_node(
        graph,
        run_info,
        backend,
        {2: ['node 2 of the flow it runs ("forward_integer_child")', "it runs on a notebook kernel"]},
    )
    assert [record.getMessage() for record in caplog.records if record.module == "placement"] == []


@pytest.mark.parametrize(
    ("parameter", "parameter_type", "opening"),
    [("mode", "string", "kernel"), ("n", "integer", "0")],
    ids=["string", "integer"],
)
def test_a_run_flow_node_is_not_refused_for_a_nested_flow_s_gate_on_a_parameter_it_binds_from_a_column(
    protocol_on, declare, tmp_path, monkeypatch, parameter, parameter_type, opening
):
    backend = declare(without(everything(), scheme="file"))
    grandchild_id = register_gated_child(
        tmp_path,
        8435,
        "nested_grandchild",
        opening,
        "equals",
        opening,
        parameter=parameter,
        parameter_type=parameter_type,
    )
    forward = [
        input_schema.RunFlowParameterBinding(
            parameter_name=parameter, source="constant", constant_value=f"${{{parameter}}}"
        )
    ]
    child = run_flow_graph(
        8436,
        grandchild_id,
        forward,
        [schemas.FlowParameter(name=parameter, default_value=opening, type=parameter_type)],
    )
    path = tmp_path / "nested_child.yaml"
    child.save_flow(str(path))
    child.close_flow()
    bound = [input_schema.RunFlowParameterBinding(parameter_name=parameter, source="column", column_name="id")]
    graph = run_flow_graph(8437, test_run_flow_node.register_flow_file(path, "nested_child"), bound)
    grandchild_runs = []
    run_graph = FlowGraph.run_graph

    def recording_grandchild_runs(self, **kwargs):
        run_info = run_graph(self, **kwargs)
        if self._subflow_depth == 2:
            grandchild_runs.append(run_info)
        return run_info

    monkeypatch.setattr(FlowGraph, "run_graph", recording_grandchild_runs)

    run_info = graph.run_graph()

    assert run_info.success is True, [result.error for result in run_info.node_step_result]
    assert [result.compute_error for result in run_info.node_step_result] == [None, None]
    [grandchild_run] = grandchild_runs
    assert {result.node_id: result.skipped for result in grandchild_run.node_step_result} == {
        1: False,
        2: False,
        3: True,
    }
    assert backend.asked == 1


def test_a_run_flow_node_over_a_flow_that_cannot_be_built_fails_when_it_runs(protocol_on, declare, tmp_path):
    backend = declare(without(everything(), scheme="file"))
    path = tmp_path / "not_yaml.yaml"
    path.write_text("flowfile_version: [unclosed\n  - :\n", encoding="utf-8")
    graph = run_flow_graph(8425, test_run_flow_node.register_flow_file(path, "not_yaml"))

    run_info = graph.run_graph()

    results = {result.node_id: result for result in run_info.node_step_result}
    assert results.keys() == {1, 2}, results
    assert results[1].success is True
    assert results[2].success is False
    assert results[2].error.startswith("while parsing a flow sequence"), results[2].error
    assert {result.compute_error for result in results.values()} == {None}
    assert run_info.success is False
    assert backend.work == ["submit 1"]


def test_a_flow_its_backend_can_run_runs_as_it_does_unplaced(declare, monkeypatch):
    backend = declare(everything())
    rows = {}
    for flag, flow_id in ((True, 8405), (False, 8406)):
        monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", flag)
        graph = fuzzy_flow(flow_id)

        run_info = graph.run_graph()

        assert run_info.success is True, [result.error for result in run_info.node_step_result]
        assert run_info.nodes_completed == 3
        assert "Flow completed!" in Path(graph.flow_logger.log_file_path).read_text()
        rows[flag] = graph.get_node(3).get_resulting_data().collect().sort(pl.all()).rows()
    assert rows[True] == rows[False]
    assert len(rows[True]) == 3
    assert backend.asked == 1
    assert "run_op fuzzy_join" in backend.work


def test_with_the_flag_off_nothing_is_placed(protocol_off, declare):
    backend = declare(without(everything(), operation="fuzzy_join"))
    graph = fuzzy_flow(8407)

    run_info = graph.run_graph()

    assert run_info.success is True, [result.error for result in run_info.node_step_result]
    assert backend.asked == 0
    assert "run_op fuzzy_join" in backend.work


def test_a_backend_missing_from_the_map_leaves_its_nodes_unplaced_and_unrefused():
    graph = model_flow(8408)

    placement = place(graph, {registry.LOCAL_BACKEND_ID: everything()}, SingleBackendPolicy(registry.WORKER_BACKEND_ID))

    assert placement == Placement(assignment={}, boundaries=[], refusals=[])


def test_a_formula_gate_is_refused_when_the_backend_of_the_input_it_probes_lacks_the_formula_library():
    graph = make_graph(8474, execution_location=registry.LOCAL_BACKEND_ID)
    add_manual_input(graph, 1, [{"a": 1}, {"a": 2}])
    graph.get_node(1).setting_input.execution_location = registry.WORKER_BACKEND_ID
    add_promise(graph, 2, "gate")
    graph.add_gate(
        input_schema.NodeGate(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            gate_input=transform_schema.GateInput(condition_source="formula", formula="[a] > 1"),
        )
    )
    connect(graph, 1, 2)
    capabilities = {
        registry.LOCAL_BACKEND_ID: everything(),
        registry.WORKER_BACKEND_ID: without(everything(), plugin="polars_expr_transformer"),
    }

    placement = place(graph, capabilities, MixedPlacementPolicy(registry.LOCAL_BACKEND_ID))

    [refusal] = placement.refusals
    assert (refusal.node_id, refusal.backend_id) == (2, registry.LOCAL_BACKEND_ID)
    assert refusal.reason.startswith(f'its formula is checked on backend "{registry.WORKER_BACKEND_ID}", where node 1')
    assert "polars_expr_transformer" in refusal.reason


def test_an_apply_model_node_on_another_backend_than_its_train_model_is_refused():
    graph = model_flow(8473, registry.WORKER_BACKEND_ID)
    graph.get_node(3).setting_input.execution_location = registry.LOCAL_BACKEND_ID
    capabilities = {registry.WORKER_BACKEND_ID: everything(), registry.LOCAL_BACKEND_ID: everything()}

    placement = place(graph, capabilities, MixedPlacementPolicy(registry.WORKER_BACKEND_ID))

    [refusal] = placement.refusals
    assert (refusal.node_id, refusal.backend_id) == (3, registry.LOCAL_BACKEND_ID)
    assert f'Train Model node 2 fits and holds on backend "{registry.WORKER_BACKEND_ID}"' in refusal.reason


def test_a_backend_that_cannot_be_asked_keeps_its_nodes_assigned_unchecked_and_unrefused():
    graph = model_flow(8472)
    down = ComputeUnavailable("Connection refused")

    placement = place(
        graph, {}, SingleBackendPolicy(registry.WORKER_BACKEND_ID), unreachable={registry.WORKER_BACKEND_ID: down}
    )

    assert placement.assignment == {node.node_id: registry.WORKER_BACKEND_ID for node in graph.nodes}
    assert placement.refusals == []
    assert placement.unreachable == {registry.WORKER_BACKEND_ID: down}


def test_the_nodes_of_a_backend_that_cannot_be_asked_fail_with_its_error_and_send_nothing(protocol_on, declare):
    backend = declare(ComputeUnavailable("Connection refused"))
    graph = fuzzy_flow(8412)

    run_info = graph.run_graph()

    errors = {result.node_id: result.error for result in run_info.node_step_result}
    assert errors.keys() == {1, 2}, errors
    for error in errors.values():
        assert error.startswith("Connection refused"), error
    assert {result.compute_error for result in run_info.node_step_result} == {"ComputeUnavailable"}
    assert graph.get_node(3).results.errors is None
    assert run_info.success is False
    assert backend.work == []
    assert backend.asked == 1


def test_a_node_a_version_mismatch_failed_carries_the_error_s_class_on_its_row(protocol_off, declare):
    declare(VersionMismatch("polars_version: core 1, worker 2"))
    graph = fuzzy_flow(8413)

    run_info = graph.run_graph()

    results = {result.node_id: result for result in run_info.node_step_result}
    assert results.keys() == {1, 2}, results
    for result in results.values():
        assert result.success is False
        assert result.error.startswith("polars_version: core 1, worker 2"), result.error
        assert result.compute_error == "VersionMismatch"
    assert run_info.success is False


@pytest.mark.parametrize(
    "policy, node_type, location, remedy",
    [
        (SingleBackendPolicy(BACKEND_ID), "filter", "auto", FLOW_REMEDY),
        (SingleBackendPolicy(BACKEND_ID), "filter", "local", OWN_WITHOUT_STORE_REMEDY),
        (MixedPlacementPolicy(BACKEND_ID), "filter", "auto", AUTO_REMEDY),
        (MixedPlacementPolicy(BACKEND_ID), "filter", "local", OWN_REMEDY),
        (MixedPlacementPolicy(BACKEND_ID), "google_analytics_reader", "auto", None),
    ],
    ids=["no-store-auto", "no-store-own", "store-auto", "store-own", "pinned"],
)
def test_a_refusal_names_the_setting_that_moves_its_node(policy, node_type, location, remedy):
    node = SimpleNamespace(node_type=node_type, setting_input=SimpleNamespace(execution_location=location))

    assert remedy_for(node, policy) == remedy


def test_a_version_mismatch_refuses_every_node_of_the_backend_before_any_starts(protocol_on, declare):
    backend = declare(VersionMismatch("polars_version: core 1, worker 2"))
    graph = fuzzy_flow(8414)

    run_info = graph.run_graph()

    results = {result.node_id: result for result in run_info.node_step_result}
    assert results.keys() == {1, 2, 3}, results
    for result in results.values():
        assert result.error == f'cannot run on backend "declaring": polars_version: core 1, worker 2. {FLOW_REMEDY}'
        assert result.compute_error == "Refusal"
    assert run_info.success is False
    assert backend.work == []


def test_a_run_on_local_in_a_process_that_serves_is_refused_before_local_is_made(protocol_on, declare, monkeypatch):
    monkeypatch.setattr(registry, "_serving", False)
    registry.mark_serving()
    local = DeclaringBackend(registry.LOCAL_BACKEND_ID, everything())
    made = []
    registry.register_backend(registry.LOCAL_BACKEND_ID, lambda: made.append(local) or local)
    graph = make_graph(8451, execution_location="local")
    add_manual_input(graph, 1, [{"a": 1}])

    run_info = graph.run_graph()

    assert_refused_before_any_node(graph, run_info, local, {1: ["this core serves; compute runs on its worker"]})
    assert made == []
    assert local.asked == 0


def test_a_node_another_error_failed_carries_no_compute_error(protocol_off, declare):
    declare(RuntimeError("boom"))
    graph = fuzzy_flow(8415)

    run_info = graph.run_graph()

    results = {result.node_id: result for result in run_info.node_step_result}
    assert results.keys() == {1, 2}, results
    for result in results.values():
        assert result.success is False
        assert "boom" in result.error, result.error
        assert result.compute_error is None
    assert run_info.success is False


def test_a_node_polars_own_compute_error_failed_carries_no_compute_error(protocol_off, declare):
    declare(pl.exceptions.ComputeError("could not decode the json"))
    graph = fuzzy_flow(8471)

    run_info = graph.run_graph()

    results = {result.node_id: result for result in run_info.node_step_result}
    assert results.keys() == {1, 2}, results
    for result in results.values():
        assert result.success is False
        assert result.compute_error is None, result.compute_error


def test_a_fetched_node_a_compute_error_failed_carries_the_error_s_class_on_its_row(declare, flag):
    declare(ComputeUnavailable("Connection refused"))
    graph = fuzzy_flow(8417 if flag else 8416)

    run_info = graph.trigger_fetch_node(1)

    [result] = run_info.node_step_result
    assert result.node_id == 1
    assert result.success is False
    assert result.error.startswith("Connection refused"), result.error
    assert result.compute_error == "ComputeUnavailable"


def test_a_node_the_run_skips_is_not_placed(protocol_on, declare):
    backend = declare(without(everything(), operation="fuzzy_join"))
    graph = fuzzy_flow(8409)
    add_promise(graph, 4, "select")
    connect(graph, 3, 4)

    run_info = graph.run_graph()

    assert_refused_before_any_node(graph, run_info, backend, {3: ["it needs the fuzzy_join operation"]})
    assert run_info.number_of_nodes == 3


def test_the_model_nodes_are_placed_on_the_run_s_backend():
    graph = model_flow(8410)
    worker = everything().model_copy(update={"operations": frozenset(kind.value for kind in OperationKind)})
    backends = {registry.LOCAL_BACKEND_ID: everything(), registry.WORKER_BACKEND_ID: worker}

    placement = place(graph, backends, SingleBackendPolicy(registry.LOCAL_BACKEND_ID))

    assert placement == Placement(
        assignment={1: registry.LOCAL_BACKEND_ID, 2: registry.LOCAL_BACKEND_ID, 3: registry.LOCAL_BACKEND_ID},
        boundaries=[],
        refusals=[],
    )


def test_a_model_published_to_or_fetched_from_a_filesystem_store_is_refused_off_cores_host(protocol_on, declare):
    backend = declare(without(everything(), scheme="file"))
    graph = model_flow(8411, "remote", publish="m", catalog="m")

    run_info = graph.run_graph()

    assert_refused_before_any_node(
        graph,
        run_info,
        backend,
        {
            2: ["it publishes its model to the artifact store, which is on local disk, not S3"],
            3: ["it fetches its model from the artifact store, which is on local disk, not S3"],
        },
    )
    assert not any("operation" in result.error for result in run_info.node_step_result)
    assert backend.asked == 1


def test_a_flow_scoped_model_runs_on_a_backend_off_cores_host(declare, flag):
    backend = declare(without(everything(), scheme="file"))
    graph = model_flow(8419 if flag else 8418, "remote")

    run_info = graph.run_graph()

    assert run_info.success is True, [result.error for result in run_info.node_step_result]
    assert {"run_op ml_train", "run_op ml_apply"} <= set(backend.work)
    assert backend.asked == (1 if flag else 0)
    scored = graph.get_node(3).get_resulting_data().collect()
    assert scored["pred"].to_list() == pytest.approx(scored["y"].to_list())
    backend.release(graph.get_node(2).results.model_ref)


@pytest.fixture
def s3_artifact_store(monkeypatch):
    """The S3 artifact store, made for the test and dropped after it."""
    monkeypatch.setenv("FLOWFILE_ARTIFACT_STORAGE", "s3")
    monkeypatch.setenv("FLOWFILE_S3_BUCKET", "models")
    reset_storage_backend()
    yield
    reset_storage_backend()


def test_an_s3_artifact_store_needs_https_where_a_model_crosses_it(s3_artifact_store):
    graph = model_flow(8420, "remote", publish="m", catalog="m")
    policy = SingleBackendPolicy(BACKEND_ID)

    without_https = place(graph, {BACKEND_ID: without(everything(), scheme="https")}, policy)
    without_file = place(graph, {BACKEND_ID: without(everything(), scheme="file")}, policy)

    assert without_https.refusals == [
        Refusal(2, BACKEND_ID, "it publishes its model to the artifact store over https"),
        Refusal(3, BACKEND_ID, "it fetches its model from the artifact store over https"),
    ]
    assert without_file.refusals == []


@pytest.fixture(scope="module")
def csv_url():
    """A CSV of three rows served on a free loopback port; the fixture is its ``http://`` URL."""
    with serve(b"n,label\n1,a\n2,b\n3,c\n", ranges=True) as url:
        yield f"{url}/rows.csv"


def url_read_flow(flow_id: int, url: str) -> FlowGraph:
    """Read node 1 over the CSV at ``url``."""
    graph = make_graph(flow_id)
    add_promise(graph, 1, "read")
    received = input_schema.ReceivedTable(path=url, file_type="csv", table_settings={"encoding": "utf-8"})
    graph.add_read(input_schema.NodeRead(flow_id=flow_id, node_id=1, received_file=received))
    return graph


def parameter_read_flow(flow_id: int, path: str, dir_value: str) -> FlowGraph:
    """Read node 1 over the CSV at ``path``, in a flow whose string parameter ``dir`` is ``dir_value``."""
    graph = make_graph(flow_id)
    graph.flow_settings.parameters = [schemas.FlowParameter(name="dir", default_value=dir_value, type="string")]
    add_promise(graph, 1, "read")
    received = input_schema.ReceivedTable(path=path, file_type="csv", table_settings={"encoding": "utf-8"})
    graph.add_read(input_schema.NodeRead(flow_id=flow_id, node_id=1, received_file=received))
    return graph


def test_a_read_from_a_url_is_refused_on_a_backend_that_does_not_read_its_scheme(protocol_on, declare, csv_url):
    backend = declare(without(everything(), scheme="http"))
    graph = url_read_flow(8432, csv_url)

    run_info = graph.run_graph()

    assert_refused_before_any_node(graph, run_info, backend, {1: ["it reads http URLs"]})


def test_a_read_from_a_url_runs_on_a_backend_that_reads_its_scheme(declare, flag, csv_url):
    backend = declare(everything())
    graph = url_read_flow(8431 if flag else 8430, csv_url)

    run_info = graph.run_graph()

    assert run_info.success is True, [result.error for result in run_info.node_step_result]
    assert graph.get_node(1).get_resulting_data().collect().rows() == [(1, "a"), (2, "b"), (3, "c")]
    assert backend.work == ["submit 1"]


def cannot_resolve(name: str, method: str) -> str:
    """The reason a backend that reaches storage with its own identity is refused a stored connection of ``method``."""
    return (
        f'connection "{name}" uses {method}, but this backend only resolves env_vars and managed_identity connections'
    )


@pytest.fixture
def stored_connection():
    """``stored_connection(auth_method)`` stores an S3 connection of that method for user 1 and answers its name.

    Each is deleted after the test; nothing reaches its endpoint.
    """
    names: list[str] = []

    def store(auth_method: str) -> str:
        name = f"placement {auth_method} {uuid.uuid4().hex[:6]}"
        keys = (
            {"aws_access_key_id": "AKIDPLACEMENT", "aws_secret_access_key": SecretStr("placement")}
            if auth_method == "access_key"
            else {}
        )
        connection = FullCloudStorageConnection(
            connection_name=name,
            storage_type="s3",
            auth_method=auth_method,
            aws_region="us-east-1",
            endpoint_url="http://localhost:9000",
            aws_allow_unsafe_html=True,
            **keys,
        )
        with get_db_context() as db:
            db_connections.store_cloud_connection(db, connection, user_id=1)
        names.append(name)
        return name

    yield store
    with get_db_context() as db:
        for name in names:
            db_connections.delete_cloud_connection(db, name, user_id=1)


def add_cloud_reader(graph: FlowGraph, node_id: int, connection_name: str | None) -> None:
    """A cloud storage reader of user 1 over a parquet file, through ``connection_name`` (``None``: no connection).

    Built with the start node's schema prefetch blocked: the prefetch would take the read's schema from the graph's
    backend in a thread of its own, so building the reader sends no backend anything.
    """
    add_promise(graph, node_id, "cloud_storage_reader")
    token = schema_prefetch_blocked.set(True)
    try:
        graph.add_cloud_storage_reader(
            input_schema.NodeCloudStorageReader(
                flow_id=graph.flow_id,
                node_id=node_id,
                user_id=1,
                cloud_storage_settings=CloudStorageReadSettings(
                    connection_name=connection_name, resource_path="s3://placement-bucket/data.parquet"
                ),
            )
        )
    finally:
        schema_prefetch_blocked.reset(token)


def test_a_cloud_read_of_an_access_key_connection_is_refused_on_an_ambient_backend(
    protocol_on, declare, stored_connection
):
    backend = declare(ambient())
    name = stored_connection("access_key")
    graph = make_graph(8438)
    add_cloud_reader(graph, 1, name)

    run_info = graph.run_graph()

    assert_refused_before_any_node(graph, run_info, backend, {1: [cannot_resolve(name, "access_key")]})


def test_an_ambient_backend_is_refused_no_connection_of_its_own_identity_and_a_master_key_one_looks_none_up(
    stored_connection, monkeypatch
):
    env_vars = stored_connection("env_vars")
    access_key = stored_connection("access_key")
    graph = make_graph(8439)
    add_cloud_reader(graph, 1, env_vars)
    add_cloud_reader(graph, 2, None)
    add_cloud_reader(graph, 3, access_key)
    policy = SingleBackendPolicy(BACKEND_ID)

    on_ambient = place(graph, {BACKEND_ID: ambient()}, policy)

    assert on_ambient.refusals == [Refusal(3, BACKEND_ID, cannot_resolve(access_key, "access_key"))]

    def no_lookup(*args, **kwargs):
        raise AssertionError("a backend that resolves a stored connection itself is placed without looking one up")

    monkeypatch.setattr(db_connections, "get_cloud_connection", no_lookup)

    on_master_key = place(graph, {BACKEND_ID: everything()}, policy)

    assert on_master_key == Placement(
        assignment={1: BACKEND_ID, 2: BACKEND_ID, 3: BACKEND_ID}, boundaries=[], refusals=[]
    )


def test_a_catalog_node_over_an_object_storage_namespace_is_refused_on_an_ambient_backend_for_its_connection(
    catalog_clean_state,
):
    _ensure_connection()
    graph = make_graph(8440)
    add_manual_input(graph, 1, [{"id": 1, "name": "ada"}])
    add_test_catalog_writer(
        graph,
        node_id=2,
        depending_on_id=1,
        table_name="dim",
        namespace_id=_create_cloud_schema("s3://flowfile-test/placement"),
    )

    placement = place(graph, {BACKEND_ID: ambient()}, SingleBackendPolicy(BACKEND_ID))

    assert placement.refusals == [Refusal(2, BACKEND_ID, cannot_resolve("catalog-cloudvt-minio", "access_key"))]


@pytest.mark.parametrize(
    ("flow_id", "directory", "refusals"),
    [
        (8444, "s3://bucket/prefix", []),
        (8445, None, [Refusal(1, BACKEND_ID, "it reads local files")]),
    ],
    ids=["object_storage", "local"],
)
def test_a_read_on_a_parameter_is_placed_by_the_path_it_resolves_to(tmp_path, flow_id, directory, refusals):
    graph = parameter_read_flow(flow_id, "${dir}/x.csv", directory or str(tmp_path))

    placement = place(graph, {BACKEND_ID: without(everything(), scheme="file")}, SingleBackendPolicy(BACKEND_ID))

    assert placement == Placement(assignment={1: BACKEND_ID}, boundaries=[], refusals=refusals)
    assert graph.get_node(1).setting_input.received_file.path == "${dir}/x.csv"


def test_a_read_on_a_parameter_runs_as_it_does_unplaced(declare, tmp_path, flag):
    backend = declare(everything())
    (tmp_path / "x.csv").write_text("n,label\n1,a\n2,b\n", encoding="utf-8")
    graph = parameter_read_flow(8447 if flag else 8446, "${dir}/x.csv", str(tmp_path))

    run_info = graph.run_graph()

    assert run_info.success is True, [result.error for result in run_info.node_step_result]
    assert graph.get_node(1).get_resulting_data().collect().rows() == [(1, "a"), (2, "b")]
    assert backend.work == ["submit 1"]
    assert graph.get_node(1).setting_input.received_file.path == "${dir}/x.csv"


def test_a_read_on_a_parameter_its_flow_does_not_define_is_not_refused_and_fails_when_it_runs(
    protocol_on, declare, tmp_path
):
    backend = declare(without(everything(), scheme="file"))
    graph = parameter_read_flow(8448, "${other}/x.csv", str(tmp_path))

    run_info = graph.run_graph()

    [result] = run_info.node_step_result
    assert result.node_id == 1
    assert result.success is False
    assert "Unresolved parameter references" in result.error, result.error
    assert result.compute_error is None
    assert run_info.success is False
    assert backend.work == []
    assert "no node was started" not in Path(graph.flow_logger.log_file_path).read_text()


def unchecked(error: Exception) -> str:
    """The reason a node is refused whose needs could not be read from its settings with its flow's parameters in."""
    return f"its settings could not be checked with the flow's parameters filled in ({type(error).__name__}: {error})"


def test_a_cloud_read_on_a_parameter_whose_value_is_no_path_is_refused_before_any_node_starts(protocol_on, declare):
    backend = declare(everything())
    graph = make_graph(8449)
    graph.flow_settings.parameters = [schemas.FlowParameter(name="n", default_value="1", type="integer")]
    add_promise(graph, 1, "cloud_storage_reader")
    read = CloudStorageReadSettings(resource_path="${n}", file_format="csv")
    graph.add_cloud_storage_reader(
        input_schema.NodeCloudStorageReader(flow_id=8449, node_id=1, cloud_storage_settings=read)
    )

    run_info = graph.run_graph()

    reason = unchecked(TypeError("argument of type 'int' is not iterable"))
    assert_refused_before_any_node(graph, run_info, backend, {1: [reason]})
    assert graph.get_node(1).setting_input.cloud_storage_settings.resource_path == "${n}"


@pytest.mark.parametrize(
    ("flow_id", "derivation", "error"),
    [
        (8450, requirements_for, RuntimeError("the derivation failed")),
        (8451, _catalog_target, OperationalError("SELECT 1", {}, Exception("database is locked"))),
    ],
    ids=["requirements_for", "_catalog_target"],
)
def test_a_write_whose_needs_cannot_be_read_is_refused_before_any_node_starts_and_writes_nothing(
    protocol_on, declare, tmp_path, monkeypatch, caplog, flow_id, derivation, error
):
    backend = declare(without(everything(), scheme="file"))
    graph = make_graph(flow_id)
    add_manual_input(graph, 1, [{"id": 1}, {"id": 2}])
    add_promise(graph, 2, "output")
    output = input_schema.OutputSettings(
        name="out.csv", directory=str(tmp_path), file_type="csv", table_settings=input_schema.OutputCsvTable()
    )
    graph.add_output(input_schema.NodeOutput(flow_id=flow_id, node_id=2, output_settings=output))
    connect(graph, 1, 2)

    def derive(*args, **kwargs):
        node_settings = args[0] if derivation is _catalog_target else args[1]
        if isinstance(node_settings, input_schema.NodeOutput):
            raise error
        return derivation(*args, **kwargs)

    monkeypatch.setattr(f"{place.__module__}.{derivation.__name__}", derive)

    run_info = graph.run_graph()

    assert_refused_before_any_node(graph, run_info, backend, {2: [unchecked(error)]})
    assert not (tmp_path / "out.csv").exists()
    [warning] = [record for record in caplog.records if record.module == "placement"]
    assert warning.levelname == "WARNING"
    assert warning.getMessage() == f'Node 2 of flow {flow_id} is refused on backend "{BACKEND_ID}": {unchecked(error)}'
    assert warning.exc_info[1] is error
