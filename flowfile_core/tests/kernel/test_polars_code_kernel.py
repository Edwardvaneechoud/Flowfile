"""Polars Code on a person's own kernel on a server (no Docker)."""

import ast
import asyncio

import polars as pl
import pytest

from flowfile_core.flowfile import flow_graph as flow_graph_module
from flowfile_core.flowfile.execution.protocol import OperationKind
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine, execute_polars_code
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.flow_graph.builders import code as code_builders
from flowfile_core.flowfile.flow_node.requirements import requirements_for
from flowfile_core.kernel.models import MANAGED_KERNEL_PREFIX, ImageFlavour, is_managed_kernel, managed_kernel_id
from flowfile_core.kernel.polars_code import CPU_VARIABLE, MEMORY_VARIABLE, kernel_script, owner_kernel
from flowfile_core.schemas import input_schema, transform_schema
from shared.polars_code_parser import IN_PROCESS_REFUSED_MESSAGE
from tests.conftest import protocol_states
from tests.flowfile.execution.graph_helpers import (
    add_manual_input,
    add_promise,
    add_select_on,
    add_sort_on,
    connect,
    make_graph,
)

OWNER = 7
ADD_ONE = "output_df = input_df.with_columns((pl.col('a') + 1).alias('b'))"

protocol = protocol_states()


class TestManagedKernelNames:
    def test_a_persons_kernel_is_named_after_their_id(self):
        assert managed_kernel_id(7) == "polars-code-7"
        assert is_managed_kernel(managed_kernel_id(7))

    def test_the_prefix_is_reserved_in_any_case(self):
        assert is_managed_kernel("Polars-Code-7")
        assert is_managed_kernel(MANAGED_KERNEL_PREFIX.upper() + "x")

    def test_other_ids_are_not_managed(self):
        assert not is_managed_kernel("my-polars-code-7")
        assert not is_managed_kernel("polars-code")
        assert not is_managed_kernel("default")


class _Ctx:
    """What a script sees as ``flowfile_ctx``: its inputs held in memory, and what it published."""

    def __init__(self, *frames: pl.DataFrame) -> None:
        self.frames = frames
        self.published: dict[str, pl.DataFrame] = {}

    def read_inputs(self) -> dict[str, list[pl.LazyFrame]]:
        return {"main": [frame.lazy() for frame in self.frames]}

    def publish_output(self, df: pl.LazyFrame | pl.DataFrame, name: str = "main") -> None:
        self.published[name] = df.lazy().collect()


def _run_script(code: str, *frames: pl.DataFrame) -> tuple[_Ctx, dict]:
    """Run ``kernel_script(code)`` as a kernel does: ``exec`` in a namespace that holds ``flowfile_ctx``."""
    ctx = _Ctx(*frames)
    namespace = {"flowfile_ctx": ctx}
    exec(kernel_script(code), namespace)
    return ctx, namespace


class TestKernelScript:
    A = pl.DataFrame({"a": [1, 2]})

    def test_a_one_line_expression(self):
        ctx, _ = _run_script("input_df.select(pl.col('a') * 10)", self.A)

        assert ctx.published["main"].to_dict(as_series=False) == {"a": [10, 20]}

    def test_a_snippet_that_sets_output_df(self):
        ctx, _ = _run_script(f"doubled = input_df\n{ADD_ONE}", self.A)

        assert ctx.published["main"].to_dict(as_series=False) == {"a": [1, 2], "b": [2, 3]}

    def test_the_function_form_takes_the_inputs_in_wiring_order(self):
        right = pl.DataFrame({"a": [2], "name": ["two"]})

        ctx, _ = _run_script("def run(left, right):\n    return left.join(right, on='a')", self.A, right)

        assert ctx.published["main"].to_dict(as_series=False) == {"a": [2], "name": ["two"]}

    def test_three_inputs_are_numbered(self):
        ctx, _ = _run_script("pl.concat([input_df_3, input_df_1, input_df_2])", self.A, self.A.head(1), self.A.tail(1))

        assert ctx.published["main"]["a"].to_list() == [2, 1, 2, 1]

    def test_no_inputs(self):
        ctx, _ = _run_script("output_df = pl.LazyFrame({'n': [1]})")

        assert ctx.published["main"].to_dict(as_series=False) == {"n": [1]}

    def test_an_eager_result_is_published(self):
        ctx, _ = _run_script("output_df = input_df.collect().head(1)", self.A)

        assert ctx.published["main"].to_dict(as_series=False) == {"a": [1]}

    def test_a_result_that_is_no_frame_fails(self):
        with pytest.raises(TypeError, match="must produce a Polars DataFrame or LazyFrame, not int"):
            _run_script("output_df = 5", self.A)

    def test_the_sandbox_refuses_what_it_refuses_on_a_desktop(self):
        with pytest.raises(ValueError, match="Import statements are not allowed"):
            _run_script("import os\noutput_df = input_df", self.A)

    @pytest.mark.parametrize("code", [ADD_ONE, "output_df = 5"], ids=["after a run", "after a failure"])
    def test_the_script_leaves_no_name_behind(self, code):
        ctx = _Ctx(self.A)
        namespace = {"flowfile_ctx": ctx}
        try:
            exec(kernel_script(code), namespace)
        except TypeError:
            pass

        assert set(namespace) == {"flowfile_ctx", "__builtins__"}

    @pytest.mark.parametrize(
        "code",
        [
            "'''",
            '"""',
            "\\",
            "x = 1\n_flowfile_polars_code()\n",
            "')\nimport os\nflowfile_ctx.escaped = True\n('",
            "output_df = input_df.select(pl.lit('{code}{source!r}{}'))",
        ],
        ids=["single quotes", "double quotes", "backslash", "the script's own name", "closing the literal", "braces"],
    )
    def test_the_code_is_one_string_literal_of_the_script(self, code):
        tree = ast.parse(kernel_script(code))

        assert [type(statement) for statement in tree.body] == [ast.FunctionDef, ast.Try]
        literals = [node.value for node in ast.walk(tree) if isinstance(node, ast.Constant)]
        assert code in literals

    def test_text_that_closes_the_literal_runs_as_sandboxed_code_only(self):
        ctx = _Ctx(self.A)

        with pytest.raises(ValueError):
            exec(kernel_script("')\nimport os\nflowfile_ctx.escaped = True\n('"), {"flowfile_ctx": ctx})

        assert not hasattr(ctx, "escaped")


class _Kernels:
    """A stand-in for the kernel manager's registry: kernels this process knows, and kernels only the catalog holds."""

    def __init__(self, known: dict[str, int] | None = None, in_catalog: dict[str, int] | None = None) -> None:
        self.owners = dict(known or {})
        self.in_catalog = dict(in_catalog or {})
        self.created: list = []

    def get_kernel_sync(self, kernel_id: str):
        return kernel_id if kernel_id in self.owners else None

    def get_kernel_owner(self, kernel_id: str) -> int | None:
        return self.owners.get(kernel_id)

    def reconcile_configs_from_db(self, owner_id: int | None = None) -> None:
        self.owners.update({kernel: owner for kernel, owner in self.in_catalog.items() if owner == owner_id})

    async def create_kernel(self, config, user_id: int):
        self.created.append(config)
        self.owners[config.id] = user_id


class TestOwnerKernel:
    def test_the_first_run_creates_the_kernel_and_the_next_reuses_it(self):
        kernels = _Kernels()

        assert owner_kernel(OWNER, lambda: kernels) == "polars-code-7"
        assert owner_kernel(OWNER, lambda: kernels) == "polars-code-7"

        (config,) = kernels.created
        assert (config.id, config.name, config.image_flavour) == ("polars-code-7", "Polars Code", ImageFlavour.LITE)
        assert config.persistence_enabled is False
        assert (config.cpu_cores, config.memory_gb) == (2.0, 4.0)
        assert kernels.owners == {"polars-code-7": OWNER}

    def test_a_kernel_another_process_created_is_found_in_the_catalog(self):
        kernels = _Kernels(in_catalog={"polars-code-7": OWNER})

        assert owner_kernel(OWNER, lambda: kernels) == "polars-code-7"
        assert kernels.created == []

    def test_it_is_created_from_a_thread_that_runs_an_event_loop(self):
        kernels = _Kernels()

        async def in_a_loop() -> str:
            return owner_kernel(OWNER, lambda: kernels)

        assert asyncio.run(in_a_loop()) == "polars-code-7"
        assert len(kernels.created) == 1

    def test_the_size_follows_the_two_variables(self, monkeypatch):
        monkeypatch.setenv(CPU_VARIABLE, "0.5")
        monkeypatch.setenv(MEMORY_VARIABLE, "16")
        kernels = _Kernels()

        owner_kernel(OWNER, lambda: kernels)

        assert (kernels.created[0].cpu_cores, kernels.created[0].memory_gb) == (0.5, 16.0)

    @pytest.mark.parametrize("value", ["", "lots", "0", "-2"])
    def test_a_size_that_is_no_positive_number_is_the_default(self, value, monkeypatch):
        monkeypatch.setenv(CPU_VARIABLE, value)
        monkeypatch.setenv(MEMORY_VARIABLE, value)
        kernels = _Kernels()

        owner_kernel(OWNER, lambda: kernels)

        assert (kernels.created[0].cpu_cores, kernels.created[0].memory_gb) == (2.0, 4.0)

    def test_a_kernel_with_the_persons_id_that_is_someone_elses_is_refused(self):
        kernels = _Kernels(known={"polars-code-7": 8})

        with pytest.raises(RuntimeError, match="'polars-code-7' .* belongs to another user"):
            owner_kernel(OWNER, lambda: kernels)

        assert kernels.created == []

    def test_a_step_without_an_owner_is_refused(self):
        with pytest.raises(RuntimeError, match="has no owner"):
            owner_kernel(None, _Kernels)

    def test_without_docker_it_is_refused_with_the_reason(self):
        def no_docker():
            raise OSError("Cannot connect to the Docker daemon")

        with pytest.raises(RuntimeError, match="Docker is not available: Cannot connect to the Docker daemon"):
            owner_kernel(OWNER, no_docker)


@pytest.fixture(params=["docker", "server"])
def on_a_server(request, monkeypatch) -> str:
    monkeypatch.setenv("FLOWFILE_MODE", request.param)
    return request.param


class TestRefusedInCore:
    def test_core_does_not_run_the_code_on_a_server(self, on_a_server, tmp_path):
        written = tmp_path / "written_by_polars_code.txt"
        code = f"pl.DataFrame({{'c': ['any text']}}).write_csv({str(written)!r})\noutput_df = input_df"

        with pytest.raises(PermissionError, match=IN_PROCESS_REFUSED_MESSAGE):
            execute_polars_code(FlowDataEngine(pl.LazyFrame({"a": [1]})), code=code)

        assert not written.exists()

    def test_core_still_runs_the_code_on_a_desktop(self):
        result = execute_polars_code(FlowDataEngine(pl.LazyFrame({"a": [1]})), code=ADD_ONE)

        assert result.collect().to_dicts() == [{"a": 1, "b": 2}]


def _never(*args, **kwargs):
    raise AssertionError("Polars Code reached a server process")


def _add_polars_code(graph: FlowGraph, node_id: int, code: str, sources: list[int]) -> None:
    add_promise(graph, node_id, "polars_code")
    graph.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=graph.flow_id,
            node_id=node_id,
            depending_on_ids=sources,
            polars_code_input=transform_schema.PolarsCodeInput(polars_code=code),
            user_id=OWNER,
        )
    )
    for source in sources:
        connect(graph, source, node_id)


@pytest.fixture
def kernel_calls(monkeypatch) -> list[dict]:
    """Every ``_execute_on_kernel`` call of a test, each answered with one row, with no way into a server process."""
    calls: list[dict] = []
    kernels = _Kernels()

    def answer(self, **kwargs):
        calls.append(kwargs)
        return FlowDataEngine(pl.LazyFrame({"answered": [1]}))

    monkeypatch.setattr(FlowGraph, "_execute_on_kernel", answer)
    monkeypatch.setattr(flow_graph_module, "get_kernel_manager", lambda: kernels)
    monkeypatch.setattr(code_builders, "_user_code_engine", _never)
    monkeypatch.setattr(code_builders, "execute_polars_code", _never)
    return calls


class TestOnAServer:
    def test_a_run_uses_the_owners_kernel_whatever_the_protocol_flag(self, on_a_server, protocol, kernel_calls):
        graph = make_graph(91441, execution_location="local")
        add_manual_input(graph, 1, [{"a": 1}])
        _add_polars_code(graph, 2, ADD_ONE, [1])

        run_info = graph.run_graph()

        assert run_info.success is True, [result.error for result in run_info.node_step_result]
        (call,) = kernel_calls
        assert call["kernel_id"] == "polars-code-7"
        assert call["node_id"] == 2
        assert call["code"] == kernel_script(ADD_ONE)
        assert call["output_names"] == ["main"]
        assert call["named_inputs"] is False
        assert len(call["flow_data_engine"]) == 1
        assert graph.get_node(2).get_resulting_data().collect().to_dicts() == [{"answered": 1}]

    def test_a_node_that_has_not_run_is_not_run_to_predict_its_columns(self, on_a_server, kernel_calls):
        graph = make_graph(91442, execution_location="local")
        add_manual_input(graph, 1, [{"a": 1}])
        _add_polars_code(graph, 2, ADD_ONE, [1])
        add_select_on(graph, 3, 2, [("a", "a")], keep_missing=True)
        add_sort_on(graph, 4, 2, "a")
        node = graph.get_node(2)

        assert not node.get_predicted_schema(force=True)
        assert "kernel node" in node.results.warnings and "has not run" in node.results.warnings
        assert node.get_predicted_resulting_data().schema == []
        # A select predicts from its input's schema; a sort runs its function over its input's predicted frame.
        assert not graph.get_node(3).get_predicted_schema(force=True)
        assert not graph.get_node(4).get_predicted_schema(force=True)
        assert kernel_calls == []

    def test_a_node_without_inputs_is_not_run_when_it_is_placed(self, on_a_server, kernel_calls):
        graph = make_graph(91443, execution_location="local")
        _add_polars_code(graph, 1, "output_df = pl.LazyFrame({'n': [1]})", [])

        assert not graph.get_node(1).get_predicted_schema(force=True)
        assert kernel_calls == []

    def test_after_a_run_the_columns_are_the_results(self, on_a_server, kernel_calls):
        graph = make_graph(91444, execution_location="local")
        add_manual_input(graph, 1, [{"a": 1}])
        _add_polars_code(graph, 2, ADD_ONE, [1])
        graph.run_graph()

        assert [column.name for column in graph.get_node(2).get_predicted_schema(force=True)] == ["answered"]
        assert len(kernel_calls) == 1

    def test_a_step_without_an_owner_fails_with_the_reason(self, on_a_server, monkeypatch):
        monkeypatch.setattr(flow_graph_module, "get_kernel_manager", _Kernels)
        graph = make_graph(91445, execution_location="local")
        add_manual_input(graph, 1, [{"a": 1}])
        add_promise(graph, 2, "polars_code")
        graph.add_polars_code(
            input_schema.NodePolarsCode(
                flow_id=graph.flow_id,
                node_id=2,
                depending_on_ids=[1],
                polars_code_input=transform_schema.PolarsCodeInput(polars_code=ADD_ONE),
            )
        )
        connect(graph, 1, 2)

        run_info = graph.run_graph()

        errors = {result.node_id: result.error for result in run_info.node_step_result if not result.success}
        assert "has no owner" in errors[2]


class TestRequirements:
    def _node(self, flow_id: int):
        graph = make_graph(flow_id, execution_location="local")
        add_manual_input(graph, 1, [{"a": 1}])
        _add_polars_code(graph, 2, ADD_ONE, [1])
        return graph.get_node(2)

    def test_on_a_desktop_polars_code_is_the_workers_operation(self):
        node = self._node(91451)

        needs = requirements_for(node.node_template, node.setting_input)

        assert (needs.operation, needs.needs_kernel) == (OperationKind.polars_code, False)
        assert node._executes_on_kernel is False
        assert node.schema_callback is None

    def test_on_a_server_polars_code_needs_a_kernel(self, on_a_server):
        node = self._node(91452)

        needs = requirements_for(node.node_template, node.setting_input)

        assert (needs.operation, needs.needs_kernel) == (OperationKind.file_write, True)
        assert needs.write_schemes == {"file"}
        assert node._executes_on_kernel is True

    def test_a_promise_answers_the_same_way(self, monkeypatch):
        graph = make_graph(91453, execution_location="local")
        add_promise(graph, 1, "polars_code")
        node = graph.get_node(1)

        assert requirements_for(node.node_template, node.setting_input).needs_kernel is False
        monkeypatch.setenv("FLOWFILE_MODE", "server")
        assert requirements_for(node.node_template, node.setting_input).needs_kernel is True
