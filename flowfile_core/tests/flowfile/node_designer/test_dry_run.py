"""Tests for the custom-node dry-run backend.

Error paths (syntax / no-sample-data / worker-down) run without a worker; the
happy path and timeout stub the worker backend's transport (the status, cancel
and clear helpers it calls) so no live worker is required while the run is
followed through the backend itself.
"""

import io
import json
from types import SimpleNamespace

import polars as pl
import pytest
import requests

from flowfile_core.flowfile.execution.backends import worker as worker_module
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import subprocess_operations as subops
from flowfile_core.flowfile.node_designer.state import (
    DesignerState,
    EnvironmentState,
    SectionState,
    TextInputState,
)
from flowfile_core.flowfile.user_defined import dry_run as dr
from flowfile_core.flowfile.user_defined.dry_run import (
    DryRunRequest,
    DryRunResponse,
    run_dry_run,
)

CODE_OK = (
    "import polars as pl\n"
    "from flowfile import node_designer as nd\n\n\n"
    "class Passthrough(nd.CustomNodeBase):\n"
    '    node_name: str = "Passthrough"\n'
    "    def process(self, *inputs):\n"
    "        return inputs[0]\n"
)

CODE_WITH_EXAMPLES = (
    "import polars as pl\n"
    "from flowfile import node_designer as nd\n\n\n"
    "class Passthrough(nd.CustomNodeBase):\n"
    '    node_name: str = "Passthrough"\n'
    '    example_inputs: list[dict[str, list]] = [{"a": [1, 2, 3]}]\n'
    "    def process(self, *inputs):\n"
    "        return inputs[0]\n"
)


def _designer_state() -> DesignerState:
    return DesignerState(
        class_name="Passthrough",
        settings_class_name="PassthroughSettings",
        node_name="Passthrough",
        sections=[SectionState(name="main", components=[TextInputState(name="prefix", label="Prefix")])],
        process_code="def process(self, *inputs):\n    return inputs[0]",
        example_inputs=None,
    )


# -- request validation --------------------------------------------------------


def test_requires_exactly_one_of_code_or_designer_state():
    with pytest.raises(ValueError):
        DryRunRequest(sample_inputs=None)
    with pytest.raises(ValueError):
        DryRunRequest(code="x", designer_state=_designer_state(), sample_inputs=None)


def test_row_limit_and_timeout_clamped():
    req = DryRunRequest(code=CODE_OK, sample_inputs=[{"a": [1]}], row_limit=99999, timeout_seconds=99999)
    assert req.row_limit == 1000
    assert req.timeout_seconds == 120
    req2 = DryRunRequest(code=CODE_OK, sample_inputs=[{"a": [1]}], row_limit=0, timeout_seconds=0)
    assert req2.row_limit == 1
    assert req2.timeout_seconds == 1


# -- error paths that never touch the worker -----------------------------------


def test_syntax_error_is_caught_in_core(monkeypatch):
    called = {"worker": False}

    def _boom(*a, **k):
        called["worker"] = True
        raise AssertionError("worker must not be called on a syntax error")

    monkeypatch.setattr(dr, "trigger_custom_node_operation", _boom)
    bad = "def process(self):\n    return  = broken"
    resp = run_dry_run(DryRunRequest(code=bad, sample_inputs=[{"a": [1]}]), user_id=1)
    assert resp.success is False
    assert resp.error_kind == "syntax"
    assert called["worker"] is False


def test_oversized_sample_rows_rejected_before_worker(monkeypatch):
    def _boom(*a, **k):
        raise AssertionError("worker must not be called for oversized sample inputs")

    monkeypatch.setattr(dr, "trigger_custom_node_operation", _boom)
    huge = [{"a": list(range(dr._MAX_SAMPLE_ROWS + 1))}]
    resp = run_dry_run(DryRunRequest(code=CODE_OK, sample_inputs=huge), user_id=1)
    assert resp.success is False
    assert resp.error_kind == "no_sample_data"
    assert "rows" in resp.error


def test_sample_input_total_cell_cap_rejected(monkeypatch):
    def _boom(*a, **k):
        raise AssertionError("worker must not be called for oversized sample inputs")

    monkeypatch.setattr(dr, "trigger_custom_node_operation", _boom)
    rows = dr._MAX_SAMPLE_ROWS // 2  # each column under the row cap
    n_cols = (dr._MAX_SAMPLE_CELLS // rows) + 2  # ... but together over the cell cap
    sample = {f"c{i}": list(range(rows)) for i in range(n_cols)}
    resp = run_dry_run(DryRunRequest(code=CODE_OK, sample_inputs=[sample]), user_id=1)
    assert resp.success is False
    assert resp.error_kind == "no_sample_data"
    assert "cells" in resp.error


def test_no_sample_data_runs_with_empty_inputs(monkeypatch):
    # No sample data no longer blocks: the node runs against one empty frame per input.
    captured = {}

    def _capture(request):
        captured["inputs"] = request.inputs
        raise ConnectionError("stop here")

    monkeypatch.setattr(dr, "trigger_custom_node_operation", _capture)
    resp = run_dry_run(DryRunRequest(code=CODE_OK, sample_inputs=None), user_id=1)
    assert resp.error_kind != "no_sample_data"
    assert len(captured["inputs"]) == 1  # CODE_OK defaults to number_of_inputs=1


def test_zero_input_node_runs_without_sample_data(monkeypatch):
    captured = {}

    def _capture(request):
        captured["inputs"] = request.inputs
        raise ConnectionError("stop here")

    monkeypatch.setattr(dr, "trigger_custom_node_operation", _capture)
    code = (
        "import polars as pl\n"
        "from flowfile import node_designer as nd\n\n\n"
        "class Src(nd.CustomNodeBase):\n"
        '    node_name: str = "Src"\n'
        "    number_of_inputs: int = 0\n"
        "    def process(self, *inputs):\n"
        '        return pl.LazyFrame({"x": [1, 2, 3]})\n'
    )
    resp = run_dry_run(DryRunRequest(code=code, sample_inputs=None), user_id=1)
    assert resp.error_kind != "no_sample_data"
    assert captured["inputs"] == []  # a source node runs with zero inputs


def _capture_first_input_frame(monkeypatch, request: DryRunRequest) -> pl.DataFrame:
    """Run up to the worker seam and return the first serialized sample as a DataFrame."""
    captured = {}

    def _capture(req):
        captured["inputs"] = req.inputs
        raise ConnectionError("stop here")

    monkeypatch.setattr(dr, "trigger_custom_node_operation", _capture)
    resp = run_dry_run(request, user_id=1)
    assert resp.error_kind == "load", resp.error  # reached the worker seam, not a sample error
    return pl.LazyFrame.deserialize(io.BytesIO(captured["inputs"][0])).collect()


def test_typed_sample_inputs_cast_to_declared_schema(monkeypatch):
    """RawData samples cast cells to the declared dtypes (whole numbers -> Float64)."""
    sample = {
        "columns": [
            {"name": "name", "data_type": "String"},
            {"name": "value", "data_type": "Float64"},
        ],
        "data": [["bob", "magret", "fish", "dog"], [21, 62.1, 1.2, 20]],
    }
    df = _capture_first_input_frame(monkeypatch, DryRunRequest(code=CODE_OK, sample_inputs=[sample]))
    assert df.schema["value"] == pl.Float64
    assert df["value"].to_list() == [21.0, 62.1, 1.2, 20.0]
    assert df["name"].to_list() == ["bob", "magret", "fish", "dog"]


def test_dict_sample_input_mixed_int_float_promotes_to_float(monkeypatch):
    """Legacy {col: values} samples with mixed int/float build a Float64 column, not an error."""
    sample = {"name": ["bob", "magret", "fish", "dog"], "value": [21, 62.1, 1.2, 20]}
    df = _capture_first_input_frame(monkeypatch, DryRunRequest(code=CODE_OK, sample_inputs=[sample]))
    assert df.schema["value"] == pl.Float64
    assert df["value"].to_list() == [21.0, 62.1, 1.2, 20.0]


def test_nested_example_inputs_with_null_reach_worker_typed(monkeypatch):
    """Parsed example_inputs with List/Struct cells and a null row stay nested, not repr() strings."""
    code = CODE_WITH_EXAMPLES.replace(
        '[{"a": [1, 2, 3]}]',
        '[{"id": [1, 2], "tags": [["a", "b"], None], "meta": [{"k": 1}, None]}]',
    )
    df = _capture_first_input_frame(monkeypatch, DryRunRequest(code=code, sample_inputs=None))
    assert df.schema["tags"] == pl.List(pl.String)
    assert df.schema["meta"] == pl.Struct({"k": pl.Int64})
    assert df["tags"].to_list() == [["a", "b"], None]
    assert df["meta"].to_list() == [{"k": 1}, None]


def test_example_inputs_lifted_from_code(monkeypatch):
    captured = {}

    def _capture(request):
        captured["inputs"] = request.inputs
        raise ConnectionError("stop here")  # short-circuit after capturing

    monkeypatch.setattr(dr, "trigger_custom_node_operation", _capture)
    resp = run_dry_run(DryRunRequest(code=CODE_WITH_EXAMPLES, sample_inputs=None), user_id=1)
    # example_inputs were found and serialized (1 input port), then the worker was unreachable
    assert len(captured["inputs"]) == 1
    assert resp.error_kind == "load"


def test_worker_unavailable_is_load_error(monkeypatch):
    def _refuse(request):
        raise ConnectionError("connection refused")

    monkeypatch.setattr(dr, "trigger_custom_node_operation", _refuse)
    resp = run_dry_run(DryRunRequest(code=CODE_OK, sample_inputs=[{"a": [1, 2, 3]}]), user_id=1)
    assert resp.success is False
    assert resp.error_kind == "load"
    assert "unavailable" in resp.error.lower()


# -- happy path + failure via stubbed worker -----------------------------------


class _Status:
    def __init__(self, task_id):
        self.background_task_id = task_id


def _status(payload: dict) -> SimpleNamespace:
    """The worker's ``Status`` for a task, as the transport's ``get_status`` returns it."""
    return SimpleNamespace(
        status=payload["status"],
        progress=payload.get("progress", 0),
        error_message=payload.get("error_message"),
        results=payload.get("results"),
    )


def _stub_worker(monkeypatch, status_payloads):
    """status_payloads: successive answers of the worker for the task's status: a dict, or an exception to raise."""
    monkeypatch.setattr(dr, "trigger_custom_node_operation", lambda request: _Status("task-1"))
    calls = {"get": 0, "clear": 0, "cancel": 0, "task_ids": set()}

    def _get_status(task_id):
        calls["task_ids"].add(task_id)
        answer = status_payloads[min(calls["get"], len(status_payloads) - 1)]
        calls["get"] += 1
        if isinstance(answer, Exception):
            raise answer
        return _status(answer)

    def _clear(task_id):
        calls["task_ids"].add(task_id)
        calls["clear"] += 1
        return True

    def _cancel(task_id):
        calls["task_ids"].add(task_id)
        calls["cancel"] += 1
        return True

    monkeypatch.setattr(worker_module, "get_status", _get_status)
    monkeypatch.setattr(worker_module, "cancel_task", _cancel)
    monkeypatch.setattr(dr, "clear_task_from_worker", _clear)
    return calls


def test_happy_path_maps_preview(monkeypatch):
    completed = {
        "status": "Completed",
        "results": json.dumps(
            {
                "outputs": {"main": {"path": "/tmp/x.arrow", "row_count": 3}},
                "preview": {
                    "main": {
                        "columns": [{"name": "a", "data_type": "Int64"}],
                        "rows": [[1], [2], [3]],
                        "truncated": False,
                    }
                },
                "logs": ["INFO: ran"],
                "duration_ms": 12.5,
            }
        ),
    }
    calls = _stub_worker(monkeypatch, [completed])
    resp = run_dry_run(DryRunRequest(code=CODE_OK, sample_inputs=[{"a": [1, 2, 3]}]), user_id=1)
    assert isinstance(resp, DryRunResponse)
    assert resp.success is True
    assert resp.executed_in == "worker"
    assert len(resp.outputs) == 1
    out = resp.outputs[0]
    assert out.name == "main"
    assert out.row_count == 3
    assert out.columns[0].name == "a"
    assert out.columns[0].data_type == "Int64"
    assert out.rows == [[1], [2], [3]]
    assert resp.logs == ["INFO: ran"]
    assert resp.duration_ms == 12.5
    assert calls["clear"] == 1  # best-effort cleanup ran


def test_execution_error_carries_traceback(monkeypatch):
    err = {
        "status": "Error",
        "error_message": "boom\nTraceback (most recent call last):\n  File x\nValueError: boom",
    }
    _stub_worker(monkeypatch, [err])
    resp = run_dry_run(DryRunRequest(code=CODE_OK, sample_inputs=[{"a": [1]}]), user_id=1)
    assert resp.success is False
    assert resp.error_kind == "execution"
    assert resp.error == "boom"
    assert resp.traceback is not None
    assert resp.traceback.startswith("Traceback (most recent call last):")


def test_multi_output_designer_state_maps_all_outputs(monkeypatch):
    state = DesignerState(
        class_name="Splitter",
        settings_class_name="SplitterSettings",
        node_name="Splitter",
        number_of_outputs=2,
        output_names=["pass", "fail"],
        process_code='def process(self, *inputs):\n    return {"pass": inputs[0], "fail": inputs[0]}',
        example_inputs=None,
    )
    completed = {
        "status": "Completed",
        "results": json.dumps(
            {
                "outputs": {
                    "pass": {"path": "/tmp/p.arrow", "row_count": 2},
                    "fail": {"path": "/tmp/f.arrow", "row_count": 1},
                },
                "preview": {
                    "pass": {"columns": [{"name": "a", "data_type": "Int64"}], "rows": [[1], [2]], "truncated": False},
                    "fail": {"columns": [{"name": "a", "data_type": "Int64"}], "rows": [[3]], "truncated": False},
                },
                "logs": [],
                "duration_ms": 4.0,
            }
        ),
    }
    _stub_worker(monkeypatch, [completed])
    resp = run_dry_run(DryRunRequest(designer_state=state, sample_inputs=[{"a": [1, 2, 3]}]), user_id=1)
    assert resp.success is True
    assert [o.name for o in resp.outputs] == ["pass", "fail"]
    assert resp.outputs[0].row_count == 2
    assert resp.outputs[1].row_count == 1


def test_the_run_is_followed_until_it_completes_and_cleared_once(monkeypatch):
    completed = {"status": "Completed", "results": {"outputs": {"main": {"row_count": 1}}, "preview": {}}}
    calls = _stub_worker(monkeypatch, [{"status": "Starting"}, {"status": "Processing", "progress": 50}, completed])
    monkeypatch.setattr(dr, "_POLL_INTERVAL", 0)

    resp = run_dry_run(DryRunRequest(code=CODE_OK, sample_inputs=[{"a": [1]}]), user_id=1)

    assert resp.success is True, resp.error
    assert resp.outputs[0].row_count == 1
    assert calls["get"] == 4, "three status polls, then the result of the finished task"
    assert (calls["clear"], calls["cancel"]) == (1, 0)
    assert calls["task_ids"] == {"task-1"}


def test_a_run_past_its_time_limit_is_cancelled_through_the_backend(monkeypatch):
    calls = _stub_worker(monkeypatch, [{"status": "Processing"}])
    monkeypatch.setattr(dr, "_POLL_INTERVAL", 0.05)

    resp = run_dry_run(DryRunRequest(code=CODE_OK, sample_inputs=[{"a": [1]}], timeout_seconds=1), user_id=1)

    assert (resp.success, resp.error_kind) == (False, "timeout")
    assert resp.error == "Dry run exceeded the time limit and was cancelled."
    assert (calls["cancel"], calls["clear"]) == (1, 1)


def test_a_cancel_the_worker_cannot_receive_still_answers_timeout(monkeypatch):
    calls = _stub_worker(monkeypatch, [{"status": "Processing"}])
    monkeypatch.setattr(dr, "_POLL_INTERVAL", 0.05)

    def _unreachable(task_id):
        calls["cancel"] += 1
        raise Exception("Failed to cancel task: connection refused")

    monkeypatch.setattr(worker_module, "cancel_task", _unreachable)

    resp = run_dry_run(DryRunRequest(code=CODE_OK, sample_inputs=[{"a": [1]}], timeout_seconds=1), user_id=1)

    assert resp.error_kind == "timeout"
    assert calls["cancel"] == 1


def test_a_run_the_worker_cancelled_answers_timeout(monkeypatch):
    calls = _stub_worker(monkeypatch, [{"status": "Cancelled"}])

    resp = run_dry_run(DryRunRequest(code=CODE_OK, sample_inputs=[{"a": [1]}]), user_id=1)

    assert (resp.error_kind, resp.error) == ("timeout", "Dry run was cancelled.")
    assert calls["clear"] == 1


def test_a_worker_lost_while_polled_is_a_load_error(monkeypatch):
    calls = _stub_worker(monkeypatch, [requests.ConnectionError("connection refused")])

    resp = run_dry_run(DryRunRequest(code=CODE_OK, sample_inputs=[{"a": [1]}]), user_id=1)

    assert (resp.success, resp.error_kind) == (False, "load")
    assert resp.error.startswith("The compute worker is unavailable:")
    assert "connection refused" in resp.error
    assert calls["clear"] == 1


def test_a_task_the_worker_no_longer_knows_is_an_execution_error(monkeypatch):
    _stub_worker(monkeypatch, [Exception('Could not fetch the status, {"detail":"Task not found"}')])

    resp = run_dry_run(DryRunRequest(code=CODE_OK, sample_inputs=[{"a": [1]}]), user_id=1)

    assert (resp.success, resp.error_kind) == (False, "execution")
    assert "Task not found" in resp.error


def test_a_failure_without_a_message_still_names_the_failure(monkeypatch):
    _stub_worker(monkeypatch, [{"status": "Unknown Error"}])

    resp = run_dry_run(DryRunRequest(code=CODE_OK, sample_inputs=[{"a": [1]}]), user_id=1)

    assert (resp.error_kind, resp.error) == ("execution", "Unknown Error")


# -- kernel-environment routing ------------------------------------------------


def _kernel_designer_state() -> DesignerState:
    return DesignerState(
        class_name="Passthrough",
        settings_class_name="PassthroughSettings",
        node_name="Passthrough",
        environment=EnvironmentState(kind="kernel"),
        sections=[],
        process_code="def process(self, *inputs):\n    return inputs[0]",
        example_inputs=None,
    )


def test_kernel_node_without_kernel_id_errors_before_worker(monkeypatch):
    """A kernel-env node with no kernel selected fails clearly, never in the worker."""

    def _boom(*a, **k):
        raise AssertionError("worker must not be called for a kernel-env node")

    monkeypatch.setattr(dr, "trigger_custom_node_operation", _boom)
    resp = run_dry_run(
        DryRunRequest(designer_state=_kernel_designer_state(), sample_inputs=[{"a": [1]}]),
        user_id=1,
    )
    assert resp.success is False
    assert resp.error_kind == "execution"
    assert "kernel" in resp.error.lower()


def test_kernel_output_read_rejects_path_traversal(tmp_path):
    """A ``../`` output name must not read another run's parquet off the shared volume."""
    output_dir = tmp_path / "flow" / "node" / "outputs"
    output_dir.mkdir(parents=True)
    pl.DataFrame({"a": [1, 2]}).write_parquet(output_dir / "main.parquet")

    victim_dir = tmp_path / "flow" / "victim" / "outputs"
    victim_dir.mkdir(parents=True)
    pl.DataFrame({"secret": [42]}).write_parquet(victim_dir / "main.parquet")

    traversal = "../../victim/outputs/main"
    outputs, err = dr._read_kernel_output_previews(str(output_dir), ["main", traversal], object(), 100)

    assert err is None
    by_name = {o.name: o for o in outputs}
    assert by_name["main"].rows == [[1], [2]]  # the run's own output still reads
    # the traversal name resolves outside output_dir, so nothing is read or leaked
    assert by_name[traversal].rows == []
    assert by_name[traversal].columns == []
    assert by_name[traversal].row_count == 0


def test_dry_run_kernel_request_disables_log_callback():
    """Dry runs blank the log callback so flowfile_ctx.log() prints to stdout the
    Test tab renders, instead of streaming to core's /raw_logs flow logger."""
    from flowfile_core.kernel.execution import build_execute_request

    class _FakeManager:
        # The callback URL keys on the network, not the volume: a kernel can only
        # resolve core by service name when it was joined to core's network.
        _docker_network = None
        _kernel_volume = None

        def to_kernel_path(self, path):
            return path

    kwargs = dict(
        node_id=7,
        code="x = 1",
        input_paths={"main": []},
        output_dir="/tmp/out",
        flow_id=dr._DRY_RUN_FLOW_ID,
        manager=_FakeManager(),
        source_registration_id=None,
    )
    # Baseline: a production request DOES point log() at core's /raw_logs.
    assert build_execute_request(**kwargs).log_callback_url != ""
    # Dry-run request blanks it.
    assert dr._build_dry_run_execute_request(**kwargs).log_callback_url == ""


def test_dry_run_kernel_request_sets_the_dry_run_flag():
    """Without this flag the kernel writes a real, versioned catalog artifact every
    time the Test panel runs a node that calls publish_global."""
    from flowfile_core.kernel.execution import build_execute_request

    class _FakeManager:
        # The callback URL keys on the network, not the volume: a kernel can only
        # resolve core by service name when it was joined to core's network.
        _docker_network = None
        _kernel_volume = None

        def to_kernel_path(self, path):
            return path

    kwargs = dict(
        node_id=7,
        code="x = 1",
        input_paths={"main": []},
        output_dir="/tmp/out",
        flow_id=dr._DRY_RUN_FLOW_ID,
        manager=_FakeManager(),
        source_registration_id=None,
    )
    assert build_execute_request(**kwargs).dry_run is False
    assert dr._build_dry_run_execute_request(**kwargs).dry_run is True


def test_dry_run_kernel_script_seeds_example_artifacts():
    """The Test panel's generated script seeds example_artifacts(); production does not."""
    from flowfile_core.flowfile.user_defined.kernel_codegen import generate_kernel_script

    source = (
        "import polars as pl\n"
        "from flowfile import node_designer as nd\n"
        "\n"
        "class N(nd.CustomNodeBase):\n"
        '    node_name: str = "N"\n'
        "\n"
        "    def example_artifacts(self) -> dict:\n"
        '        return {"m": {"coef": 2}}\n'
        "\n"
        "    def process(self, *inputs):\n"
        "        return inputs[0]\n"
    )
    kwargs = dict(
        node_source=source,
        class_name="N",
        settings_values={},
        output_names=["main"],
        number_of_inputs=1,
    )
    production = generate_kernel_script(**kwargs)
    dry = generate_kernel_script(**kwargs, dry_run=True)

    assert "_seed_hook" not in production
    assert "_result = _Node().process(*_inputs)" in production
    assert "_seed_hook" in dry
    # Both scopes: seeding only the global store breaks read_artifact() nodes.
    assert "flowfile_ctx.publish_global(_seed_key, _seed_obj)" in dry
    assert "flowfile_ctx.publish_artifact(_seed_key, _seed_obj)" in dry
    assert "example_artifacts() must return a dict" in dry
    # Delete-then-publish, so an edited hook isn't shadowed by the previous press.
    assert "flowfile_ctx.delete_artifact(_seed_key)" in dry
    assert "except ValueError" not in dry


def _seeded_aliases(script: str) -> dict:
    """The alias table the prologue actually bakes into the dry-run script."""
    import ast
    import json
    import re

    match = re.search(r"_SEED_ALIASES = _json\.loads\((.+)\)\s*$", script, re.M)
    assert match, "the dry-run prologue does not emit _SEED_ALIASES"
    return json.loads(ast.literal_eval(match.group(1)))


def test_dry_run_prologue_seeds_qualified_artifact_aliases():
    """A namespace-qualified selector value must resolve inside the sandbox too."""
    from flowfile_core.flowfile.user_defined.kernel_codegen import generate_kernel_script

    source = (
        "import polars as pl\n"
        "from flowfile import node_designer as nd\n\n\n"
        "class N(nd.CustomNodeBase):\n"
        '    node_name: str = "N"\n'
        "\n"
        "    def example_artifacts(self) -> dict:\n"
        '        return {"m": {"coef": 2}}\n'
        "\n"
        "    def process(self, *inputs):\n"
        "        return inputs[0]\n"
    )
    def _script(settings_values: dict) -> str:
        return generate_kernel_script(
            node_source=source,
            class_name="N",
            settings_values=settings_values,
            output_names=["main"],
            number_of_inputs=1,
            dry_run=True,
        )

    dry = _script({"main": {"model": "General.models::m", "other": "plain"}})
    assert _seeded_aliases(dry) == {"m": ["General.models::m"]}
    # The seed loop keys on the bare name, so the alias is what makes the
    # qualified spelling resolve inside the sandbox.
    assert "for _seed_key in [_seed_name] + _SEED_ALIASES.get(_seed_name, [])" in dry

    # A multi-select contributes every qualified entry it holds.
    multi = _script({"main": {"models": ["General.models::m", "Other.ns::m", "bare"]}})
    assert _seeded_aliases(multi) == {"m": ["General.models::m", "Other.ns::m"]}

    # No qualified value ⇒ nothing to alias.
    assert _seeded_aliases(_script({"main": {"model": "m", "count": 3}})) == {}


@pytest.mark.kernel
def test_dry_run_executes_on_kernel(monkeypatch):
    """A kernel-env dry-run runs process() inside the Docker kernel (Docker required).

    Points the kernel-manager singleton at the fixture's manager and writes sample
    parquet locally (OFFLOAD_TO_WORKER off) so no live worker is needed.
    """
    import flowfile_core.kernel as kernel_pkg
    from flowfile_core.configs.settings import OFFLOAD_TO_WORKER
    from tests.kernel_fixtures import managed_kernel

    code = (
        "import polars as pl\n"
        "from flowfile import node_designer as nd\n\n\n"
        "class KernelNode(nd.CustomNodeBase):\n"
        '    node_name: str = "KernelNode"\n'
        '    environment: str = "kernel"\n'
        "    def process(self, *inputs):\n"
        "        return inputs[0].with_columns(pl.lit(1).alias('added'))\n"
    )

    prev = bool(OFFLOAD_TO_WORKER)
    OFFLOAD_TO_WORKER.set(False)
    try:
        with managed_kernel() as (manager, kernel_id):
            monkeypatch.setattr(kernel_pkg, "get_kernel_manager", lambda: manager)
            resp = run_dry_run(
                DryRunRequest(code=code, sample_inputs=[{"a": [1, 2, 3]}], kernel_id=kernel_id),
                user_id=1,
            )
    finally:
        OFFLOAD_TO_WORKER.set(prev)

    assert resp.success is True, resp.error
    assert resp.executed_in == "kernel"
    assert resp.outputs[0].row_count == 3
    assert "added" in [c.name for c in resp.outputs[0].columns]


# -- a worker that fails core's handshake while the run is polled ---------------


def test_worker_failing_the_handshake_while_polled_answers_500_with_the_message(monkeypatch):
    from fastapi.testclient import TestClient

    from flowfile_core import main
    from flowfile_core.auth.jwt import get_current_active_user
    from flowfile_core.auth.models import User
    from shared.compute_protocol import local_capabilities

    admin = User(username="local_user", id=1, disabled=False, is_admin=True, must_change_password=False)
    reported = local_capabilities().model_copy(update={"polars_version": "0.0.1"})
    monkeypatch.setattr(dr, "trigger_custom_node_operation", lambda request: _Status("task-1"))
    monkeypatch.setattr(handshake, "fetch_worker_capabilities", lambda worker_url: reported)
    handshake.reset_handshake()
    main.app.dependency_overrides[get_current_active_user] = lambda: admin
    try:
        response = TestClient(main.app).post(
            "/user_defined_components/dry-run", json={"code": CODE_OK, "sample_inputs": [{"a": [1, 2, 3]}]}
        )
    finally:
        main.app.dependency_overrides.pop(get_current_active_user, None)
        handshake.reset_handshake()

    assert response.status_code == 500, response.text
    assert response.json()["detail"] == (
        f"The worker at {subops.WORKER_URL} does not match core (polars_version: core {pl.__version__}, worker 0.0.1)"
    )
