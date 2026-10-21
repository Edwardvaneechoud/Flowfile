"""A headless run child must not sweep the cache and temp directories a live core still references, and both
headless runners make core's own backend the process's default through the backend registry, unless ``--backend``
or a backend id saved as the flow's location names another; a spawned run passes ``--backend`` only when its request
names one; and a run on the worker leaves no result it minted held there once its runner returns."""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

import flowfile_core
from flowfile_core.flowfile.execution.backends.local import LocalBackend
from shared.storage_config import storage

MAIN_PY = Path(flowfile_core.__file__).resolve().parent / "main.py"
RUN_ID = "987654"


def _frozen_verb(flow_path: str, *options: str) -> list[str]:
    """main.py's ``--run-flow`` verb, run as ``__main__`` the way the frozen binary enters it."""
    argv = [str(MAIN_PY), "--run-flow", flow_path, "--run-id", RUN_ID, *options]
    runner = f"import runpy, sys\nsys.argv = {argv!r}\nrunpy.run_path({str(MAIN_PY)!r}, run_name='__main__')\n"
    return [sys.executable, "-c", runner]


def _dev_verb(flow_path: str, *options: str) -> list[str]:
    return [sys.executable, "-m", "flowfile", "run", "flow", flow_path, "--run-id", RUN_ID, *options]


def _plant_stale(path: Path, age_hours: float) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("still referenced by the running core")
    stale = time.time() - age_hours * 3600
    os.utime(path, (stale, stale))
    return path


@pytest.mark.parametrize("command", [_frozen_verb, _dev_verb], ids=["frozen-verb", "dev-cli"])
def test_run_flow_child_leaves_cache_and_temp_alone(tmp_path, monkeypatch, command):
    storage_dir = tmp_path / "storage"
    cached = _plant_stale(storage_dir / "cache" / "stale.arrow", age_hours=2)
    scratch = _plant_stale(storage_dir / "temp" / "stale.txt", age_hours=48)
    env = {
        **os.environ,
        "FLOWFILE_DB_PATH": str(tmp_path / "catalog.db"),
        "FLOWFILE_STORAGE_DIR": str(storage_dir),
        "FLOWFILE_USER_DATA_DIR": str(tmp_path / "user_data"),
        "FLOWFILE_TELEMETRY": "0",
    }

    result = subprocess.run(
        command(str(tmp_path / "missing.yaml")), env=env, capture_output=True, text=True, timeout=300
    )

    output = result.stdout + result.stderr
    assert result.returncode == 1, output
    assert f"Run {RUN_ID} not found" in output, output
    assert cached.exists() and scratch.exists()

    # Control: a core start's sweep does delete these entries, so their survival above is meaningful.
    monkeypatch.setattr(storage, "_base_dir", storage_dir)
    storage.cleanup_directories()
    assert not cached.exists() and not scratch.exists()


def _frozen_runner(flow_path: str, backend: str | None = None) -> int:
    from flowfile_core.run_flow_cli import run_flow_cli

    return run_flow_cli(flow_path, int(RUN_ID), backend=backend)


def _dev_runner(flow_path: str, backend: str | None = None) -> int:
    from flowfile.__main__ import run_flow

    return run_flow(flow_path, backend=backend)


@pytest.mark.parametrize("runner", [_frozen_runner, _dev_runner], ids=["frozen-verb", "dev-cli"])
def test_a_headless_run_defaults_to_core_s_own_backend_and_leaves_the_offload_flag_alone(tmp_path, runner):
    from flowfile_core.configs import settings
    from flowfile_core.flowfile.execution import registry

    flag_before = (settings.OFFLOAD_TO_WORKER, bool(settings.OFFLOAD_TO_WORKER))
    default_before = registry.default_backend_id()
    previous = registry.default_override()
    try:
        assert runner(str(tmp_path / "missing.yaml")) == 1
        assert registry.default_override() == registry.LOCAL_BACKEND_ID
        assert registry.default_backend_id() == registry.LOCAL_BACKEND_ID
    finally:
        registry.set_default_override(previous)

    assert (settings.OFFLOAD_TO_WORKER, bool(settings.OFFLOAD_TO_WORKER)) == flag_before
    assert registry.default_backend_id() == default_before


class CountingBackend(LocalBackend):
    """Core's own backend under another id; ``submitted`` holds the node id of every plan submitted to it."""

    def __init__(self, backend_id: str):
        super().__init__()
        self.id = backend_id
        self.submitted: list[int | str] = []

    def submit(self, plan, inputs, hints, result_id, **kwargs):
        self.submitted.append(kwargs["node_id"])
        return super().submit(plan, inputs, hints, result_id, **kwargs)


@pytest.fixture
def counting() -> list[CountingBackend]:
    """``counting`` is registered; the list holds each instance its factory made."""
    from flowfile_core.flowfile.execution import registry

    made: list[CountingBackend] = []

    def make_counting() -> CountingBackend:
        made.append(CountingBackend("counting"))
        return made[-1]

    registry.reset()
    registry.register_backend("counting", make_counting)
    yield made
    registry.reset()


@pytest.fixture
def in_process_run(monkeypatch):
    """A runner called in this process installs no telemetry, and the default override it sets is undone after."""
    from flowfile_core import telemetry
    from flowfile_core.flowfile.execution import registry

    monkeypatch.setattr(telemetry, "install_headless", lambda: None)
    previous = registry.default_override()
    yield
    registry.set_default_override(previous)


@pytest.fixture
def session_worker() -> str:
    """The session worker's URL. A child names it in ``FLOWFILE_WORKER_URL``: importing ``flowfile`` (the dev verb,
    and any test that ran it in this process) points the default worker at the single-file server's port."""
    from tests.conftest import WORKER_HOST, WORKER_PORT, is_worker_running

    if os.environ.get("SKIP_WORKER_TESTS") == "1" or not is_worker_running():
        pytest.skip("needs the session worker (SKIP_WORKER_TESTS=1 or no worker running)")
    return f"http://{WORKER_HOST}:{WORKER_PORT}"


def _save_group_by_flow(path: Path) -> Path:
    """Manual input -> group_by in Development mode, saved ``remote`` as the designer saves a flow by default."""
    from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
    from flowfile_core.schemas import input_schema, schemas, transform_schema

    graph = FlowGraph(
        schemas.FlowSettings(
            flow_id=99461, name="headless_backend", path=".", execution_location="local", execution_mode="Development"
        )
    )
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist([{"a": i % 2} for i in range(4)]),
        )
    )
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="group_by"))
    graph.add_group_by(
        input_schema.NodeGroupBy(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            groupby_input=transform_schema.GroupByInput(
                agg_cols=[transform_schema.AggColl("a", "groupby"), transform_schema.AggColl("a", "count", "n")]
            ),
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    graph.flow_settings.execution_location = "remote"
    graph.save_flow(str(path))
    return path


def _closed_port_url() -> str:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return f"http://127.0.0.1:{probe.getsockname()[1]}"


def test_both_verbs_hand_the_backend_option_to_their_runner(monkeypatch):
    import flowfile.__main__ as flowfile_main
    from flowfile_core import run_flow_cli

    calls: list[tuple[str, str | None]] = []

    def frozen_runner(flow_path, run_id, backend=None):
        calls.append(("frozen-verb", backend))
        return 0

    def dev_runner(flow_path, param_overrides=None, run_id=None, backend=None):
        calls.append(("dev-cli", backend))
        return 0

    monkeypatch.setattr(run_flow_cli, "run_flow_cli", frozen_runner)
    monkeypatch.setattr(flowfile_main, "run_flow", dev_runner)

    assert run_flow_cli.main(["main.py", "--run-flow", "f.yaml", "--run-id", "1", "--backend", "worker:team"]) == 0
    assert run_flow_cli.main(["main.py", "--run-flow", "f.yaml", "--run-id", "1"]) == 0
    for options in (["--backend", "worker:team"], []):
        monkeypatch.setattr(sys, "argv", ["flowfile", "run", "flow", "f.yaml", *options])
        with pytest.raises(SystemExit) as exited:
            flowfile_main.main()
        assert exited.value.code == 0

    assert calls == [
        ("frozen-verb", "worker:team"),
        ("frozen-verb", None),
        ("dev-cli", "worker:team"),
        ("dev-cli", None),
    ]


@pytest.mark.parametrize("flag", [False, True], ids=["flag-off", "flag-on"])
@pytest.mark.parametrize("runner", [_frozen_runner, _dev_runner], ids=["frozen-verb", "dev-cli"])
def test_a_headless_run_on_a_named_backend_submits_every_node_there(
    tmp_path, monkeypatch, counting, in_process_run, runner, flag
):
    from flowfile_core.configs import settings
    from flowfile_core.flowfile.execution import registry

    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", flag)
    flow_path = _save_group_by_flow(tmp_path / "flow.yaml")

    assert runner(str(flow_path), backend="counting") == 0

    assert registry.default_override() == "counting"
    assert len(counting) == 1
    assert sorted(counting[0].submitted) == [1, 2]


@pytest.mark.parametrize("flag", [False, True], ids=["flag-off", "flag-on"])
@pytest.mark.parametrize("runner", [_frozen_runner, _dev_runner], ids=["frozen-verb", "dev-cli"])
def test_a_headless_run_on_the_worker_leaves_no_result_it_minted_held_there(
    tmp_path, monkeypatch, in_process_run, session_worker, runner, flag
):
    from flowfile_core.configs import settings
    from flowfile_core.flowfile.execution import registry
    from tests.flowfile.execution.test_mixed_placement import record_refs, still_held

    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", flag)
    monkeypatch.setattr(settings, "WORKER_URL", session_worker)
    registry.reset()
    answered = record_refs(monkeypatch, "submit", "run_op")
    flow_path = _save_group_by_flow(tmp_path / "flow.yaml")
    try:
        assert runner(str(flow_path), backend="worker") == 0

        assert {(method, backend.url) for method, backend, _ in answered} == {("submit", session_worker)}
        assert still_held(answered) == []
    finally:
        registry.reset()


@pytest.mark.parametrize("flag", [False, True], ids=["flag-off", "flag-on"])
@pytest.mark.parametrize("runner", [_frozen_runner, _dev_runner], ids=["frozen-verb", "dev-cli"])
def test_a_headless_run_with_no_backend_named_runs_a_saved_remote_flow_in_its_own_process(
    tmp_path, monkeypatch, counting, in_process_run, runner, flag
):
    from flowfile_core.configs import settings
    from flowfile_core.flowfile.execution import registry

    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", flag)
    flow_path = _save_group_by_flow(tmp_path / "flow.yaml")
    monkeypatch.setenv("FLOWFILE_COMPUTE_BACKEND", "counting")

    assert runner(str(flow_path)) == 0

    assert registry.default_override() == registry.LOCAL_BACKEND_ID
    assert counting == []


@pytest.mark.parametrize("flag", [False, True], ids=["flag-off", "flag-on"])
@pytest.mark.parametrize("runner", [_frozen_runner, _dev_runner], ids=["frozen-verb", "dev-cli"])
def test_a_headless_run_with_no_backend_named_runs_on_the_backend_id_the_flow_names(
    tmp_path, monkeypatch, counting, in_process_run, runner, flag
):
    from flowfile_core.configs import settings
    from flowfile_core.flowfile.execution import registry
    from flowfile_core.flowfile.manage import io_flowfile

    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", flag)
    flow_path = _save_group_by_flow(tmp_path / "flow.yaml")
    real_open_flow = io_flowfile.open_flow

    def open_flow_naming_counting(*args, **kwargs):
        flow = real_open_flow(*args, **kwargs)
        flow.flow_settings.execution_location = "counting"
        return flow

    monkeypatch.setattr(io_flowfile, "open_flow", open_flow_naming_counting)

    assert runner(str(flow_path)) == 0

    assert registry.default_override() == "counting"
    assert len(counting) == 1
    assert sorted(counting[0].submitted) == [1, 2]


@pytest.mark.parametrize("frozen", [False, True], ids=["dev-cli", "frozen-verb"])
def test_a_spawned_run_passes_backend_only_when_its_request_names_one(tmp_path, monkeypatch, frozen):
    from shared import subprocess_utils

    commands: list[list[str]] = []

    def popen(cmd, **kwargs):
        commands.append(cmd)
        return SimpleNamespace(pid=4242)

    monkeypatch.setattr(subprocess_utils, "subprocess", SimpleNamespace(Popen=popen))
    monkeypatch.setattr(subprocess_utils, "run_log_path", lambda run_id: tmp_path / f"run_{run_id}.log")
    monkeypatch.setattr(sys, "frozen", frozen, raising=False)

    assert subprocess_utils.spawn_flow_subprocess("flow.yaml", 7, backend="worker") == 4242
    assert subprocess_utils.spawn_flow_subprocess("flow.yaml", 7) == 4242

    named, plain = commands
    assert named == [*plain, "--backend", "worker"]
    assert "--backend" not in plain
    assert plain[1] == ("--run-flow" if frozen else "-m")


@pytest.mark.parametrize("flag", ["0", "1"], ids=["flag-off", "flag-on"])
@pytest.mark.parametrize("command", [_frozen_verb, _dev_verb], ids=["frozen-verb", "dev-cli"])
def test_a_headless_run_on_backend_worker_runs_on_the_worker(tmp_path, session_worker, command, flag):
    flow_path = str(_save_group_by_flow(tmp_path / "flow.yaml"))
    env = {
        **os.environ,
        "FLOWFILE_COMPUTE_PROTOCOL": flag,
        "FLOWFILE_TELEMETRY": "0",
        "FLOWFILE_WORKER_URL": session_worker,
        "PYTHONIOENCODING": "utf-8",
    }

    on_worker = subprocess.run(
        command(flow_path, "--backend", "worker"),
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=300,
    )
    unreachable = subprocess.run(
        command(flow_path, "--backend", "worker"),
        env={**env, "FLOWFILE_WORKER_URL": _closed_port_url()},
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=300,
    )

    assert on_worker.returncode == 0, on_worker.stdout + on_worker.stderr
    output = unreachable.stdout + unreachable.stderr
    assert unreachable.returncode == 1, output
    assert "Could not reach the worker" in output, output


def _finished_run(flow_path: Path, backend: str | None, monkeypatch) -> tuple[bool | None, int]:
    """Spawns a scheduled run of ``flow_path`` through the run service, waits for its child, and reads its record.

    The wait goes through the child's ``Popen``: on Windows ``os.waitpid`` takes a process handle, not a pid."""
    from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
    from flowfile_core.catalog.services.runs import FlowRunService
    from flowfile_core.database import models as db_models
    from flowfile_core.database.connection import get_db_context
    from shared import subprocess_utils

    children: list[subprocess.Popen] = []

    def popen(cmd, **kwargs):
        children.append(subprocess.Popen(cmd, **kwargs))
        return children[-1]

    monkeypatch.setattr(subprocess_utils, "subprocess", SimpleNamespace(Popen=popen))
    with get_db_context() as db:
        registration = db_models.FlowRegistration(
            flow_uuid=str(uuid.uuid4()), name="headless_backend", flow_path=str(flow_path), owner_id=1
        )
        db.add(registration)
        db.commit()
        db.refresh(registration)
        run = FlowRunService(SQLAlchemyCatalogRepository(db)).spawn_flow_run(
            registration, user_id=1, run_type="scheduled", suppress_telemetry=True, backend=backend
        )
        run_id, pid = run.id, run.pid
    assert [child.pid for child in children] == [pid]
    children[0].wait(timeout=300)
    with get_db_context() as db:
        finished = db.get(db_models.FlowRun, run_id)
        return finished.success, finished.nodes_completed


@pytest.mark.parametrize("flag", ["0", "1"], ids=["flag-off", "flag-on"])
def test_a_scheduled_run_runs_on_the_backend_its_request_names_and_in_its_own_process_otherwise(
    tmp_path, monkeypatch, session_worker, flag
):
    monkeypatch.setenv("FLOWFILE_COMPUTE_PROTOCOL", flag)
    monkeypatch.setenv("FLOWFILE_WORKER_URL", session_worker)
    flow_path = _save_group_by_flow(tmp_path / "flow.yaml")

    assert _finished_run(flow_path, backend="worker", monkeypatch=monkeypatch) == (True, 2)

    monkeypatch.setenv("FLOWFILE_WORKER_URL", _closed_port_url())
    assert _finished_run(flow_path, backend="worker", monkeypatch=monkeypatch)[0] is False
    assert _finished_run(flow_path, backend=None, monkeypatch=monkeypatch) == (True, 2)
