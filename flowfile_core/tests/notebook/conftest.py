"""Shared fixtures for the canvas-notebook tests: the corpus and its placeholder manifest, the production
runner and its test-only ``exec`` twin, the corpus clean-run once per runner, a per-user ``TestClient``
factory, and the ``kernel-sim`` manager that runs a notebook kernel's calls in-process."""

import contextlib
import contextvars
import copy
import io
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from flowfile_core import flow_file_handler, main
from flowfile_core.auth.jwt import get_current_active_user, get_current_user
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.kernel.models import ExecuteResult, KernelInfo, KernelState
from flowfile_core.notebook import bridge
from flowfile_core.notebook.interpret import CellInterpreter
from flowfile_core.notebook.push import seed_snapshot
from flowfile_core.notebook.render import NotebookRendering, render
from flowfile_core.notebook.runner import NotebookRunner, install_notebook_runner
from flowfile_frame.notebook_cells import exec_cell
from test_utils.notebook_demo import storage_files
from tests.notebook.corpus import build_corpus, demo_graph, load_expected_placeholders

NOTEBOOK_OWNER_ID = 1
RUNNER_KINDS = ("interpreting", "exec")
_RUN_MINTED_FIELDS = {"flow_id", "flowfile_id", "flowfile_name"}

KERNEL_CALLS_DURING_CORPUS: list[str] = []

IN_KERNEL_OP: contextvars.ContextVar[bool] = contextvars.ContextVar("in_kernel_op", default=False)
"""Set in the thread a :class:`KernelSimManager` runs a kernel op on; the session's calls back to core run in a
fresh context, so a listener that reads it tells the kernel's own work from core's (:func:`kernel_db_opens`)."""

_DATA_LAYER = (
    "/catalog/",
    "/database/",
    "/auth/",
    "/secret_manager/",
    "/kernel/persistence.py",
    "/database_connection_manager/",
    "/kafka/connection_manager.py",
)
_PACKAGES = ("flowfile_frame/flowfile_frame/", "flowfile_core/flowfile_core/", "flowfile/flowfile/")


def _opened_from(stack) -> str:
    """The nearest flowfile frame above the data layer that opened a connection, as ``module.py:function``."""
    for frame in reversed(stack):
        filename = frame.filename.replace("\\", "/")
        if "/tests/" in filename or any(part in filename for part in _DATA_LAYER):
            continue
        for package in _PACKAGES:
            if package in filename:
                return f"{filename.split(package, 1)[1]}:{frame.name}"
    return "?"


@pytest.fixture
def kernel_db_opens() -> list[str]:
    """Every catalog connection a kernel op (``IN_KERNEL_OP``) checked out, by the flowfile call site that opened it.

    The ``kernel-sim`` kernel shares core's engine, so a pool listener under the sim's marker sees exactly the
    connections the kernel's own work would open in a container; core's answers to the session's calls back run in
    a fresh context and are not counted.
    """
    import traceback

    from sqlalchemy import event

    from flowfile_core.database import connection

    opened: list[str] = []

    def checked_out(dbapi_connection, connection_record, connection_proxy):
        if IN_KERNEL_OP.get():
            opened.append(_opened_from(traceback.extract_stack()[:-1]))

    event.listen(connection.engine, "checkout", checked_out)
    try:
        yield opened
    finally:
        event.remove(connection.engine, "checkout", checked_out)


@contextmanager
def no_kernel_manager():
    """Record every ``KernelManager`` construction and ``get_kernel_manager()`` call inside the block.

    CI runs the whole core suite in one process, so an earlier test may already have initialised the
    manager; checking the global would then fail for the wrong reason. Counting the calls the notebook
    code makes is independent of that state. ``flow_graph`` binds the accessor by name, so that site is
    patched too (``dry_run`` reaches it through the package).
    """
    import flowfile_core.kernel as kernel_package
    from flowfile_core.flowfile import flow_graph as flow_graph_module

    calls: list[str] = []
    original_cls, original_get = kernel_package.KernelManager, kernel_package.get_kernel_manager

    class CountingKernelManager(original_cls):
        def __init__(self, *args, **kwargs):
            calls.append("KernelManager()")
            super().__init__(*args, **kwargs)

    def counting_get(*args, **kwargs):
        calls.append("get_kernel_manager()")
        return original_get(*args, **kwargs)

    sites = [(kernel_package, "get_kernel_manager"), (flow_graph_module, "get_kernel_manager")]
    saved = [(module, name, getattr(module, name)) for module, name in sites]
    kernel_package.KernelManager = CountingKernelManager
    for module, name in sites:
        setattr(module, name, counting_get)
    try:
        yield calls
    finally:
        kernel_package.KernelManager = original_cls
        for module, name, value in saved:
            setattr(module, name, value)


@pytest.fixture(scope="session")
def notebook_corpus(tmp_path_factory):
    """``[(name, FlowGraph)]``: the codegen flows, the frame-built native nodes and the demo, last as ``"demo"``.

    Session scoped and built once; tests must not mutate the graphs. The demo's catalog, child flow and
    custom node stay seeded until the session ends.
    """
    with no_kernel_manager() as calls:
        corpus = build_corpus(lambda name: tmp_path_factory.mktemp(name))
        with demo_graph() as graph:
            KERNEL_CALLS_DURING_CORPUS.extend(calls)
            yield corpus + [("demo", graph)]


@pytest.fixture(scope="session")
def expected_placeholders():
    """Flow name -> node ids allowed to render as placeholders; the ledger may only shrink."""
    return load_expected_placeholders()


class ExecRunner(NotebookRunner):
    """The production runner with its executor swapped for ``exec``: runs the cells as Python, for comparison.

    Every step but the cell executor is the production runner's (snapshot session, lock, user, result).
    Test-only: core never runs cells with ``exec``.
    """

    executor = staticmethod(lambda: exec_cell)


RUNNERS = {"interpreting": NotebookRunner, "exec": ExecRunner}


def cell_provenance(graph: FlowGraph, rendering: NotebookRendering) -> dict[str, list[tuple[str, int]]]:
    """The provenance a push sends: each rendered cell's ``(node_type, canvas_id)`` pairs."""
    return {
        cell.cell_id: [(graph.get_node(node_id).node_type, node_id) for node_id in cell.node_ids]
        for cell in rendering.cells
        if cell.node_ids
    }


def clean_run_request(graph: FlowGraph, rendering: NotebookRendering) -> bridge.CleanRunRequest:
    """The request ``plan_push`` hands the runner for ``graph``'s unedited rendered cells."""
    return bridge.CleanRunRequest(
        cells=[(cell.cell_id, cell.code) for cell in rendering.cells],
        provenance=cell_provenance(graph, rendering),
        ceiling=max((node.node_id for node in graph.nodes), default=0),
        snapshot=seed_snapshot(graph),
    )


def masked_payload(payload):
    """``payload`` without what a clean run mints: the session graph's identity and fresh Python Script cell ids."""

    def mask(value, key=None):
        if isinstance(value, dict):
            return {k: mask(v, k) for k, v in value.items() if k not in _RUN_MINTED_FIELDS}
        if isinstance(value, list):
            if key == "cells":
                return [{k: v for k, v in cell.items() if k != "id"} for cell in value]
            return [mask(item) for item in value]
        return value

    return mask(copy.deepcopy(payload))


@dataclass
class CorpusRun:
    """One corpus flow clean-run by one runner; ``interpreter`` is the interpreting run's executor, else ``None``."""

    graph: FlowGraph
    rendering: NotebookRendering
    provenance: dict[str, list[tuple[str, int]]]
    result: bridge.CleanRunResult
    interpreter: CellInterpreter | None


@dataclass
class CorpusPass:
    """Every corpus flow clean-run once by one runner, with the storage files and kernel calls the pass made."""

    runs: dict[str, CorpusRun]
    files: set[Path]
    kernel_calls: list[str]


class CorpusRuns:
    """The corpus clean-run once per runner kind, on first use, for every test module of the session.

    ``"interpreting"`` is the production :class:`NotebookRunner`, keeping each run's interpreter so tests can
    read what it used and counted; ``"exec"`` is :class:`ExecRunner`. A pass calls its runner the way
    ``plan_push`` does, as the notebook owner, inside :func:`no_kernel_manager` and between two
    ``storage_files()`` listings taken after the snapshots, whose first pivot schema prediction writes a worker
    cache file. Every lookup hands out copies of the results, so a test that changes one (``reconcile`` fills an
    output node's table settings in place) cannot change what another test reads.
    """

    def __init__(self, corpus: list[tuple[str, FlowGraph]]) -> None:
        self._corpus = corpus
        self._runners = {kind: runner_class() for kind, runner_class in RUNNERS.items()}
        self._interpreters: list[CellInterpreter] = []
        self._passes: dict[str, CorpusPass] = {}

        def keeping_interpreter() -> CellInterpreter:
            self._interpreters.append(NotebookRunner.executor())
            return self._interpreters[-1]

        self._runners["interpreting"].executor = keeping_interpreter

    def __getitem__(self, kind: str) -> CorpusPass:
        if kind not in self._passes:
            self._passes[kind] = self._pass(kind)
        corpus_pass = self._passes[kind]
        runs = {name: replace(run, result=run.result.model_copy(deep=True)) for name, run in corpus_pass.runs.items()}
        return replace(
            corpus_pass, runs=runs, files=set(corpus_pass.files), kernel_calls=list(corpus_pass.kernel_calls)
        )

    def _pass(self, kind: str) -> CorpusPass:
        runner = self._runners[kind]
        runs = {}
        with no_kernel_manager() as kernel_calls:
            rendered = [(name, graph, render(graph)) for name, graph in self._corpus]
            requests = [clean_run_request(graph, rendering) for _, graph, rendering in rendered]
            before = storage_files()
            for (name, graph, rendering), request in zip(rendered, requests):
                kept = len(self._interpreters)
                result = runner.clean_run(NOTEBOOK_OWNER_ID, graph.flow_id, request)
                interpreter = self._interpreters[-1] if len(self._interpreters) > kept else None
                runs[name] = CorpusRun(graph, rendering, cell_provenance(graph, rendering), result, interpreter)
        return CorpusPass(runs, storage_files() - before, list(kernel_calls))

    def close(self) -> None:
        """Drop the runners and their passes: they own no threads, so no mode may be left and the lock is free."""
        from flowfile_frame import notebook

        self._runners.clear()
        self._passes.clear()
        assert notebook.current() is None
        assert not notebook.RUN_LOCK.locked()


@pytest.fixture(scope="session")
def corpus_runs(notebook_corpus):
    """:class:`CorpusRuns` over the corpus, shared by the corpus test modules and shut down at the end."""
    runs = CorpusRuns(notebook_corpus)
    yield runs
    runs.close()


@pytest.fixture(params=RUNNER_KINDS)
def runner_kind(request) -> str:
    """Each runner in turn: the production interpreting runner, then the test-only ``exec`` one."""
    return request.param


@pytest.fixture
def runner():
    """The runner ``main.py`` installs (through the same function), restoring the previous one afterwards."""
    before = bridge._runner
    install_notebook_runner()
    yield bridge.get_clean_runner()
    bridge.set_clean_runner(before)


@pytest.fixture
def exec_runner():
    """:class:`ExecRunner` installed for the test, restoring the previous runner afterwards."""
    before = bridge._runner
    bridge.set_clean_runner(ExecRunner())
    yield bridge.get_clean_runner()
    bridge.set_clean_runner(before)


@pytest.fixture
def open_as(tmp_path):
    """Open a frame-built flow the way the editor does (save, then ``open_flow``, history on) for ``user_id``."""
    opened = []

    def _open(graph, user_id=NOTEBOOK_OWNER_ID):
        path = tmp_path / f"flow_{len(opened)}.yaml"
        graph.save_flow(str(path))
        graph = open_flow(path)
        flow_file_handler._flows[graph.flow_id] = graph
        flow_file_handler._register_user_session(user_id, graph.flow_id)
        opened.append((graph.flow_id, user_id))
        return graph

    yield _open
    for flow_id, user_id in opened:
        flow_file_handler._unregister_user_session(user_id, flow_id)
        flow_file_handler.delete_flow(flow_id)


@pytest.fixture
def orders_flow(open_as):
    import flowfile as ff

    orders = ff.from_dict({"id": [1, 2, 3], "amount": [10, 20, 30]})
    result = orders.filter(ff.col("amount") > 10).with_columns((ff.col("amount") * 2).alias("double"))
    return open_as(result.flow_graph)


@pytest.fixture
def client_as():
    def _as(user_id: int, client: tuple[str, int] | None = None) -> TestClient:
        user = PydanticUser(username=f"nb_{user_id}", id=user_id, disabled=False, is_admin=user_id == NOTEBOOK_OWNER_ID)
        main.app.dependency_overrides[get_current_active_user] = lambda: user
        main.app.dependency_overrides[get_current_user] = lambda: user
        return TestClient(main.app) if client is None else TestClient(main.app, client=client)

    yield _as
    main.app.dependency_overrides.pop(get_current_active_user, None)
    main.app.dependency_overrides.pop(get_current_user, None)


class KernelSimManager:
    """The ``kernel-sim`` runner's stand-in ``KernelManager``: one notebook kernel, no Docker.

    ``execute_sync`` runs the snippet core sends with real ``exec`` on a new thread, in a copy of the
    calling context and in ``namespaces[request.flow_id]`` (the kernel's per-flow namespace), with stdout and
    stderr captured, as the kernel runtime runs a call, and returns an
    ``ExecuteResult``; so ``notebook.kernel_runner`` and the frame's ``notebook_kernel`` session run for real.
    The shared folder is ``shared_volume_path`` and paths are the same on both sides. :meth:`node_result` and
    :meth:`node_run` are the session's transports to core: ``kernel_runner.node_result`` and
    ``held_run.run_held_node`` in a fresh context, as the kernel's owner, with their ``HTTPException`` detail
    raised as the kernel would see it.
    """

    def __init__(
        self, kernel_id: str = "nb-kernel", owner_id: int = NOTEBOOK_OWNER_ID, shared: Path | None = None
    ) -> None:
        self.kernel = KernelInfo(id=kernel_id, name="Notebook", state=KernelState.IDLE, packages=["flowfile"])
        self.owner_id = owner_id
        self.requests = []
        self.shared_volume_path = str(shared)
        self.node_results: list[dict] = []
        self.node_runs: list[dict] = []
        self.lookups: list[dict] = []
        self.namespaces: dict[int, dict] = {}
        self._docker_network = None

    def to_kernel_path(self, local_path):
        return local_path

    def _as_owner(self, call, *args):
        from fastapi import HTTPException

        from flowfile_frame.native import NativeNodeError

        user = PydanticUser(username="nb_kernel_owner", id=self.owner_id, disabled=False)
        try:
            return contextvars.Context().run(call, self.kernel.id, user, *args)
        except HTTPException as exc:
            raise NativeNodeError(str(exc.detail)) from exc

    def node_result(self, body: dict) -> dict:
        from flowfile_core.notebook import kernel_runner

        self.node_results.append(body)
        return self._as_owner(kernel_runner.node_result, body["flow_id"], body["node_id"], body.get("output_handle"))

    def node_run(self, body: dict) -> dict:
        from flowfile_core.notebook import held_run

        self.node_runs.append(body)
        return self._as_owner(held_run.run_held_node, held_run.NodeRunRequest.model_validate(body))

    def lookup(self, body: dict) -> dict:
        from flowfile_core.notebook import lookup

        self.lookups.append(body)
        return self._as_owner(lookup.answer_request, lookup.LookupRequest.model_validate(body))

    def get_kernel_sync(self, kernel_id):
        return self.kernel if kernel_id == self.kernel.id else None

    def get_kernel_owner(self, kernel_id):
        return self.owner_id if kernel_id == self.kernel.id else None

    def interrupt_execution_sync(self, kernel_id, exec_token=None):
        return False

    def execute_sync(self, kernel_id, request, flow_logger=None, cancel_event=None):
        self.requests.append(request)
        stdout, stderr, failure = io.StringIO(), io.StringIO(), []

        def run():
            IN_KERNEL_OP.set(True)
            try:
                with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                    namespace = self.namespaces.setdefault(request.flow_id, {})
                    namespace["__name__"] = "__main__"
                    exec(request.code, namespace)
            except BaseException as exc:
                failure.append(f"{type(exc).__name__}: {exc}")

        thread = threading.Thread(target=contextvars.copy_context().run, args=(run,))
        thread.start()
        thread.join()
        error = failure[0] if failure else None
        return ExecuteResult(success=error is None, stdout=stdout.getvalue(), stderr=stderr.getvalue(), error=error)


class LockingKernelSimManager(KernelSimManager):
    """:class:`KernelSimManager` with the real manager's per-kernel execution lock, for every kernel id.

    A call waits while another call holds its kernel's lock and returns cancelled once its ``cancel_event`` is
    set, as ``KernelManager.execute_sync`` does; a wait longer than ``LOCK_WAIT`` seconds raises
    ``TimeoutError`` instead of hanging the test. A Python Script node's call runs its code as any call does.
    """

    LOCK_WAIT = 5.0

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._locks: dict[str, threading.Lock] = {}
        self._locks_lock = threading.Lock()

    def execute_sync(self, kernel_id, request, flow_logger=None, cancel_event=None):
        with self._locks_lock:
            lock = self._locks.setdefault(kernel_id, threading.Lock())
        deadline = time.monotonic() + self.LOCK_WAIT
        while not lock.acquire(timeout=0.05):
            if cancel_event is not None and cancel_event.is_set():
                return ExecuteResult(success=False, error="Execution cancelled by user")
            if time.monotonic() > deadline:
                raise TimeoutError(f"waited {self.LOCK_WAIT}s for kernel '{kernel_id}': a deadlock")
        try:
            return super().execute_sync(kernel_id, request, flow_logger, cancel_event)
        finally:
            lock.release()


def _install_kernel_sim(monkeypatch, manager: KernelSimManager) -> None:
    import flowfile_core.kernel as kernel_package
    from flowfile_core.flowfile import flow_graph as flow_graph_module
    from flowfile_frame import notebook_kernel

    monkeypatch.setenv("FLOWFILE_MODE", "electron")
    monkeypatch.setattr(kernel_package, "get_kernel_manager", lambda: manager)
    monkeypatch.setattr(flow_graph_module, "get_kernel_manager", lambda: manager)
    monkeypatch.setattr(notebook_kernel, "transport", manager.node_result)
    monkeypatch.setattr(notebook_kernel, "run_transport", manager.node_run)
    monkeypatch.setattr(notebook_kernel, "lookup_transport", manager.lookup)


def _forget_kernel_sim() -> None:
    from flowfile_core.notebook import kernel_runner
    from flowfile_frame import notebook_kernel

    for flow_id in list(notebook_kernel._SESSIONS):
        notebook_kernel._close(flow_id)
    kernel_runner._sessions.clear()
    kernel_runner._verified.clear()
    kernel_runner._fingerprints.clear()
    kernel_runner._schemas_sent.clear()
    kernel_runner._results.clear()


@pytest.fixture
def locking_kernel_sim(monkeypatch, tmp_path):
    """A :class:`LockingKernelSimManager` as ``get_kernel_manager()`` (also for the canvas's kernel nodes)."""
    manager = LockingKernelSimManager(shared=tmp_path / "shared")
    _install_kernel_sim(monkeypatch, manager)
    yield manager
    _forget_kernel_sim()


@pytest.fixture
def kernel_sim(monkeypatch, tmp_path):
    """A :class:`KernelSimManager` as ``get_kernel_manager()``, in electron mode; the sessions are closed afterwards."""
    manager = KernelSimManager(shared=tmp_path / "shared")
    _install_kernel_sim(monkeypatch, manager)
    yield manager
    _forget_kernel_sim()
