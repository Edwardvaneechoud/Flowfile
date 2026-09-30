"""Shared fixtures for the canvas-notebook tests: the corpus and its placeholder manifest, the production
runner and its test-only ``exec`` twin, the corpus clean-run once per runner, and a per-user ``TestClient``
factory."""

import copy
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
    import flowfile as fl

    orders = fl.from_dict({"id": [1, 2, 3], "amount": [10, 20, 30]})
    result = orders.filter(fl.col("amount") > 10).with_columns((fl.col("amount") * 2).alias("double"))
    return open_as(result.flow_graph)


@pytest.fixture
def client_as():
    def _as(user_id: int) -> TestClient:
        user = PydanticUser(username=f"nb_{user_id}", id=user_id, disabled=False, is_admin=user_id == NOTEBOOK_OWNER_ID)
        main.app.dependency_overrides[get_current_active_user] = lambda: user
        main.app.dependency_overrides[get_current_user] = lambda: user
        return TestClient(main.app)

    yield _as
    main.app.dependency_overrides.pop(get_current_active_user, None)
    main.app.dependency_overrides.pop(get_current_user, None)
