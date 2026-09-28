"""Shared fixtures for the canvas-notebook tests: the corpus and its placeholder manifest, the in-process
clean runner and a per-user ``TestClient`` factory."""

from contextlib import contextmanager

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.auth.jwt import get_current_active_user, get_current_user
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.notebook import bridge
from tests.notebook.corpus import build_corpus, demo_graph, load_expected_placeholders

NOTEBOOK_OWNER_ID = 1

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


@pytest.fixture
def runner():
    before = bridge._runner
    bridge.set_clean_runner(bridge.InProcessCleanRunner())
    yield
    bridge.set_clean_runner(before)


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
