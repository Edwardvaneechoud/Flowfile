"""Shared fixtures for the canvas-notebook tests: the corpus and its placeholder manifest (plan section 7)."""

import pytest

from tests.notebook.corpus import build_corpus, demo_graph, load_expected_placeholders


@pytest.fixture(scope="session")
def notebook_corpus(tmp_path_factory):
    """``[(name, FlowGraph)]``: the codegen flows, the frame-built native nodes and the demo, last as ``"demo"``.

    Session scoped and built once; tests must not mutate the graphs. The demo's catalog, child flow and
    custom node stay seeded until the session ends.
    """
    corpus = build_corpus(lambda name: tmp_path_factory.mktemp(name))
    with demo_graph() as graph:
        yield corpus + [("demo", graph)]


@pytest.fixture(scope="session")
def expected_placeholders():
    """Flow name -> node ids allowed to render as placeholders; the ledger may only shrink."""
    return load_expected_placeholders()
