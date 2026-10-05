"""The canonical node header must exec where the ``flowfile`` package is absent.

The PyInstaller sidecars bundle ``flowfile_core`` and the SDK but not the
top-level ``flowfile`` package; ``load_node_module`` aliases it onto the SDK
there and leaves a process with the real package alone.
"""

import importlib.util
import sys

import pytest

from shared import node_designer as sdk
from shared.node_designer.loading import find_custom_node_class, load_node_module

SOURCE = (
    "import polars as pl\n"
    "from flowfile import node_designer as nd\n\n\n"
    "class Passthrough(nd.CustomNodeBase):\n"
    '    node_name: str = "Alias Passthrough"\n\n'
    "    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:\n"
    "        return inputs[0]\n"
)


def _flowfile_entries() -> list[str]:
    return [name for name in sys.modules if name == "flowfile" or name.startswith("flowfile.")]


@pytest.fixture
def no_flowfile_package(monkeypatch):
    """Make the process look like a frozen sidecar: no ``flowfile`` module loaded, none findable."""
    saved = set(_flowfile_entries())
    for name in saved:
        monkeypatch.delitem(sys.modules, name)
    real_find_spec = importlib.util.find_spec
    monkeypatch.setattr(
        importlib.util,
        "find_spec",
        lambda name, package=None: None if name == "flowfile" else real_find_spec(name, package),
    )
    yield
    for name in _flowfile_entries():
        if name not in saved:
            del sys.modules[name]


def test_canonical_header_execs_without_flowfile_package(no_flowfile_package):
    module = load_node_module(source=SOURCE, module_name="alias_passthrough")
    assert module.nd is sdk
    node_class = find_custom_node_class(module)
    assert node_class().node_name == "Alias Passthrough"
    assert getattr(sys.modules["flowfile"], "__file__", None) is None


def test_other_flowfile_imports_still_refused_without_package(no_flowfile_package):
    with pytest.raises(ImportError, match="not available in this execution context"):
        load_node_module(source="from flowfile import FlowFrame\n")


def test_real_flowfile_package_is_used_when_importable():
    module = load_node_module(source=SOURCE, module_name="alias_passthrough_real")
    assert sys.modules["flowfile"].__file__
    assert module.nd.CustomNodeBase is sdk.CustomNodeBase
