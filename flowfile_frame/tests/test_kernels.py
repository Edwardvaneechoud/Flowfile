"""``ff.kernels``: the current user's saved kernel definitions, read from the catalog DB without Docker.

Each test inserts real users and kernel rows through ``flowfile_core.kernel.persistence`` and
deletes them afterwards.
"""

import ast
import sys
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import polars as pl
import pytest

import flowfile_core.kernel as kernel_package
import flowfile_frame as ff
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.kernel import persistence
from flowfile_core.kernel.models import ImageFlavour
from flowfile_core.kernel.models import KernelInfo as SavedKernel
from flowfile_frame.kernels import KernelInfo, KernelLookupError
from flowfile_frame.notebook import notebook_mode
from shared.node_designer import CustomNodeBase


@pytest.fixture
def kernel_rows():
    """Users ``me``, ``other`` and ``empty``; ``me`` and ``other`` own kernels."""
    tag = uuid4().hex[:8]
    with get_db_context() as db:
        users = {
            who: db_models.User(username=f"kernels-{who}-{tag}", email=f"kernels-{who}-{tag}@example.com")
            for who in ("me", "other", "empty")
        }
        db.add_all(users.values())
        db.commit()
        ids = {who: user.id for who, user in users.items()}
    ml = SavedKernel(id=f"ml-{tag}", name="ML", packages=["scikit-learn", "xgboost"], image_flavour=ImageFlavour.ML)
    rows = [
        (ml, ids["me"]),
        (SavedKernel(id=f"base-{tag}", name="Base"), ids["me"]),
        (SavedKernel(id=f"theirs-{tag}", name="Theirs", image_flavour=ImageFlavour.LITE), ids["other"]),
    ]
    with get_db_context() as db:
        for kernel, owner in rows:
            persistence.save_kernel(db, kernel, owner)
    added = []
    yield SimpleNamespace(tag=tag, users=ids, ml=ml.id, base=f"base-{tag}", theirs=f"theirs-{tag}", added=added)
    with get_db_context() as db:
        for kernel_id in [kernel.id for kernel, _ in rows] + added:
            persistence.delete_kernel(db, kernel_id)
        db.query(db_models.User).filter(db_models.User.id.in_(ids.values())).delete(synchronize_session=False)
        db.commit()


@pytest.fixture
def saved(kernel_rows):
    """``kernel_rows`` with the frame acting as ``me``: a notebook mode for that user."""
    with notebook_mode(user_id=kernel_rows.users["me"]):
        yield kernel_rows


def test_list_is_the_current_users_kernels_sorted_by_id(saved):
    assert ff.kernels.list() == [
        KernelInfo(id=saved.base, name="Base", flavour="base", packages=[]),
        KernelInfo(id=saved.ml, name="ML", flavour="ml", packages=["scikit-learn", "xgboost"]),
    ]


def test_lookup_by_id(saved):
    info = ff.kernels.get(saved.ml)
    assert info == KernelInfo(saved.ml, "ML", "ml", ["scikit-learn", "xgboost"])
    assert ff.kernels[saved.ml] == info
    assert saved.ml in ff.kernels and saved.base in ff.kernels
    assert saved.theirs not in ff.kernels and 42 not in ff.kernels
    assert list(ff.kernels) == [saved.base, saved.ml]
    assert len(ff.kernels) == 2
    assert repr(ff.kernels) == f"fl.kernels({[saved.base, saved.ml]!r})"
    assert not hasattr(ff.kernels, "base")


def test_reads_the_db_on_every_call(saved):
    assert len(ff.kernels) == 2
    late = f"late-{saved.tag}"
    saved.added.append(late)
    with get_db_context() as db:
        persistence.save_kernel(db, SavedKernel(id=late, name="Late"), saved.users["me"])
    assert late in ff.kernels and len(ff.kernels) == 3


def test_unknown_id_names_the_users_kernels(saved):
    with pytest.raises(KernelLookupError) as raised:
        ff.kernels[saved.theirs]
    assert isinstance(raised.value, ff.NativeNodeError) and isinstance(raised.value, KeyError)
    assert str(raised.value) == (
        f"No kernel {saved.theirs!r}; your kernels are {saved.base!r}, {saved.ml!r}. "
        "Kernels are created in the Designer, on the Python Kernels page."
    )
    with pytest.raises(KernelLookupError, match="No kernel 7;"):
        ff.kernels.get(7)


def test_unknown_id_without_kernels(kernel_rows):
    with notebook_mode(user_id=kernel_rows.users["empty"]):
        assert ff.kernels.list() == [] and len(ff.kernels) == 0
        with pytest.raises(KernelLookupError) as raised:
            ff.kernels.get(kernel_rows.ml)
    assert str(raised.value) == (
        f"No kernel {kernel_rows.ml!r}; you have no kernels yet. "
        "Kernels are created in the Designer, on the Python Kernels page."
    )


def test_notebook_mode_reads_the_sessions_user(kernel_rows):
    with notebook_mode(user_id=kernel_rows.users["other"]):
        assert list(ff.kernels) == [kernel_rows.theirs]
        assert ff.kernels[kernel_rows.theirs].flavour == "lite"


def _passthrough(orders):
    return orders


def test_a_kernel_info_is_a_kernel_argument(saved):
    info = ff.kernels[saved.ml]
    placed = ff.python_script(kernel=info)(_passthrough)(ff.from_dict({"amount": [1, 2]}))
    stored = placed.flow_graph.get_node(placed.node_id).setting_input
    assert stored.python_script_input.kernel_id == saved.ml
    script = ff.PythonScript(ff.from_dict({"amount": [1, 2]}), code="x = 1", kernel=info)
    assert script.kernel == saved.ml
    assert script.node.setting_input.python_script_input.kernel_id == saved.ml


class KernelsTestScorer(CustomNodeBase):
    node_name: str = "Kernels Test Scorer"
    node_category: str = "Testing"
    environment: str = "kernel"

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        raise AssertionError("a kernel node never runs at build time")

    def predict_output_schema(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0].with_columns(pl.lit(0.0).alias("score"))


def test_a_kernel_info_places_a_kernel_custom_node(kernel_rows, store_snapshot):
    with notebook_mode(user_id=kernel_rows.users["me"]):
        kernel = ff.kernels[kernel_rows.base]
    node = ff.CustomNode(KernelsTestScorer, ff.from_dict({"amount": [1, 2]}), kernel=kernel)
    assert node.kernel == kernel_rows.base
    assert node.node.setting_input.kernel_id == kernel_rows.base
    assert node.output.columns == ["amount", "score"]


def test_listing_never_reaches_docker(saved, monkeypatch):
    """With every route to a kernel manager or Docker client raising, the API still answers from the DB."""
    import docker

    def refuse(*args, **kwargs):
        raise AssertionError("ff.kernels reached for Docker")

    monkeypatch.setattr(kernel_package, "_manager", None)
    for name in ("KernelManager", "get_kernel_manager", "get_kernel_manager_if_initialized"):
        monkeypatch.setattr(kernel_package, name, refuse)
    monkeypatch.setattr(docker, "from_env", refuse)
    monkeypatch.setattr(docker, "DockerClient", refuse)
    assert [info.id for info in ff.kernels.list()] == [saved.base, saved.ml]
    assert ff.kernels[saved.ml].packages == ["scikit-learn", "xgboost"]
    assert saved.ml in ff.kernels and len(ff.kernels) == 2 and repr(ff.kernels)
    with pytest.raises(KernelLookupError):
        ff.kernels[saved.theirs]


def test_module_never_imports_the_kernel_manager_or_docker():
    # ``flowfile_frame.kernels`` the attribute is the singleton; the module is in sys.modules.
    tree = ast.parse(Path(sys.modules["flowfile_frame.kernels"].__file__).read_text(encoding="utf-8"))
    modules, names = set(), set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules |= {alias.name for alias in node.names}
        elif isinstance(node, ast.ImportFrom):
            modules.add(node.module)
            names |= {alias.name for alias in node.names}
        elif isinstance(node, ast.Attribute):
            names.add(node.attr)
        elif isinstance(node, ast.Name):
            names.add(node.id)
    assert not {m for m in modules if m.split(".")[0] == "docker" or m == "flowfile_core.kernel.manager"}
    assert not names & {"manager", "KernelManager", "get_kernel_manager", "get_kernel_manager_if_initialized"}
