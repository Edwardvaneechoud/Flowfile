"""Local dry-run: kernel-env nodes that use the `flowfile_ctx` runtime global must
still execute (an in-memory fake context is injected since there's no Docker kernel
in CI), and the fake must behave like the real client rather than returning None.

These spawn a child `python -c` runner (the real dry-run path), so they are slower
than pure unit tests but exercise the actual execution seam.
"""

from pathlib import Path

from flowfile_core.flowfile.community_nodes.dry_run_local import run_bundle_dry_run

_CTX_NODE = '''
import polars as pl
from flowfile import node_designer as nd


class CtxNode(nd.CustomNodeBase):
    node_name: str = "Ctx Node"
    environment: str = "kernel"
    example_inputs: list = [{"a": [1, 2, 3]}]
    example_settings: dict = {}

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        flowfile_ctx.log_info("running")
        flowfile_ctx.log_warning("careful")
        flowfile_ctx.publish_progress(0.5)  # unknown method -> no-op stub
        return inputs[0].with_columns((pl.col("a") * 2).alias("b"))
'''

# Mirrors the AutoML store/reuse pair: the producer asserts on the returned id,
# the consumer relies on get_global raising on a miss.
_GLOBAL_ROUND_TRIP_NODE = '''
import polars as pl
from flowfile import node_designer as nd


class GlobalRoundTrip(nd.CustomNodeBase):
    node_name: str = "Global Round Trip"
    environment: str = "kernel"
    example_inputs: list = [{"a": [1, 2, 3]}]
    example_settings: dict = {}

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        artifact_id = flowfile_ctx.publish_global("model", {"weights": [1, 2]}, tags=["ml"])
        if artifact_id is None or artifact_id == -1:
            raise RuntimeError("Failed to persist model to the global artifact store.")
        bundle = flowfile_ctx.get_global("model")
        if bundle != {"weights": [1, 2]}:
            raise RuntimeError("get_global did not return what publish_global stored: %r" % (bundle,))
        try:
            flowfile_ctx.get_global("absent")
        except KeyError:
            pass
        else:
            raise RuntimeError("get_global on a missing artifact must raise KeyError")
        names = sorted(a.name for a in flowfile_ctx.list_global_artifacts())
        if names != ["model"]:
            raise RuntimeError("list_global_artifacts returned %r" % (names,))
        return inputs[0].with_columns(pl.lit(int(artifact_id)).alias("artifact_id"))
'''

_LOCAL_ARTIFACT_NODE = '''
import polars as pl
from flowfile import node_designer as nd


class LocalArtifacts(nd.CustomNodeBase):
    node_name: str = "Local Artifacts"
    environment: str = "kernel"
    example_inputs: list = [{"a": [1]}]
    example_settings: dict = {}

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        flowfile_ctx.publish_artifact("m", {"k": 1})
        if flowfile_ctx.read_artifact("m") != {"k": 1}:
            raise RuntimeError("read_artifact round trip failed")
        if [a.name for a in flowfile_ctx.list_artifacts()] != ["m"]:
            raise RuntimeError("list_artifacts is wrong")
        try:
            flowfile_ctx.publish_artifact("m", {"k": 2})
        except ValueError:
            pass
        else:
            raise RuntimeError("duplicate publish_artifact must raise ValueError")
        flowfile_ctx.delete_artifact("m")
        try:
            flowfile_ctx.read_artifact("m")
        except KeyError:
            pass
        else:
            raise RuntimeError("read_artifact after delete must raise KeyError")
        path = flowfile_ctx.get_shared_location("sub/out.txt")
        with open(path, "w") as f:
            f.write("ok")
        with open(path) as f:
            if f.read() != "ok":
                raise RuntimeError("get_shared_location is not writable")
        return inputs[0]
'''

_SEEDED_NODE = '''
import polars as pl
from flowfile import node_designer as nd


class SeededConsumer(nd.CustomNodeBase):
    node_name: str = "Seeded Consumer"
    environment: str = "kernel"
    example_inputs: list = [{"a": [1, 2]}]
    example_settings: dict = {}

    def example_artifacts(self) -> dict:
        return {"seeded_model": {"coef": 2}}

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        if not flowfile_ctx.is_dry_run():
            raise RuntimeError("is_dry_run() must be True in the dry run")
        bundle = flowfile_ctx.get_global("seeded_model")
        local = flowfile_ctx.read_artifact("seeded_model")
        if bundle != {"coef": 2} or local != {"coef": 2}:
            raise RuntimeError("example_artifacts was not seeded into both stores")
        return inputs[0].with_columns((pl.col("a") * bundle["coef"]).alias("scaled"))
'''

_QUALIFIED_SETTINGS_NODE = '''
import polars as pl
from flowfile import node_designer as nd


class QualifiedConsumer(nd.CustomNodeBase):
    node_name: str = "Qualified Consumer"
    environment: str = "kernel"
    example_inputs: list = [{"a": [1, 2]}]
    example_settings: dict = {"main": {"model": "General.models::seeded_model", "other": "plain"}}

    def example_artifacts(self) -> dict:
        return {"seeded_model": {"coef": 3}}

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        bundle = flowfile_ctx.get_global("General.models::seeded_model")
        if bundle != {"coef": 3}:
            raise RuntimeError("the qualified settings value was not seeded")
        if flowfile_ctx.get_global("seeded_model") != {"coef": 3}:
            raise RuntimeError("the bare name must stay seeded too")
        return inputs[0].with_columns((pl.col("a") * bundle["coef"]).alias("scaled"))
'''

# The AST lift keeps a declared `output_names = []` verbatim.
_EMPTY_OUTPUT_NAMES_NODE = '''
import polars as pl
from flowfile import node_designer as nd


class EmptyOutputNames(nd.CustomNodeBase):
    node_name: str = "Empty Output Names"
    output_names: list[str] = []
    example_inputs: list = [{"a": [1, 2, 3]}]
    example_settings: dict = {}

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0].with_columns((pl.col("a") * 2).alias("b"))
'''

_CATALOG_NODE = '''
import polars as pl
from flowfile import node_designer as nd


class CatalogNode(nd.CustomNodeBase):
    node_name: str = "Catalog Node"
    environment: str = "kernel"
    example_inputs: list = [{"a": [1]}]
    example_settings: dict = {}

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        flowfile_ctx.read_catalog_table("t")
        return inputs[0]
'''


def _dry_run(tmp_path: Path, source: str):
    (tmp_path / "node.py").write_text(source, encoding="utf-8")
    return run_bundle_dry_run(tmp_path)


def test_kernel_node_using_flowfile_ctx_dry_runs(tmp_path: Path):
    outcome = _dry_run(tmp_path, _CTX_NODE)
    assert outcome.success, outcome.error
    assert outcome.output_names == ["main"]


def test_empty_output_names_falls_back_to_main(tmp_path: Path):
    """A node declaring `output_names = []` must dry-run, not IndexError."""
    outcome = _dry_run(tmp_path, _EMPTY_OUTPUT_NAMES_NODE)
    assert outcome.success, outcome.error
    assert outcome.output_names == ["main"]


def test_global_artifact_store_round_trips(tmp_path: Path):
    """publish_global returns a real positive id and get_global reads it back."""
    outcome = _dry_run(tmp_path, _GLOBAL_ROUND_TRIP_NODE)
    assert outcome.success, outcome.error


def test_flow_local_artifact_store_matches_kernel_semantics(tmp_path: Path):
    outcome = _dry_run(tmp_path, _LOCAL_ARTIFACT_NODE)
    assert outcome.success, outcome.error


def test_example_artifacts_are_seeded_and_is_dry_run_is_true(tmp_path: Path):
    outcome = _dry_run(tmp_path, _SEEDED_NODE)
    assert outcome.success, outcome.error


def test_qualified_settings_values_are_seeded_as_aliases(tmp_path: Path):
    """A `catalog.schema::name` selector value must resolve without a live catalog."""
    outcome = _dry_run(tmp_path, _QUALIFIED_SETTINGS_NODE)
    assert outcome.success, outcome.error


def test_catalog_apis_fail_loudly_instead_of_returning_none(tmp_path: Path):
    outcome = _dry_run(tmp_path, _CATALOG_NODE)
    assert not outcome.success
    assert "read_catalog_table" in outcome.error
    assert "is_dry_run" in outcome.error


_BAD_HOOK_NODE = '''
import polars as pl
from flowfile import node_designer as nd


class BadHook(nd.CustomNodeBase):
    node_name: str = "Bad Hook"
    environment: str = "kernel"
    example_inputs: list = [{"a": [1]}]
    example_settings: dict = {}

    def example_artifacts(self):
        return [{"m": 1}]

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0]
'''


def test_example_artifacts_wrong_return_type_is_a_clear_error(tmp_path: Path):
    """Same message the kernel prologue raises, not a raw AttributeError on .items()."""
    outcome = _dry_run(tmp_path, _BAD_HOOK_NODE)
    assert not outcome.success
    assert outcome.error == "example_artifacts() must return a dict, got list"
