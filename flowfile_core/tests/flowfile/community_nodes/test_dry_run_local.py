"""Local dry-run: kernel-env nodes that use the `flowfile_ctx` runtime global must
still execute (a stub context is injected since there's no Docker kernel in CI).

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


def test_kernel_node_using_flowfile_ctx_dry_runs(tmp_path: Path):
    (tmp_path / "node.py").write_text(_CTX_NODE, encoding="utf-8")
    outcome = run_bundle_dry_run(tmp_path)
    assert outcome.success, outcome.error
    assert outcome.output_names == ["main"]
