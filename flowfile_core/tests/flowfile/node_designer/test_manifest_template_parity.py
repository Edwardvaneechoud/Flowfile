"""AST-vs-exec template parity: the palette template built exec-free at scan time
(``manifest_to_template(scan_node_source(src))``) must equal the one the exec'd
class produces (``to_node_template()``) for every healthy fixture node."""
from pathlib import Path

import pytest

from flowfile_core.flowfile.node_designer.parsing import NodeSourceError, scan_node_source
from flowfile_core.flowfile.user_defined.registry import compute_node_key
from flowfile_core.flowfile.user_defined.templates import manifest_to_template
from shared.node_designer.loading import find_custom_node_class, load_node_module

CORPUS = Path(__file__).parent / "corpus"
BUNDLE_VALID = Path(__file__).parent.parent / "community_nodes" / "bundle_corpus" / "valid" / "node.py"
COMMUNITY_REPO_NODES = Path("/Users/edwardvaneechoud/flowfile_backup/flowfile-community-nodes/nodes")

HEALTHY_SOURCES = sorted(CORPUS.glob("insubset_*.py")) + [
    CORPUS / "codeonly_section_builder.py",
    CORPUS / "codeonly_dynamic_kwargs.py",
    BUNDLE_VALID,
]
if COMMUNITY_REPO_NODES.is_dir():
    HEALTHY_SOURCES += sorted(COMMUNITY_REPO_NODES.glob("*/node.py"))


@pytest.mark.parametrize("path", HEALTHY_SOURCES, ids=lambda p: f"{p.parent.name}/{p.name}")
def test_ast_template_matches_exec_template(path):
    source = path.read_text(encoding="utf-8")
    manifest = scan_node_source(source)
    node_key = compute_node_key(manifest.node_name)
    ast_template = manifest_to_template(manifest, node_key)

    module = load_node_module(source=source, module_name=f"parity_{path.parent.name}_{path.stem}")
    node_class = find_custom_node_class(module, class_name=manifest.class_name)
    exec_template = node_class().to_node_template()

    assert ast_template.model_dump() == exec_template.model_dump()


def test_broken_corpus_files_fail_scan_not_crash():
    for name, needle in [
        ("codeonly_no_class.py", "No CustomNodeBase subclass"),
        ("codeonly_two_classes.py", "Multiple CustomNodeBase subclasses"),
    ]:
        with pytest.raises(NodeSourceError, match=needle):
            scan_node_source((CORPUS / name).read_text(encoding="utf-8"))
    with pytest.raises(NodeSourceError, match="Syntax error"):
        scan_node_source((CORPUS / "codeonly_syntax_error.pytxt").read_text(encoding="utf-8"))


def test_non_literal_node_name_is_scan_error():
    source = (
        "import polars as pl\n"
        "from flowfile import node_designer as nd\n\n"
        'NAME = "Computed"\n\n\n'
        "class ComputedNode(nd.CustomNodeBase):\n"
        '    node_name: str = NAME + " Node"\n\n'
        "    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:\n"
        "        return inputs[0]\n"
    )
    with pytest.raises(NodeSourceError, match="node_name must be a string literal"):
        scan_node_source(source)


def test_manifest_lifts_palette_fields_with_safe_fallbacks():
    source = (
        "import polars as pl\n"
        "from flowfile import node_designer as nd\n\n\n"
        "class PaletteNode(nd.CustomNodeBase):\n"
        '    node_name: str = "Palette Node"\n'
        '    node_group: str = "special"\n'
        '    node_type: str = "output"\n'
        '    transform_type: str = "narrow"\n\n'
        "    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:\n"
        "        return inputs[0]\n"
    )
    manifest = scan_node_source(source)
    assert manifest.node_group == "special"
    assert manifest.node_type == "output"
    assert manifest.transform_type == "narrow"

    # invalid literals fall back to defaults instead of nuking the manifest
    bad = source.replace('"output"', '"weird"').replace('"narrow"', '"sideways"')
    manifest = scan_node_source(bad)
    assert manifest.node_name == "Palette Node"
    assert manifest.node_type == "process"
    assert manifest.transform_type == "wide"
