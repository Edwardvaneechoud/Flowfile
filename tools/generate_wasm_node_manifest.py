"""Generate the WASM node-support manifest the share-link builder reads.

``flowfile_core.flowfile.share`` has to know which of core's node types the
in-browser Pyodide build can actually run, and which settings shapes it runs
*differently*. That answer lives in ``flowfile_wasm`` — a TypeScript palette and
a Python execution engine that never ship with flowfile_core — so it is derived
here at build time and committed as package data next to the code that reads it.

Three sources, each the single source of truth for one part of the answer:

* ``flowfile_wasm/src/config/nodeCatalog.ts`` — the palette. ``available: false``
  entries are full-app teasers (rendered locked), everything else is runnable.
* ``flowfile_wasm/src/utils/coreExport.ts`` — ``TYPE_TO_CORE``, the five node
  types the browser editor spells differently from core.
* ``flowfile_wasm/src/pyodide/engine/nodes_aggregate.py`` — the aggregation
  functions the engine implements, separately for group_by and pivot.

Every core node type must land in exactly one tier, so a node added to core with
no browser counterpart fails ``make check_wasm_node_manifest`` instead of
silently sharing as if it worked.
"""

import argparse
import ast
import json
import os
import re
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
WASM_ROOT = REPO_ROOT / "flowfile_wasm"
NODE_CATALOG = WASM_ROOT / "src" / "config" / "nodeCatalog.ts"
CORE_EXPORT = WASM_ROOT / "src" / "utils" / "coreExport.ts"
NODES_AGGREGATE = WASM_ROOT / "src" / "pyodide" / "engine" / "nodes_aggregate.py"
MANIFEST_PATH = REPO_ROOT / "flowfile_core" / "flowfile_core" / "flowfile" / "share" / "wasm_node_support.json"

# The browser Read/Write panels offer these three and nothing else
# (flowfile_wasm/src/stores/flow-store.ts read staging, engine nodes_io.py).
FILE_TYPES = ["csv", "excel", "parquet"]

EXPECTED_SUPPORTED = 23
EXPECTED_LOCKED = 16

_NODE_FIELD_RE = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)\s*:\s*")
_STRING_RE = re.compile(r"'((?:[^'\\]|\\.)*)'|\"((?:[^\"\\]|\\.)*)\"")


class ManifestError(RuntimeError):
    """The manifest could not be derived from flowfile_wasm's sources."""


def strip_comments(text: str) -> str:
    """Blank out ``//`` and ``/* */`` comments without touching string literals.

    Comments are replaced by spaces (newlines kept) so every offset in the
    result still lines up with the original file.
    """
    out: list[str] = []
    i = 0
    n = len(text)
    quote: str | None = None
    while i < n:
        ch = text[i]
        if quote:
            out.append(ch)
            if ch == "\\" and i + 1 < n:
                out.append(text[i + 1])
                i += 2
                continue
            if ch == quote:
                quote = None
            i += 1
            continue
        if ch in "'\"`":
            quote = ch
            out.append(ch)
            i += 1
            continue
        if ch == "/" and i + 1 < n and text[i + 1] == "/":
            while i < n and text[i] != "\n":
                out.append(" ")
                i += 1
            continue
        if ch == "/" and i + 1 < n and text[i + 1] == "*":
            end = text.find("*/", i + 2)
            end = n if end == -1 else end + 2
            out.append("".join(" " if c != "\n" else "\n" for c in text[i:end]))
            i = end
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def _balanced_object(text: str, start: int) -> str:
    """The ``{...}`` literal beginning at ``start``, string- and nesting-aware."""
    depth = 0
    quote: str | None = None
    i = start
    while i < len(text):
        ch = text[i]
        if quote:
            if ch == "\\":
                i += 2
                continue
            if ch == quote:
                quote = None
            i += 1
            continue
        if ch in "'\"`":
            quote = ch
        elif ch in "{[":
            depth += 1
        elif ch in "}]":
            depth -= 1
            if depth == 0:
                return text[start : i + 1]
        i += 1
    raise ManifestError(f"unbalanced literal starting at offset {start} in nodeCatalog.ts")


def _string_literals(fragment: str) -> list[str]:
    values = []
    for match in _STRING_RE.finditer(fragment):
        values.append(match.group(1) if match.group(1) is not None else match.group(2))
    return values


def _parse_node_literal(literal: str) -> dict:
    """One palette entry's flat ``{ type: 'x', inputs: 1, ... }`` object."""
    body = literal[1:-1]
    fields: dict[str, str] = {}
    keys = list(_NODE_FIELD_RE.finditer(body))
    # Only top-level keys: a key inside `keywords: [...]` cannot occur (string
    # array), so a flat scan over the literal is enough.
    for index, match in enumerate(keys):
        end = keys[index + 1].start() if index + 1 < len(keys) else len(body)
        fields[match.group(1)] = body[match.end() : end].rstrip().rstrip(",")

    def text(name: str) -> str | None:
        raw = fields.get(name)
        if raw is None:
            return None
        literals = _string_literals(raw)
        if not literals:
            raise ManifestError(f"palette entry field {name!r} is not a string literal: {raw!r}")
        return literals[0]

    def number(name: str) -> int:
        raw = fields.get(name)
        if raw is None:
            raise ManifestError(f"palette entry is missing required field {name!r}: {literal!r}")
        try:
            return int(raw.strip())
        except ValueError as exc:
            raise ManifestError(f"palette entry field {name!r} is not an integer: {raw!r}") from exc

    node_type = text("type")
    if not node_type:
        raise ManifestError(f"palette entry without a type: {literal!r}")
    return {
        "type": node_type,
        "name": text("name") or node_type,
        "inputs": number("inputs"),
        "outputs": number("outputs"),
        "available": fields.get("available", "true").strip() != "false",
        "docs_anchor": text("docsAnchor"),
    }


def parse_node_catalog(text: str) -> tuple[list[dict], list[str]]:
    """``(palette entries, SUPPORTED_NODE_TYPES)`` from nodeCatalog.ts."""
    source = strip_comments(text)

    marker = "export function createNodeCategories()"
    start = source.find(marker)
    if start == -1:
        raise ManifestError("nodeCatalog.ts has no createNodeCategories() export")
    body_start = source.find("{", start)
    body = _balanced_object(source, body_start)

    nodes: list[dict] = []
    for match in re.finditer(r"\{\s*type\s*:", body):
        nodes.append(_parse_node_literal(_balanced_object(body, match.start())))
    if not nodes:
        raise ManifestError("no palette entries found in createNodeCategories()")

    const_match = re.search(r"export const SUPPORTED_NODE_TYPES\s*=\s*\[", source)
    if not const_match:
        raise ManifestError("nodeCatalog.ts has no SUPPORTED_NODE_TYPES export")
    array = _balanced_object(source, const_match.end() - 1)
    supported = _string_literals(array)
    if not supported:
        raise ManifestError("SUPPORTED_NODE_TYPES is empty")
    return nodes, supported


def parse_type_to_core(text: str) -> dict[str, str]:
    """The five browser→core node renames from coreExport.ts."""
    source = strip_comments(text)
    match = re.search(r"const TYPE_TO_CORE\s*:[^=]*=\s*\{", source)
    if not match:
        raise ManifestError("coreExport.ts has no TYPE_TO_CORE map")
    body = _balanced_object(source, match.end() - 1)
    mapping = {}
    for entry in re.finditer(r"([A-Za-z_][A-Za-z0-9_]*)\s*:\s*'([^']+)'", body):
        mapping[entry.group(1)] = entry.group(2)
    if not mapping:
        raise ManifestError("TYPE_TO_CORE is empty")
    return mapping


def parse_capabilities(text: str) -> tuple[list[str], list[str]]:
    """``(group_by aggs, pivot aggs)`` read out of the browser engine's AST."""
    tree = ast.parse(text)

    group_by: list[str] = []
    pivot: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "_build_agg_exprs":
            for compare in ast.walk(node):
                if (
                    isinstance(compare, ast.Compare)
                    and isinstance(compare.left, ast.Name)
                    and compare.left.id == "agg"
                    and len(compare.comparators) == 1
                    and isinstance(compare.comparators[0], ast.Constant)
                    and isinstance(compare.comparators[0].value, str)
                ):
                    group_by.append(compare.comparators[0].value)
        if isinstance(node, ast.FunctionDef) and node.name == "execute_pivot":
            for assign in ast.walk(node):
                if (
                    isinstance(assign, ast.Assign)
                    and len(assign.targets) == 1
                    and isinstance(assign.targets[0], ast.Name)
                    and assign.targets[0].id == "agg_map"
                    and isinstance(assign.value, ast.Dict)
                ):
                    pivot = [
                        key.value
                        for key in assign.value.keys
                        if isinstance(key, ast.Constant) and isinstance(key.value, str)
                    ]
    if not group_by:
        raise ManifestError("no aggregation branches found in _build_agg_exprs")
    if not pivot:
        raise ManifestError("no agg_map found in execute_pivot")
    if not set(pivot) <= set(group_by):
        raise ManifestError("pivot supports an aggregation group_by does not; the manifest shape assumes a subset")
    return group_by, pivot


def _core_node_types() -> dict[str, str]:
    """``{core node type: display name}`` for the whole registry.

    Importing flowfile_core runs its startup migrations, so the caller has
    already pointed storage at a scratch directory.
    """
    from flowfile_core.configs.node_store.nodes import get_all_standard_nodes

    _, node_dict, _ = get_all_standard_nodes()
    return {item: template.name for item, template in node_dict.items()}


def build_manifest(wasm_root: Path = WASM_ROOT) -> dict:
    """Build the manifest dict from flowfile_wasm's palette, dialect and engine."""
    try:
        catalog_text = (wasm_root / "src" / "config" / "nodeCatalog.ts").read_text(encoding="utf-8")
        export_text = (wasm_root / "src" / "utils" / "coreExport.ts").read_text(encoding="utf-8")
        aggregate_text = (wasm_root / "src" / "pyodide" / "engine" / "nodes_aggregate.py").read_text(encoding="utf-8")
    except OSError as exc:
        raise ManifestError(f"could not read flowfile_wasm sources: {exc}") from exc

    palette, supported_types = parse_node_catalog(catalog_text)
    type_to_core = parse_type_to_core(export_text)
    group_by_aggs, pivot_aggs = parse_capabilities(aggregate_text)

    available = {node["type"] for node in palette if node["available"]}
    locked = {node["type"] for node in palette if not node["available"]}
    if available != set(supported_types):
        raise ManifestError(
            "SUPPORTED_NODE_TYPES disagrees with the palette's available entries: "
            f"only in the constant {sorted(set(supported_types) - available)}, "
            f"only in the palette {sorted(available - set(supported_types))}"
        )
    if len(available) != EXPECTED_SUPPORTED or len(locked) != EXPECTED_LOCKED:
        raise ManifestError(
            f"palette has {len(available)} available and {len(locked)} locked entries, "
            f"expected {EXPECTED_SUPPORTED}/{EXPECTED_LOCKED}. If the browser build really gained or "
            "lost a node, update EXPECTED_SUPPORTED/EXPECTED_LOCKED in this generator."
        )

    core_types = _core_node_types()
    nodes: dict[str, dict] = {}
    for entry in palette:
        core_type = type_to_core.get(entry["type"], entry["type"])
        if core_type not in core_types:
            raise ManifestError(
                f"palette node {entry['type']!r} maps to core type {core_type!r}, which is not in "
                "flowfile_core's node registry. Add a TYPE_TO_CORE rename, or fix the palette."
            )
        record = {
            "tier": "supported" if entry["available"] else "locked",
            "wasm_type": entry["type"],
        }
        if entry["docs_anchor"]:
            record["docs_anchor"] = entry["docs_anchor"]
        nodes[core_type] = record

    for core_type in core_types:
        nodes.setdefault(core_type, {"tier": "absent"})

    counts = {
        tier: sum(1 for record in nodes.values() if record["tier"] == tier)
        for tier in ("supported", "locked", "absent")
    }
    if set(nodes) != set(core_types):
        raise ManifestError("the manifest does not partition flowfile_core's node registry")
    if counts["supported"] != EXPECTED_SUPPORTED or counts["locked"] != EXPECTED_LOCKED:
        raise ManifestError(f"tiers came out as {counts}; two palette entries probably map to the same core type")

    return {
        "generator": "tools/generate_wasm_node_manifest.py",
        "source": ("flowfile_wasm/src/{config/nodeCatalog.ts,utils/coreExport.ts,pyodide/engine/nodes_aggregate.py}"),
        "nodes": dict(sorted(nodes.items())),
        "dialect": {core: wasm for wasm, core in sorted(type_to_core.items(), key=lambda kv: kv[1])},
        "capabilities": {
            "group_by_aggs": group_by_aggs,
            "pivot_aggs": pivot_aggs,
            "read_file_types": list(FILE_TYPES),
            "output_file_types": list(FILE_TYPES),
        },
        "counts": counts,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=MANIFEST_PATH)
    parser.add_argument("--wasm-root", type=Path, default=WASM_ROOT)
    args = parser.parse_args()

    # Importing flowfile_core creates and migrates a catalog DB; keep that out
    # of the developer's real ~/.flowfile when all we want is the node registry.
    with tempfile.TemporaryDirectory(prefix="flowfile_wasm_manifest_") as scratch:
        os.environ["FLOWFILE_STORAGE_DIR"] = scratch
        os.environ["FLOWFILE_SECURE_STORAGE_PATH"] = str(Path(scratch) / "secure")
        try:
            manifest = build_manifest(args.wasm_root)
        except ManifestError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {args.output} (node tiers: {manifest['counts']})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
