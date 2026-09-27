"""Generate the static ``fl.`` completion source for the canvas notebook dock.

The dock's CodeMirror editor completes ``fl.<name>``, FlowFrame methods and Expr
methods without a kernel (Jedi stays kernel-only), so the candidates are derived
here at build time and committed as JSON next to the component that reads it.

Two sources:

* ``flowfile.__all__`` — the top-level names users type after ``fl.``, introspected
  at runtime. Annotations are dropped from the signatures so the output does not
  depend on how a given Python version formats typing objects.
* ``flowfile_frame/flowfile_frame/{flow_frame,expr}.pyi`` — the committed stubs
  for ``FlowFrame`` and ``Expr``, parsed with ``ast``; the generator already keeps
  one ``#`` summary line above each def, which becomes ``doc_first_line``.

Output is sorted and free of memory addresses so ``make check_fl_completions``
can diff it.
"""

import argparse
import ast
import inspect
import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
STUB_DIR = REPO_ROOT / "flowfile_frame" / "flowfile_frame"
OUTPUT_PATH = REPO_ROOT / "flowfile_frontend/src/renderer/app/components/canvasNotebook/flCompletions.json"
STUB_CLASSES = {"FlowFrame": "flow_frame.pyi", "Expr": "expr.pyi"}
_ADDRESS = re.compile(r" at 0x[0-9a-fA-F]+")


def _first_line(doc: str | None) -> str:
    for line in (doc or "").splitlines():
        if line.strip():
            return line.strip()
    return ""


def _kind(obj) -> str:
    if inspect.ismodule(obj):
        return "module"
    if inspect.isclass(obj):
        return "class"
    if callable(obj):
        return "function"
    return "constant"


def _runtime_signature(obj) -> str:
    try:
        sig = inspect.signature(obj)
    except (TypeError, ValueError):
        return ""
    params = [p.replace(annotation=inspect.Parameter.empty) for p in sig.parameters.values()]
    bare = sig.replace(parameters=params, return_annotation=inspect.Signature.empty)
    return _ADDRESS.sub("", str(bare))


def fl_entries() -> list[dict]:
    """One entry per name in ``flowfile.__all__``."""
    import flowfile

    entries = []
    for name in sorted(set(flowfile.__all__)):
        obj = getattr(flowfile, name)
        kind = _kind(obj)
        signature = _runtime_signature(obj) if kind in ("function", "class") else ""
        doc = inspect.getdoc(obj) if kind != "constant" else None
        entries.append({"name": name, "kind": kind, "signature": signature, "doc_first_line": _first_line(doc)})
    return entries


def _comment_above(lines: list[str], lineno: int) -> str:
    """The contiguous ``#`` block right above 1-based ``lineno``, first line only."""
    block = []
    i = lineno - 2
    while i >= 0 and lines[i].strip().startswith("#"):
        block.insert(0, lines[i].strip().lstrip("#").strip())
        i -= 1
    return _first_line("\n".join(block))


def _def_signature(node: ast.FunctionDef, is_property: bool) -> str:
    returns = ast.unparse(node.returns) if node.returns is not None else ""
    if is_property:
        return f": {returns}" if returns else ""
    args = node.args
    if args.posonlyargs or args.args:
        (args.posonlyargs or args.args).pop(0)
    signature = f"({ast.unparse(args)})"
    return f"{signature} -> {returns}" if returns else signature


def stub_entries(stub: Path, class_name: str) -> list[dict]:
    """Public methods, properties and annotated attributes of ``class_name`` in ``stub``."""
    source = stub.read_text(encoding="utf-8")
    lines = source.splitlines()
    cls = next(n for n in ast.parse(source).body if isinstance(n, ast.ClassDef) and n.name == class_name)
    found: dict[str, dict] = {}
    for node in cls.body:
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            name, signature, doc = node.target.id, f": {ast.unparse(node.annotation)}", ""
        elif isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            decorators = [ast.unparse(d) for d in node.decorator_list]
            if any(d.endswith((".setter", ".deleter")) for d in decorators):
                continue
            first = node.decorator_list[0].lineno if node.decorator_list else node.lineno
            name = node.name
            signature = _def_signature(node, "property" in decorators)
            doc = _comment_above(lines, first)
        else:
            continue
        if name.startswith("_") or name in found:
            continue
        found[name] = {"name": name, "signature": signature, "doc_first_line": doc}
    return [found[name] for name in sorted(found)]


def build() -> dict:
    result = {"fl": fl_entries()}
    for class_name, stub in STUB_CLASSES.items():
        result[class_name] = stub_entries(STUB_DIR / stub, class_name)
    return result


def render() -> str:
    return json.dumps(build(), indent=2, ensure_ascii=False) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH)
    args = parser.parse_args(argv)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(render(), encoding="utf-8")
    print(f"Wrote {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
