"""Generate the static ``ff.`` completion source for the notebook cell editors.

The shared cell editor (``pythonScript/notebookEditor.ts``) completes ``ff.<name>``,
a FlowFrame's methods and the ``ff.FlowGroup`` colours without a kernel, so the
candidates are derived here at build time from ``flowfile.__all__``, ``FlowFrame``
and ``schemas.GroupColor`` and committed as JSON. Annotations are dropped from the
signatures so the output does not depend on how a given Python version formats
typing objects.

Output is sorted and free of memory addresses so ``make check_fl_completions``
can diff it.
"""

import argparse
import inspect
import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_PATH = REPO_ROOT / "flowfile_frontend/src/renderer/app/components/notebook/flCompletions.json"
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


def frame_entries() -> list[dict]:
    """One entry per public method or property of ``FlowFrame``, the receiver of a canvas notebook cell."""
    import flowfile

    entries = []
    for name in sorted(n for n in dir(flowfile.FlowFrame) if not n.startswith("_")):
        attr = inspect.getattr_static(flowfile.FlowFrame, name)
        if isinstance(attr, property):
            kind, signature = "property", ""
        elif inspect.isfunction(attr):
            kind, signature = "method", re.sub(r"^\(self(?:, )?", "(", _runtime_signature(attr))
        else:
            continue
        doc = _first_line(attr.__doc__)
        entries.append({"name": name, "kind": kind, "signature": signature, "doc_first_line": doc})
    return entries


def group_colors() -> list[dict]:
    from typing import get_args

    from flowfile_core.schemas.schemas import GroupColor

    return [{"name": color} for color in sorted(get_args(GroupColor))]


def build() -> dict:
    return {"ff": fl_entries(), "frame": frame_entries(), "group_colors": group_colors()}


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
