"""Generate type stubs for thin flowfile_frame submodules.

Covers selectors, flow_frame_methods, cloud_storage/*, database/*, catalog,
kafka, lazy, utils, series, group_frame, expr_name, list_name_space.

Walks each module and emits a .pyi stub mirroring the source file path. Reuses
the same signature/type-formatting idioms as ``expr_stub_generator.py`` and
``flow_frame_stub_generator.py``; kept self-contained so a single ``python
submodule_stub_generator.py`` invocation regenerates every submodule stub.
"""

from __future__ import annotations

import argparse
import ast
import importlib
import inspect
import os
import re
from typing import Union

SUBMODULES: list[str] = [
    "flowfile_frame.selectors",
    "flowfile_frame.flow_frame_methods",
    "flowfile_frame.kafka",
    "flowfile_frame.lazy",
    "flowfile_frame.utils",
    "flowfile_frame.series",
    "flowfile_frame.group_frame",
    "flowfile_frame.expr_name",
    "flowfile_frame.list_name_space",
    "flowfile_frame.catalog",
    "flowfile_frame.cloud_storage",
    "flowfile_frame.cloud_storage.frame_helpers",
    "flowfile_frame.cloud_storage.secret_manager",
    "flowfile_frame.database",
    "flowfile_frame.database.connection_manager",
    "flowfile_frame.database.frame_helpers",
]


_POLARS_PREFIX_RE = re.compile(r"polars(?:\.[a-zA-Z_]+)*\.")
_COLLECTIONS_PREFIX_RE = re.compile(r"\bcollections\.abc\.")
_TYPING_PREFIX_RE = re.compile(r"\btyping\.")
_PATHLIB_PREFIX_RE = re.compile(r"\bpathlib\.")


def _strip_common_prefixes(s: str) -> str:
    s = _POLARS_PREFIX_RE.sub("pl.", s)
    s = _COLLECTIONS_PREFIX_RE.sub("", s)
    s = _TYPING_PREFIX_RE.sub("", s)
    s = _PATHLIB_PREFIX_RE.sub("", s)
    return s


def format_type_annotation(annotation_obj) -> str:
    if annotation_obj is None or annotation_obj is type(None):
        return "None"
    if isinstance(annotation_obj, type):
        return annotation_obj.__name__
    if isinstance(annotation_obj, str):
        m = re.match(r"<class '([^']+)'>", annotation_obj)
        if m:
            full = m.group(1)
            return "None" if full.endswith("NoneType") else full.split(".")[-1]
        return _strip_common_prefixes(annotation_obj)
    s = str(annotation_obj).replace("<class '", "").replace("'>", "")
    s = _strip_common_prefixes(s)
    return "None" if s.endswith("NoneType") else s


def format_param(param: inspect.Parameter) -> str:
    name = param.name
    if param.kind == inspect.Parameter.VAR_KEYWORD:
        return f"**{name}"
    if param.kind == inspect.Parameter.VAR_POSITIONAL:
        return f"*{name}"

    text = name
    if param.annotation is not inspect.Parameter.empty:
        ann = param.annotation
        if hasattr(ann, "__origin__") and ann.__origin__ is Union:
            parts = [format_type_annotation(a) for a in ann.__args__]
            text = f"{name}: Union[{', '.join(parts)}]"
        else:
            text = f"{name}: {format_type_annotation(ann)}"
    if param.default is not inspect.Parameter.empty:
        try:
            text = f"{text}={repr(param.default)}"
        except Exception:
            text = f"{text}=..."
    return text


def render_signature(method, name: str, indent: str) -> str | None:
    try:
        sig = inspect.signature(method)
    except (ValueError, TypeError):
        return None

    params: list[str] = []
    for i, (pname, p) in enumerate(sig.parameters.items()):
        if i == 0 and pname in ("self", "cls") and indent:
            params.append(pname)
            continue
        params.append(format_param(p))

    rt = "Any"
    if sig.return_annotation is not inspect.Parameter.empty:
        rt = format_type_annotation(sig.return_annotation)

    return f"{indent}def {name}({', '.join(params)}) -> {rt}: ..."


def collect_module_imports(module) -> list[str]:
    """Use AST to extract every import statement from the source."""
    src_path = getattr(module, "__file__", None)
    if not src_path:
        return []
    try:
        with open(src_path, encoding="utf-8") as f:
            source = f.read()
    except OSError:
        return []
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []
    out: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            try:
                out.append(ast.unparse(node))
            except Exception:
                continue
    return out


def _local_definitions(module) -> set[str]:
    """Names defined at module scope via `def` or `class` in the source file.

    Robust against `@functools.wraps` decorators that copy ``__module__`` from
    a wrapped function (e.g. ``flowfile_frame.lazy.fold`` reports its module
    as ``polars.functions.lazy`` but is locally defined here).
    """
    src_path = getattr(module, "__file__", None)
    if not src_path:
        return set()
    try:
        with open(src_path, encoding="utf-8") as f:
            tree = ast.parse(f.read())
    except (OSError, SyntaxError):
        return set()
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
    return names


def render_class(cls, name: str) -> list[str]:
    bases = [b.__name__ for b in cls.__bases__ if b is not object]
    header = f"class {name}({', '.join(bases)}):" if bases else f"class {name}:"
    out: list[str] = [header]

    body: list[str] = []
    for attr_name, attr in inspect.getmembers(cls):
        if attr_name.startswith("_") and attr_name != "__init__":
            continue
        if inspect.isfunction(attr) or inspect.ismethod(attr):
            # Only include methods defined on this class, not inherited from
            # a base in another module — those are covered by their own stub.
            if attr.__qualname__.split(".")[0] != name:
                continue
            sig = render_signature(attr, attr_name, indent="    ")
            if sig:
                body.append(sig)
        elif isinstance(attr, property) and attr.fget is not None:
            try:
                ann = inspect.signature(attr.fget).return_annotation
                rt = format_type_annotation(ann) if ann is not inspect.Parameter.empty else "Any"
            except (ValueError, TypeError):
                rt = "Any"
            body.append("    @property")
            body.append(f"    def {attr_name}(self) -> {rt}: ...")

    out.extend(body if body else ["    ..."])
    out.append("")
    return out


def generate_submodule_stub(module_name: str) -> str:
    module = importlib.import_module(module_name)
    src = getattr(module, "__file__", None)
    if not src:
        raise RuntimeError(f"Module {module_name} has no __file__")
    output_path = os.path.splitext(src)[0] + ".pyi"

    header = [
        f"# This file was auto-generated to provide type information for {module_name}",
        "# DO NOT MODIFY THIS FILE MANUALLY",
        "# Run `python flowfile_frame/submodule_stub_generator.py` to regenerate",
        "from __future__ import annotations",
        "",
        "from typing import Any, Callable, Iterable, Optional, Union",
    ]
    imports = collect_module_imports(module)
    if imports:
        seen_imports = {"from __future__ import annotations"}
        # Avoid clashing with the seed `from typing import ...` line above; keep
        # only the first occurrence of each `from typing import` chunk.
        deduped: list[str] = []
        for line in imports:
            stripped = line.strip()
            if stripped in seen_imports:
                continue
            seen_imports.add(stripped)
            deduped.append(line)
        if deduped:
            header.extend(["", *deduped])

    body: list[str] = []
    local_names = _local_definitions(module)

    # Collect local classes first so we can sort them so that bases are
    # rendered before subclasses (matters because class headers like
    # ``class Foo(Bar):`` need ``Bar`` resolvable at type-check time).
    local_classes: list[tuple[str, type]] = []
    for name, obj in inspect.getmembers(module, inspect.isclass):
        if name.startswith("_") or name not in local_names:
            continue
        local_classes.append((name, obj))

    local_class_names = {n for n, _ in local_classes}
    placed: set[str] = set()
    ordered: list[tuple[str, type]] = []
    remaining = list(local_classes)
    while remaining:
        progress = False
        next_remaining: list[tuple[str, type]] = []
        for name, cls in remaining:
            in_module_bases = {b.__name__ for b in cls.__bases__ if b.__name__ in local_class_names}
            if in_module_bases.issubset(placed):
                ordered.append((name, cls))
                placed.add(name)
                progress = True
            else:
                next_remaining.append((name, cls))
        remaining = next_remaining
        if not progress:
            # Cycle or unresolved base — emit remaining in original order to avoid hanging.
            ordered.extend(remaining)
            break

    seen: set[str] = set()
    for name, obj in ordered:
        if name in seen:
            continue
        seen.add(name)
        body.extend(render_class(obj, name))

    for name, obj in inspect.getmembers(module, inspect.isfunction):
        if name.startswith("_") or name in seen or name not in local_names:
            continue
        seen.add(name)
        sig = render_signature(obj, name, indent="")
        if sig:
            body.extend([sig, ""])

    content = "\n".join([*header, "", *body, ""])
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(content)
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate stubs for flowfile_frame submodules.")
    parser.add_argument(
        "--module",
        action="append",
        default=None,
        help="Module to process (can be repeated). Default: all known submodules.",
    )
    args = parser.parse_args()

    modules = args.module or SUBMODULES
    successes = 0
    for mod in modules:
        try:
            out = generate_submodule_stub(mod)
            print(f"OK   {mod} -> {out}")
            successes += 1
        except Exception as e:  # noqa: BLE001
            print(f"FAIL {mod}: {type(e).__name__}: {e}")
    print(f"\nGenerated {successes}/{len(modules)} stubs.")


if __name__ == "__main__":
    main()
