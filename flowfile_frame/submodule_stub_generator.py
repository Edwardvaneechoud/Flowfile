"""AST-based stub generator for flowfile_frame submodules.

Walks the flowfile_frame package directory and emits a sibling ``.pyi`` for
each ``.py``. Uses AST as the source of truth for function and class
signatures, so type annotations are copied verbatim from the source. This
avoids the runtime-introspection inconsistency where modules using
``from __future__ import annotations`` expose annotations as strings while
modules without it expose them as evaluated class objects (whose ``__name__``
loses the original module qualification).

What this generator does NOT cover:

* ``flow_frame.py`` and ``expr.py`` — these classes have methods added at
  runtime via decorators (``@add_lazyframe_methods``, ``@add_expr_methods``),
  so the dedicated generators (``flow_frame_stub_generator.py``,
  ``expr_stub_generator.py``) introspect the live classes instead.

Everything else in ``flowfile_frame/`` is handled by walking the source AST.
"""

from __future__ import annotations

import argparse
import ast
import os
import re
import sys
from pathlib import Path

PACKAGE_DIR = Path(__file__).parent / "flowfile_frame"

# Source files this generator should NOT process — handled by the dedicated
# class-specific generators because they involve runtime method injection.
HANDLED_BY_OTHER_GENERATORS = {"expr.py", "flow_frame.py"}


def _unparse(node: ast.AST) -> str:
    try:
        return ast.unparse(node)
    except Exception:  # noqa: BLE001
        return "Any"


def _is_public(name: str) -> bool:
    """Public names: don't start with `_`, plus `__init__` always allowed."""
    return name == "__init__" or not name.startswith("_")


def _render_function(node: ast.FunctionDef | ast.AsyncFunctionDef, indent: str) -> str:
    args = _unparse(node.args)
    if node.returns is not None:
        rt = _unparse(node.returns)
    else:
        rt = "Any"
    prefix = "async " if isinstance(node, ast.AsyncFunctionDef) else ""
    return f"{indent}{prefix}def {node.name}({args}) -> {rt}: ..."


def _is_property(decorators: list[ast.expr]) -> bool:
    for d in decorators:
        if isinstance(d, ast.Name) and d.id == "property":
            return True
        if isinstance(d, ast.Attribute) and d.attr == "property":
            return True
    return False


def _render_class(cls: ast.ClassDef, indent: str = "") -> list[str]:
    bases = [_unparse(b) for b in cls.bases]
    keywords = [f"{kw.arg}={_unparse(kw.value)}" for kw in cls.keywords if kw.arg]
    head_args = ", ".join(bases + keywords)
    header = f"{indent}class {cls.name}({head_args}):" if head_args else f"{indent}class {cls.name}:"

    body_indent = indent + "    "
    body: list[str] = []

    # Annotated class attributes (`name: Type` or `name: Type = default`).
    for stmt in cls.body:
        if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
            name = stmt.target.id
            if not _is_public(name):
                continue
            body.append(f"{body_indent}{name}: {_unparse(stmt.annotation)}")

    # Methods, properties, nested classes.
    for stmt in cls.body:
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if not _is_public(stmt.name):
                continue
            if _is_property(stmt.decorator_list):
                body.append(f"{body_indent}@property")
            body.append(_render_function(stmt, indent=body_indent))
        elif isinstance(stmt, ast.ClassDef) and _is_public(stmt.name):
            body.extend(_render_class(stmt, indent=body_indent))

    if not body:
        body = [f"{body_indent}..."]
    return [header, *body, ""]


_HEADER_PROVIDED_IMPORTS = frozenset(
    {
        "from __future__ import annotations",
    }
)


def _collect_imports(tree: ast.Module) -> tuple[list[str], set[str]]:
    """Imports from the source module, deduplicated.

    Flattens any ``if TYPE_CHECKING:`` block (since ``.pyi`` files are
    consumed only by type checkers). Drops ``TYPE_CHECKING`` imports — a
    ``.pyi`` is *always* type-checking time, so the gate is dead weight.

    Returns ``(import_lines, names_in_scope)`` so callers can decide whether
    to emit additional fallback imports (e.g. ``from typing import Any``).
    """
    out: list[str] = []
    seen: set[str] = set(_HEADER_PROVIDED_IMPORTS)
    in_scope: set[str] = set()

    def _add(node: ast.AST) -> None:
        rendered = _unparse(node).strip()
        # Strip TYPE_CHECKING from any `from typing import` line.
        if rendered == "from typing import TYPE_CHECKING":
            return
        rendered = rendered.replace("TYPE_CHECKING, ", "").replace(", TYPE_CHECKING", "")
        if rendered == "from typing import":
            return
        if rendered in seen:
            return
        seen.add(rendered)
        out.append(rendered)
        # Track names brought into local scope for downstream fallback logic.
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                in_scope.add(alias.asname or alias.name)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                in_scope.add(alias.asname or alias.name.split(".")[0])

    for node in tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            _add(node)
        elif isinstance(node, ast.If):
            # `if TYPE_CHECKING:` block — flatten the imports it guards.
            cond = _unparse(node.test).strip()
            if cond == "TYPE_CHECKING" or cond.endswith(".TYPE_CHECKING"):
                for child in node.body:
                    if isinstance(child, (ast.Import, ast.ImportFrom)):
                        _add(child)
    return out, in_scope


_IDENT_RE = re.compile(r"\b[A-Za-z_][\w]*\b")


def _used_identifiers(rendered_lines: list[str]) -> set[str]:
    """Bare identifier tokens that appear in the rendered stub body."""
    return set(_IDENT_RE.findall("\n".join(rendered_lines)))


def _imported_names(line: str) -> set[str]:
    """Names that an import statement brings into scope."""
    try:
        node = ast.parse(line).body[0]
    except (SyntaxError, IndexError):
        return set()
    out: set[str] = set()
    if isinstance(node, ast.ImportFrom):
        for a in node.names:
            out.add(a.asname or a.name)
    elif isinstance(node, ast.Import):
        for a in node.names:
            out.add(a.asname or a.name.split(".")[0])
    return out


def _filter_unused_imports(imports: list[str], used: set[str]) -> list[str]:
    """Drop imports whose names never appear in the stub body.

    For multi-name imports (``from foo import a, b, c``), drops individual
    unused names rather than the whole line — keeps the stub tight while
    preserving the still-referenced ones.
    """
    out: list[str] = []
    for line in imports:
        try:
            node = ast.parse(line).body[0]
        except (SyntaxError, IndexError):
            out.append(line)
            continue
        if isinstance(node, ast.ImportFrom):
            kept = [
                a for a in node.names
                if (a.asname or a.name) in used or a.name == "*"
            ]
            if not kept:
                continue
            new_node = ast.ImportFrom(module=node.module, names=kept, level=node.level)
            ast.copy_location(new_node, node)
            out.append(_unparse(new_node))
        elif isinstance(node, ast.Import):
            kept = [
                a for a in node.names
                if (a.asname or a.name.split(".")[0]) in used
            ]
            if not kept:
                continue
            new_node = ast.Import(names=kept)
            ast.copy_location(new_node, node)
            out.append(_unparse(new_node))
        else:
            # Not an import statement — pass through.
            out.append(line)
    return out


def _topo_sort_classes(class_nodes: list[ast.ClassDef]) -> list[ast.ClassDef]:
    """Order classes so that any base defined in this module appears first.

    Class headers like ``class Foo(Bar):`` evaluate ``Bar`` at definition
    time, so the base must already be known when a type checker reads the
    stub.
    """
    by_name = {c.name: c for c in class_nodes}
    placed: set[str] = set()
    ordered: list[ast.ClassDef] = []
    remaining = list(class_nodes)
    while remaining:
        progress = False
        leftover: list[ast.ClassDef] = []
        for cls in remaining:
            local_bases = {
                b.id for b in cls.bases if isinstance(b, ast.Name) and b.id in by_name
            }
            if local_bases.issubset(placed):
                ordered.append(cls)
                placed.add(cls.name)
                progress = True
            else:
                leftover.append(cls)
        remaining = leftover
        if not progress:
            ordered.extend(remaining)
            break
    return ordered


def _render_module_assigns(tree: ast.Module) -> list[str]:
    """Public top-level assignments that matter for typing.

    Captures three patterns commonly used at module scope:

    * Annotated assignments: ``__version__: str``, ``COUNT: int = 5``
    * Bare aliases: ``LazyFrame = FlowFrame``
    * ``TypeVar`` / ``ParamSpec`` / ``TypeAlias`` definitions: ``ExprT = TypeVar(...)``

    Other call-valued or import-time expressions are skipped — they don't
    contribute to the type signature.
    """
    out: list[str] = []
    typing_factories = {"TypeVar", "ParamSpec", "NewType", "TypeAliasType"}
    for stmt in tree.body:
        if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
            name = stmt.target.id
            if not _is_public(name) and not name.startswith("__"):
                continue
            out.append(f"{name}: {_unparse(stmt.annotation)}")
        elif (
            isinstance(stmt, ast.Assign)
            and len(stmt.targets) == 1
            and isinstance(stmt.targets[0], ast.Name)
        ):
            target = stmt.targets[0].id
            if not _is_public(target):
                continue
            value = stmt.value
            if isinstance(value, ast.Name):
                # Bare alias: ``LazyFrame = FlowFrame``
                out.append(f"{target} = {value.id}")
            elif isinstance(value, ast.Call):
                # ``ExprT = TypeVar("ExprT", bound="Expr")`` and friends.
                func_name = ""
                if isinstance(value.func, ast.Name):
                    func_name = value.func.id
                elif isinstance(value.func, ast.Attribute):
                    func_name = value.func.attr
                if func_name in typing_factories:
                    out.append(f"{target} = {_unparse(value)}")
    return out


def _sibling_submodule_reexports(init_path: Path) -> list[str]:
    """For an ``__init__.py``, list each sibling submodule as a re-export.

    Emits ``from . import name as name`` for every public ``.py`` file and
    every public sub-package directory that lives next to the given
    ``__init__.py``. The ``as name`` form is required so PEP-484-strict tools
    treat it as a public re-export rather than an internal alias.
    """
    pkg_dir = init_path.parent
    siblings: list[str] = []
    for child in sorted(pkg_dir.iterdir()):
        name = child.name
        if name.startswith("_"):
            continue
        if child.is_file() and child.suffix == ".py":
            siblings.append(child.stem)
        elif child.is_dir() and (child / "__init__.py").exists():
            siblings.append(name)
    return [f"from . import {n} as {n}" for n in siblings]


def _rewrite_init_imports_as_reexports(imports: list[str]) -> list[str]:
    """For ``__init__.py`` stubs, mark imports as explicit re-exports.

    PEP 484 / mypy treat ``from foo import bar`` as an internal alias unless
    you write ``from foo import bar as bar`` (or list it in ``__all__``). The
    explicit ``as`` form is the most portable way to mark a public re-export.
    """
    out: list[str] = []
    for line in imports:
        try:
            tree = ast.parse(line)
        except SyntaxError:
            out.append(line)
            continue
        node = tree.body[0] if tree.body else None
        if isinstance(node, ast.ImportFrom):
            new_names = []
            for alias in node.names:
                if alias.asname is None and alias.name != "*":
                    new_names.append(ast.alias(name=alias.name, asname=alias.name))
                else:
                    new_names.append(alias)
            new_node = ast.ImportFrom(module=node.module, names=new_names, level=node.level)
            ast.copy_location(new_node, node)
            out.append(_unparse(new_node))
        elif isinstance(node, ast.Import):
            new_names = []
            for alias in node.names:
                if alias.asname is None and "." not in alias.name:
                    new_names.append(ast.alias(name=alias.name, asname=alias.name))
                else:
                    new_names.append(alias)
            new_node = ast.Import(names=new_names)
            ast.copy_location(new_node, node)
            out.append(_unparse(new_node))
        else:
            out.append(line)
    return out


def generate_stub(src_path: Path, module_name: str) -> Path:
    """Generate a .pyi for a single source file."""
    with src_path.open(encoding="utf-8") as f:
        source = f.read()
    try:
        tree = ast.parse(source)
    except SyntaxError as e:
        raise RuntimeError(f"{src_path}: {e}") from e

    is_init = src_path.name == "__init__.py"

    imports, in_scope = _collect_imports(tree)
    if is_init:
        imports = _rewrite_init_imports_as_reexports(imports)
        # When a package has a ``.pyi`` stub, type checkers and IDEs (notably
        # PyCharm) treat that file as the authoritative namespace and stop
        # auto-discovering sibling submodules. ``import flowfile_frame`` then
        # works, but ``flowfile_frame.expr`` resolves to "Cannot find
        # reference 'expr'" even though ``expr.pyi`` is right there. Listing
        # every sibling as ``from . import name as name`` re-exposes them.
        sibling_imports = _sibling_submodule_reexports(src_path)
        if sibling_imports:
            imports = sibling_imports + imports
        # Re-export imports always bind both halves into scope.
        for line in imports:
            try:
                node = ast.parse(line).body[0]
            except (SyntaxError, IndexError):
                continue
            if isinstance(node, ast.ImportFrom):
                for a in node.names:
                    in_scope.add(a.asname or a.name)
            elif isinstance(node, ast.Import):
                for a in node.names:
                    in_scope.add(a.asname or a.name.split(".")[0])

    classes = [n for n in tree.body if isinstance(n, ast.ClassDef) and _is_public(n.name)]
    classes = _topo_sort_classes(classes)
    functions = [
        n for n in tree.body
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and _is_public(n.name)
    ]
    assigns = _render_module_assigns(tree)

    # Render the body first so we can prune imports the body doesn't reference.
    body_lines: list[str] = []
    for cls in classes:
        body_lines.extend(_render_class(cls))
    for fn in functions:
        body_lines.append(_render_function(fn, indent=""))
    body_lines.extend(assigns)

    used = _used_identifiers(body_lines)
    if is_init:
        # __init__ stubs are pure re-exports — every import is intentional.
        kept_imports = imports
    else:
        kept_imports = _filter_unused_imports(imports, used)

    needs_any = bool(body_lines) and "Any" in used and "Any" not in {
        n for line in kept_imports for n in _imported_names(line)
    }

    header = [
        f"# Auto-generated stub for {module_name} — do not edit.",
        "# Run `make stubs` to regenerate from the Python source.",
        "from __future__ import annotations",
    ]

    parts: list[str] = list(header)
    if kept_imports:
        parts.append("")
        parts.extend(kept_imports)
    if needs_any:
        parts.append("from typing import Any")

    if assigns:
        parts.append("")
        parts.extend(assigns)
    if classes:
        parts.append("")
        for cls in classes:
            parts.extend(_render_class(cls))
    if functions:
        parts.append("")
        for fn in functions:
            parts.append(_render_function(fn, indent=""))

    # For ``__init__.pyi`` stubs, also emit ``__all__``. Some PyCharm versions
    # (and a few static-analysis tools) resolve ``package.attribute`` through
    # ``__all__`` even when they ignore PEP 484's ``as X`` re-export marker.
    # Belt-and-suspenders.
    if is_init:
        exported_names = sorted(
            {
                n for line in kept_imports for n in _imported_names(line)
            }
            | {a.split(":", 1)[0].split(" =", 1)[0].strip() for a in assigns}
        )
        if exported_names:
            parts.append("")
            quoted = ", ".join(f'"{n}"' for n in exported_names)
            parts.append(f"__all__ = [{quoted}]")

    output_path = src_path.with_suffix(".pyi")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(parts) + "\n", encoding="utf-8")
    return output_path


def discover_sources(package_dir: Path) -> list[tuple[Path, str]]:
    """Walk the package directory; return [(source_path, dotted_module_name)]."""
    package_parent = package_dir.parent
    out: list[tuple[Path, str]] = []
    for src in sorted(package_dir.rglob("*.py")):
        if src.name in HANDLED_BY_OTHER_GENERATORS:
            continue
        # Skip private modules (e.g. _internal.py), but keep __init__.py.
        if src.name.startswith("_") and src.name != "__init__.py":
            continue
        rel = src.relative_to(package_parent)
        dotted = ".".join(rel.with_suffix("").parts)
        if dotted.endswith(".__init__"):
            dotted = dotted[: -len(".__init__")]
        out.append((src, dotted))
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate stubs for flowfile_frame submodules.")
    parser.add_argument(
        "--module",
        action="append",
        default=None,
        help="Restrict to one or more dotted module names. Default: every .py under the package.",
    )
    args = parser.parse_args()

    sources = discover_sources(PACKAGE_DIR)
    if args.module:
        wanted = set(args.module)
        sources = [(p, m) for (p, m) in sources if m in wanted]
        missing = wanted - {m for _, m in sources}
        for m in sorted(missing):
            print(f"WARN unknown module: {m}", file=sys.stderr)

    failures = 0
    for src, module in sources:
        try:
            out = generate_stub(src, module)
            print(f"OK   {module} -> {out}")
        except Exception as e:  # noqa: BLE001
            print(f"FAIL {module}: {type(e).__name__}: {e}", file=sys.stderr)
            failures += 1

    print(f"\nGenerated {len(sources) - failures}/{len(sources)} stubs.")
    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
