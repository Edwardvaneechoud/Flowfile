"""``ff.custom_nodes``: list, look up and install custom nodes by node key."""

from __future__ import annotations

import ast
import builtins
import contextlib
import inspect
import linecache
import os
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any, NamedTuple

from flowfile_core.configs import node_store
from flowfile_core.flowfile.node_designer.parsing import NodeSourceError, scan_node_source
from flowfile_core.flowfile.node_designer.state import NodeManifest
from flowfile_core.flowfile.user_defined.kernel_codegen import _verbatim
from flowfile_core.flowfile.user_defined.registry import (
    CustomNodeExecError,
    LoadedNode,
    compute_node_key,
    missing_custom_node_error,
    registry,
)
from flowfile_core.schemas.schemas import NodeTemplate
from flowfile_frame._console_source import console_class_source, console_import_for
from flowfile_frame.custom_node import _INSTALLED_CLASSES, CustomNodeFactory
from flowfile_frame.native import NativeNodeError
from flowfile_frame.notebook import refuse
from flowfile_frame.python_script import _global_names
from shared.node_designer.custom_node import CustomNodeBase, NodeSettings, node_key_for


class CustomNodeLookupError(NativeNodeError, AttributeError):
    """An ``ff.custom_nodes.<name>`` lookup of a node that is unknown or does not load; ``hasattr`` sees ``False``."""


class CustomNodeInfo(NamedTuple):
    """One entry of ``ff.custom_nodes.list()``; ``file`` is ``None`` for a session-only class."""

    key: str
    name: str
    category: str
    environment: str
    inputs: int
    outputs: list[str]
    file: Path | None
    error: str | None


def _file_info(entry: LoadedNode) -> CustomNodeInfo:
    manifest = entry.manifest or NodeManifest()
    return CustomNodeInfo(
        key=entry.node_key,
        name=manifest.node_name or entry.node_key,
        category=manifest.node_category,
        environment=manifest.environment.kind,
        inputs=manifest.number_of_inputs,
        outputs=list(manifest.output_names),
        file=entry.file_path,
        error=entry.load_error,
    )


def _class_info(key: str, cls: type[CustomNodeBase]) -> CustomNodeInfo:
    instance = cls()
    return CustomNodeInfo(
        key=key,
        name=instance.node_name,
        category=instance.node_category,
        environment=instance.environment,
        inputs=instance.number_of_inputs,
        outputs=list(instance.output_names or ["main"]),
        file=None,
        error=None,
    )


def _defining_module(cls: type[CustomNodeBase]) -> tuple[str, ast.Module]:
    """Source and AST of the file, notebook cell or console selection defining ``cls``.

    A notebook cell is found through its methods' filename; a console selection (PyCharm's console,
    the ``code`` module) through the fragments ``_console_source`` keeps, since it fills no ``linecache``.
    """
    filenames = []
    with contextlib.suppress(TypeError, OSError):
        filenames.append(inspect.getsourcefile(cls))
    filenames += [
        value.__code__.co_filename
        for value in vars(cls).values()
        if inspect.isfunction(value) and value.__qualname__.startswith(f"{cls.__qualname__}.")
    ]
    for filename in dict.fromkeys(name for name in filenames if name):
        text = "".join(linecache.getlines(filename))
        try:
            module = ast.parse(text)
        except SyntaxError:
            continue
        if any(isinstance(stmt, ast.ClassDef) and stmt.name == cls.__name__ for stmt in module.body):
            return text, module
    text = console_class_source(cls)
    if text is not None:
        return text, ast.parse(text)
    kind = "custom node class" if issubclass(cls, CustomNodeBase) else "class"
    raise NativeNodeError(
        f"Cannot read the source of {kind} {cls.__name__}; save it in a .py file and install "
        "the file: ff.custom_nodes.install('path/to/node.py')"
    )


def _defining_namespace(cls: type[CustomNodeBase]) -> dict[str, Any]:
    """The globals ``cls`` was defined in: its own methods' globals, else its module's (a console's ``__main__``)."""
    for value in vars(cls).values():
        if inspect.isfunction(value) and value.__qualname__.startswith(f"{cls.__qualname__}."):
            return value.__globals__
    return vars(sys.modules[cls.__module__]) if cls.__module__ in sys.modules else {}


def _import_for(name: str, value: Any, home: str) -> str | None:
    """An import statement binding ``name`` to ``value``, or ``None`` when ``value`` was not imported.

    A module binds by its name; a class or function by its own module and qualname, when that module
    is loaded, holds it under that name and is not ``home`` (the module ``name`` is read from) or a
    console's ``__main__``, whose definitions cannot be imported by a node file.
    """
    if inspect.ismodule(value):
        aliases = [key for key, module in sys.modules.items() if module is value and key != "__main__"]
        origin = min(aliases, key=len, default=value.__name__)
        return None if origin == "__main__" else f"import {origin}" + (f" as {name}" if origin != name else "")
    origin, qualname = getattr(value, "__module__", None), getattr(value, "__qualname__", None)
    if not isinstance(origin, str) or not isinstance(qualname, str) or origin in (home, "__main__"):
        return None
    if "." in qualname or origin not in sys.modules or getattr(sys.modules[origin], qualname, None) is not value:
        return None
    return f"from {origin} import {qualname}" + (f" as {name}" if qualname != name else "")


def _bound_names(stmt: ast.Import | ast.ImportFrom) -> set[str]:
    """Names a top-level import binds; none for relative or star imports."""
    if isinstance(stmt, ast.ImportFrom) and stmt.level:
        return set()
    names = set()
    for alias in stmt.names:
        if alias.asname is not None:
            names.add(alias.asname)
        elif isinstance(stmt, ast.Import):
            names.add(alias.name.split(".")[0])
        elif alias.name != "*":
            names.add(alias.name)
    return names


def _read_names(tree: ast.AST) -> set[str]:
    """Names ``tree`` reads (every ``ast.Name``, so also each attribute chain's root) minus those it binds.

    Annotations count, so a name read only in one survives ``from __future__ import annotations``.
    """
    read, bound = set(), set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            (read if isinstance(node.ctx, ast.Load) else bound).add(node.id)
        elif isinstance(node, ast.arg):
            bound.add(node.arg)
        elif isinstance(node, ast.alias):
            bound.add(node.asname or node.name.split(".")[0])
        elif isinstance(getattr(node, "name", None), str):
            bound.add(node.name)
        elif isinstance(getattr(node, "rest", None), str):
            bound.add(node.rest)
    return read - bound


class _Fragment(NamedTuple):
    """The module, notebook cell or console selection a class was defined in, parsed once."""

    lines: list[str]
    classes: dict[str, ast.ClassDef]
    imports: dict[ast.stmt, set[str]]
    futures: list[str]


def _fragment_of(cls: type, cache: dict[str, _Fragment]) -> _Fragment:
    """The fragment defining ``cls``; ``cache`` (by source text) hands classes of one selection the same object."""
    text, module = _defining_module(cls)
    if text in cache:
        return cache[text]
    lines = text.splitlines()
    cache[text] = _Fragment(
        lines=lines,
        classes={stmt.name: stmt for stmt in module.body if isinstance(stmt, ast.ClassDef)},
        imports={
            stmt: _bound_names(stmt)
            for stmt in module.body
            if isinstance(stmt, ast.Import | ast.ImportFrom)
            and not (isinstance(stmt, ast.ImportFrom) and stmt.module == "__future__")
        },
        futures=[
            _verbatim(stmt, lines)
            for stmt in module.body
            if isinstance(stmt, ast.ImportFrom) and stmt.module == "__future__"
        ],
    )
    return cache[text]


def _class_file_source(cls: type[CustomNodeBase]) -> str:
    """A node file for ``cls``: the class, the ``NodeSettings`` classes it uses and the imports they need.

    A settings class defined beside it (in the same module or console session, a selection of its own
    is fine) is written too; one imported from another module stays an import. Any other name
    the classes read must be imported, else the install is refused; install the module's file instead.
    """
    if cls.__qualname__ != cls.__name__:
        raise NativeNodeError(
            f"Custom node class {cls.__qualname__} is defined inside a function or class; install takes a "
            "class defined at the top level of a module, or a node file: ff.custom_nodes.install('path/to/node.py')"
        )
    home, namespace = cls.__module__, _defining_namespace(cls)
    carried: dict[str, tuple[ast.ClassDef, _Fragment]] = {}
    reads: dict[str, list[str]] = {}
    fragments: dict[str, _Fragment] = {}
    pending, needed = [(cls.__name__, _fragment_of(cls, fragments))], set()
    while pending:
        name, fragment = pending.pop()
        if name in carried:
            continue
        if name not in fragment.classes:
            fragment = _fragment_of(namespace[name], fragments)
        stmt = fragment.classes[name]
        carried[name] = stmt, fragment
        reads[name] = []
        prefix = "".join(f"{line}\n" for line in fragment.futures)
        code = compile(prefix + _verbatim(stmt, fragment.lines), f"<{cls.__name__} source>", "exec")
        for used in sorted(_read_names(stmt).union(_global_names(code))):
            if used in carried:
                reads[name].append(used)
                continue
            if hasattr(builtins, used) or (used.startswith("__") and used.endswith("__")):
                continue
            value = namespace.get(used)
            if (
                isinstance(value, type)
                and issubclass(value, NodeSettings)
                and value.__name__ == used
                and not _import_for(used, value, home)
            ):
                pending.append((used, fragment))
                reads[name].append(used)
            else:
                needed.add(used)
    futures = carried[cls.__name__][1].futures
    if any(fragment.futures != futures for fragment in fragments.values()):
        raise NativeNodeError(
            f"Custom node class {cls.__name__} and a NodeSettings class it uses were defined under different "
            "`from __future__` imports; define them in one selection, or install the module's file instead: "
            "ff.custom_nodes.install('path/to/node.py')"
        )
    kept = [
        (stmt, fragment)
        for fragment in fragments.values()
        for stmt, names in fragment.imports.items()
        if names & needed
    ]
    uncovered = sorted(needed.difference(*(fragment.imports[stmt] for stmt, fragment in kept)))
    # A name imported by an earlier console selection, or inside a block: the console's own import, else where it lives.
    synthesized = {
        name: console_import_for(name, namespace[name]) or _import_for(name, namespace[name], home)
        for name in uncovered
        if name in namespace
    }
    uncovered = [name for name in uncovered if synthesized.get(name) is None]
    if uncovered:
        raise NativeNodeError(
            f"Custom node class {cls.__name__} reads {uncovered} from its module; install writes only the class, "
            "the NodeSettings classes it uses and top-level imports, so install the module's file instead: "
            "ff.custom_nodes.install('path/to/node.py')"
        )
    imports = [_verbatim(stmt, fragment.lines) for stmt, fragment in kept] + sorted(filter(None, synthesized.values()))
    header = list(dict.fromkeys(futures + imports))
    body = [_verbatim(carried[name][0], carried[name][1].lines) for name in _dependency_order(cls.__name__, reads)]
    return "\n".join(header) + "\n\n\n" + "\n\n\n".join(body) + "\n"


def _dependency_order(root: str, reads: dict[str, list[str]]) -> list[str]:
    """``root`` and the classes it reads, each after the classes it reads; a cycle (only possible through
    methods) is cut where it closes."""
    ordered: dict[str, None] = {}
    visiting: set[str] = set()

    def place(name: str) -> None:
        if name in ordered or name in visiting:
            return
        visiting.add(name)
        for used in reads[name]:
            place(used)
        ordered[name] = None

    place(root)
    return list(ordered)


def _load_error(path: Path) -> str | None:
    """Why the node file at ``path`` does not load the way the designer places it (scan, then exec), else ``None``."""
    entry = registry.load_file(path)
    if entry.is_broken:
        return entry.error
    try:
        registry.ensure_class(entry)
    except CustomNodeExecError as exc:
        return str(exc)
    return None


def _restore(path: Path, previous: bytes | None, template: NodeTemplate | None, listed: bool) -> None:
    """Put back what ``path`` held before a failed install: the previous file, else no file and the key's template."""
    if previous is not None:
        path.write_bytes(previous)
        registry.load_file(path)
        return
    registry.remove_file(path)
    path.unlink(missing_ok=True)
    if listed:
        node_store.register_custom_node(template)
    elif template is not None:
        node_store.node_dict[template.item] = template


class CustomNodes:
    """The custom nodes this process can place (``ff.custom_nodes``), by node key or display name.

    A membership test or lookup that misses first picks up node files written since the last
    scan (``registry.refresh``, exec-free), so a node another process installed resolves.
    """

    def list(self) -> builtins.list[CustomNodeInfo]:
        """Installed node files (broken ones with their error), then session-only classes."""
        infos = [_file_info(entry) for entry in registry.all()]
        for key in builtins.list(node_store.CUSTOM_NODE_STORE):
            entry = registry.get(key)
            if entry is None or entry.is_broken:
                infos.append(_class_info(key, node_store.CUSTOM_NODE_STORE[key]))
        return infos

    def get(self, name: str) -> CustomNodeFactory:
        """The node's factory; a broken node file raises its load error."""
        if not isinstance(name, str):
            raise NativeNodeError(f"A custom node is named by its node key or display name, got {type(name).__name__}")
        return CustomNodeFactory(node_key_for(name))

    def install(self, node: type[CustomNodeBase] | str | os.PathLike[str], *, overwrite: bool = False) -> Path:
        """Write a node class or ``.py`` file to the user-defined nodes directory as ``<key>.py`` and register it.

        The written file must load the way the designer loads it (scanned, then executed); one that
        does not is removed again, and a file it replaced is put back. A running designer picks it up
        when it opens a flow that uses it, and lists it after Settings → Extensions → Custom Nodes → Rescan.
        """
        refuse("ff.custom_nodes.install")
        node_class = None
        if isinstance(node, type) and issubclass(node, CustomNodeBase):
            node_class, source, label = node, _class_file_source(node), f"class {node.__name__}"
        elif isinstance(node, str | os.PathLike):
            path = Path(node)
            try:
                source = path.read_text(encoding="utf-8")
            except OSError as exc:
                raise NativeNodeError(f"Cannot read custom node file {path}: {exc}") from exc
            label = str(path)
        else:
            raise NativeNodeError(
                f"install takes a CustomNodeBase subclass or the path of a node .py file, got {type(node).__name__}"
            )
        try:
            manifest = scan_node_source(source)
        except NodeSourceError as exc:
            raise NativeNodeError(f"Cannot install custom node {label}: {exc}") from exc
        key = compute_node_key(manifest.node_name)
        template = node_store.node_dict.get(key)
        if template is not None and not template.custom_node:
            raise NativeNodeError(
                f"Custom node {label} has the node key {key!r} of a built-in node; rename its node_name"
            )
        target = registry.directory / f"{key}.py"
        holder = registry.get(key)
        if holder is not None and not holder.is_broken and holder.file_path.resolve() != target.resolve():
            if holder.file_path.exists():
                raise NativeNodeError(
                    f"Custom node {key!r} is already installed from {holder.file_path}; place that node by its "
                    f"key, ff.custom_nodes[{key!r}], or delete that file and install again"
                )
            registry.remove_file(holder.file_path)
        if target.exists() and not overwrite:
            raise NativeNodeError(f"{target} already exists; pass overwrite=True to replace it")
        template = node_store.node_dict.get(key)
        listed = any(existing.item == key for existing in node_store.nodes_list)
        try:
            previous = target.read_bytes() if target.is_file() else None
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(source, encoding="utf-8")
        except OSError as exc:
            raise NativeNodeError(f"Cannot write custom node file {target}: {exc}") from exc
        error = _load_error(target)
        if error is not None:
            _restore(target, previous, template, listed)
            raise NativeNodeError(f"Cannot install custom node {label}: {target.name} does not load: {error}")
        node_store.CUSTOM_NODE_STORE.pop(key, None)
        if node_class is None:
            _INSTALLED_CLASSES.pop(key, None)
        else:
            _INSTALLED_CLASSES[key] = node_class
        return target

    def __getattr__(self, name: str) -> CustomNodeFactory:
        if name.startswith("_"):
            raise CustomNodeLookupError(missing_custom_node_error(node_key_for(name)))
        try:
            return self.get(name)
        except NativeNodeError as exc:
            raise CustomNodeLookupError(str(exc)) from exc

    def __getitem__(self, name: str) -> CustomNodeFactory:
        return self.get(name)

    def __contains__(self, name: object) -> bool:
        if not isinstance(name, str):
            return False
        key = node_key_for(name)
        if key not in node_store.CUSTOM_NODE_STORE:
            registry.refresh()
        return key in node_store.CUSTOM_NODE_STORE

    def __iter__(self) -> Iterator[str]:
        return iter(sorted(node_store.CUSTOM_NODE_STORE))

    def __len__(self) -> int:
        return len(node_store.CUSTOM_NODE_STORE)

    def __dir__(self) -> builtins.list[str]:
        return sorted(set(super().__dir__()) | {key for key in self if key.isidentifier()})

    def __repr__(self) -> str:
        return f"ff.custom_nodes({builtins.list(self)})"


custom_nodes: CustomNodes = CustomNodes()
