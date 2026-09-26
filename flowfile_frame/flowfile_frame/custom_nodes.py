"""``fl.custom_nodes``: list, look up and install custom nodes by node key."""

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
from typing import NamedTuple

from flowfile_core.configs import node_store
from flowfile_core.flowfile.node_designer.parsing import NodeSourceError, scan_node_source
from flowfile_core.flowfile.node_designer.state import NodeManifest
from flowfile_core.flowfile.user_defined.kernel_codegen import _verbatim
from flowfile_core.flowfile.user_defined.registry import (
    LoadedNode,
    compute_node_key,
    missing_custom_node_error,
    registry,
)
from flowfile_frame.custom_node import _INSTALLED_CLASSES, CustomNodeFactory
from flowfile_frame.native import NativeNodeError
from flowfile_frame.python_script import _global_names
from shared.node_designer.custom_node import CustomNodeBase, NodeSettings, node_key_for


class CustomNodeInfo(NamedTuple):
    """One entry of ``fl.custom_nodes.list()``; ``file`` is ``None`` for a session-only class."""

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
    """Source and AST of the file or notebook cell defining ``cls`` (a notebook cell via its methods' filename)."""
    filenames = []
    with contextlib.suppress(TypeError, OSError):
        filenames.append(inspect.getsourcefile(cls))
    filenames += [value.__code__.co_filename for value in vars(cls).values() if inspect.isfunction(value)]
    for filename in dict.fromkeys(name for name in filenames if name):
        text = "".join(linecache.getlines(filename))
        try:
            module = ast.parse(text)
        except SyntaxError:
            continue
        if any(isinstance(stmt, ast.ClassDef) and stmt.name == cls.__name__ for stmt in module.body):
            return text, module
    raise NativeNodeError(
        f"Cannot read the source of custom node class {cls.__name__}; save it in a .py file and install "
        "the file: fl.custom_nodes.install('path/to/node.py')"
    )


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


def _class_file_source(cls: type[CustomNodeBase]) -> str:
    """A node file for ``cls``: the class, the ``NodeSettings`` classes it uses and the imports they need.

    Any other module-level name the classes read is refused; install the module's file instead.
    """
    if cls.__qualname__ != cls.__name__:
        raise NativeNodeError(
            f"Custom node class {cls.__qualname__} is defined inside a function or class; install takes a "
            "class defined at the top level of a module, or a node file: fl.custom_nodes.install('path/to/node.py')"
        )
    text, module = _defining_module(cls)
    lines = text.splitlines()
    namespace = vars(sys.modules[cls.__module__]) if cls.__module__ in sys.modules else {}
    classes = {stmt.name: stmt for stmt in module.body if isinstance(stmt, ast.ClassDef)}
    futures = [
        _verbatim(stmt, lines)
        for stmt in module.body
        if isinstance(stmt, ast.ImportFrom) and stmt.module == "__future__"
    ]
    imports = {
        stmt: _bound_names(stmt)
        for stmt in module.body
        if isinstance(stmt, ast.Import | ast.ImportFrom)
        and not (isinstance(stmt, ast.ImportFrom) and stmt.module == "__future__")
    }
    prefix = "".join(f"{line}\n" for line in futures)
    carried: dict[str, ast.ClassDef] = {}
    pending, needed = [cls.__name__], set()
    while pending:
        name = pending.pop()
        if name in carried:
            continue
        carried[name] = classes[name]
        code = compile(prefix + _verbatim(classes[name], lines), f"<{cls.__name__} source>", "exec")
        for used in _global_names(code):
            if used in carried or hasattr(builtins, used) or (used.startswith("__") and used.endswith("__")):
                continue
            value = namespace.get(used)
            if used in classes and isinstance(value, type) and issubclass(value, NodeSettings):
                pending.append(used)
            else:
                needed.add(used)
    kept = [stmt for stmt, names in imports.items() if names & needed]
    uncovered = sorted(needed.difference(*(imports[stmt] for stmt in kept)))
    if uncovered:
        raise NativeNodeError(
            f"Custom node class {cls.__name__} reads {uncovered} from its module; install writes only the class, "
            "the NodeSettings classes it uses and top-level imports, so install the module's file instead: "
            "fl.custom_nodes.install('path/to/node.py')"
        )
    header = futures + [_verbatim(stmt, lines) for stmt in kept]
    body = [_verbatim(stmt, lines) for stmt in sorted(carried.values(), key=lambda stmt: stmt.lineno)]
    return "\n".join(header) + "\n\n\n" + "\n\n\n".join(body) + "\n"


class CustomNodes:
    """The custom nodes this process can place (``fl.custom_nodes``), by node key or display name."""

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

        A running designer shows it after Settings → Extensions → Custom Nodes → Rescan.
        """
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
            raise NativeNodeError(f"Custom node {key!r} is already installed from {holder.file_path}")
        if target.exists() and not overwrite:
            raise NativeNodeError(f"{target} already exists; pass overwrite=True to replace it")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(source, encoding="utf-8")
        entry = registry.load_file(target)
        if entry.is_broken:
            raise NativeNodeError(f"Wrote {target}, but it does not load: {entry.error}")
        node_store.CUSTOM_NODE_STORE.pop(key, None)
        if node_class is None:
            _INSTALLED_CLASSES.pop(key, None)
        else:
            _INSTALLED_CLASSES[key] = node_class
        return target

    def __getattr__(self, name: str) -> CustomNodeFactory:
        if name.startswith("_") or (name not in self and registry.get(node_key_for(name)) is None):
            raise AttributeError(missing_custom_node_error(node_key_for(name)))
        return self.get(name)

    def __getitem__(self, name: str) -> CustomNodeFactory:
        return self.get(name)

    def __contains__(self, name: object) -> bool:
        return isinstance(name, str) and node_key_for(name) in node_store.CUSTOM_NODE_STORE

    def __iter__(self) -> Iterator[str]:
        return iter(sorted(node_store.CUSTOM_NODE_STORE))

    def __len__(self) -> int:
        return len(node_store.CUSTOM_NODE_STORE)

    def __dir__(self) -> builtins.list[str]:
        return sorted(set(super().__dir__()) | {key for key in self if key.isidentifier()})

    def __repr__(self) -> str:
        return f"fl.custom_nodes({builtins.list(self)})"


custom_nodes: CustomNodes = CustomNodes()
