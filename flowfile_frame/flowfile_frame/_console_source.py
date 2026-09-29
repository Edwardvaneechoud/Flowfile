"""Source recovery for functions defined in a console that does not keep what it ran.

``inspect.getsource`` reads a function's text from ``linecache``. PyCharm's Python console and
the ``code`` module's consoles compile each executed selection under a pseudo-filename (``<input>``,
``<console>``) and never cache the text, so ``inspect.getsource`` raises ``OSError`` for anything
defined there. The parser raises the ``compile`` audit event with the source it is given, so a hook
records the fragments compiled under those names that could define a function or class or import
a name. :func:`console_function_source` finds a function's definition among them,
:func:`console_class_source` the selection that defined a class (through its methods' code objects
and the literals its body assigns), and :func:`console_import_for` the import that bound a name.
Running selections are read from the ``code.InteractiveInterpreter.runsource`` calls on the stack
that both consoles run them through, and the one running when the hook installs (a selection that
imports flowfile itself) is recorded from there too, so it stays readable after it finished.

Audit hooks cannot be removed, so importing flowfile_frame installs the hook only in an interactive
interpreter (``python -i``, a REPL, ``code.interact``), in PyCharm's console (``pydevconsole``), or
when ``FLOWFILE_CONSOLE_SOURCE`` is truthy; any other console opts in with :func:`install_hook`.

The allow-list is deliberately short. The plain REPL's ``<stdin>`` raises the event without the
text, and ``<string>`` is where ``exec`` puts generated code (dataclasses, namedtuple, pydantic),
whose text is not the user's. The fragments are not put in ``linecache`` either: ``inspect`` picks a
function there by line number alone, so a stale fragment would silently return the wrong one.
"""

import __future__

import ast
import inspect
import os
import sys
import types
import warnings
from code import InteractiveInterpreter
from collections import deque
from collections.abc import Iterator
from typing import Any

_FRAGMENTS: dict[str, deque[bytes]] = {name: deque(maxlen=500) for name in ("<input>", "<console>")}
_RUNSOURCE = InteractiveInterpreter.runsource.__code__
_TRUTHY = frozenset({"1", "true", "yes", "on"})
_MISSING = object()
_CONSOLE_MODULES = frozenset({"__main__", "__console__"})  # what a console names its namespace
_hooked: bool = globals().get("_hooked", False)  # kept across a reload, which must not add a second hook


def _record(event: str, args: tuple[Any, ...]) -> None:
    """Audit hook, called for every audit event: keep each console source that could define a function or class."""
    if event != "compile" or len(args) != 2:
        return
    source, filename = args
    fragments = _FRAGMENTS.get(filename) if isinstance(filename, str) else None
    if (
        fragments is not None
        and isinstance(source, bytes)
        and (b"def " in source or b"lambda" in source or b"class " in source or b"import " in source)
        and (not fragments or fragments[-1] != source)
    ):
        fragments.append(source)


def install_hook() -> None:
    """Record the fragments a console compiles from now on, and the selections running now; a no-op when installed."""
    global _hooked
    if not _hooked:
        _hooked = True
        for filename in _FRAGMENTS:
            for source in reversed(tuple(_running_sources(filename))):
                _record("compile", (source, filename))
        sys.addaudithook(_record)


def _in_console() -> bool:
    """Whether this is an interactive interpreter or PyCharm's console, or ``FLOWFILE_CONSOLE_SOURCE`` is set."""
    return (
        bool(sys.flags.interactive)
        or hasattr(sys, "ps1")
        or "pydevconsole" in sys.modules
        or os.environ.get("FLOWFILE_CONSOLE_SOURCE", "").strip().lower() in _TRUTHY
    )


def _running_sources(filename: str) -> Iterator[bytes]:
    """The sources ``runsource`` calls on the stack are running under ``filename``, innermost first."""
    frame = sys._getframe()
    while frame is not None:
        if frame.f_code is _RUNSOURCE and frame.f_locals.get("filename") == filename:
            source = frame.f_locals.get("source")
            if isinstance(source, str):
                yield source.encode()
        frame = frame.f_back


def _code_objects(code: types.CodeType) -> Iterator[types.CodeType]:
    yield code
    for const in code.co_consts:
        if isinstance(const, types.CodeType):
            yield from _code_objects(const)


def _candidates(tree: ast.Module, code: types.CodeType) -> list[ast.AST]:
    """The ``def`` named like ``code``, or every lambda, that starts on ``code``'s first line (decorators included)."""
    kinds = ast.Lambda if code.co_name == "<lambda>" else (ast.FunctionDef, ast.AsyncFunctionDef)
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, kinds)
        and getattr(node, "name", "<lambda>") == code.co_name
        and min([node.lineno] + [d.lineno for d in getattr(node, "decorator_list", ())]) == code.co_firstlineno
    ]


def _same_literal(literal: Any, value: Any) -> bool:
    """Whether ``value`` is ``literal`` by type and ``repr``, so ``1`` answers neither for ``1.0`` nor for ``True``."""
    return type(literal) is type(value) and repr(literal) == repr(value)


def _defaults_match(node: ast.Lambda | ast.FunctionDef | ast.AsyncFunctionDef, fn: Any) -> bool:
    """Whether ``node``'s defaults are literals equal to ``fn``'s current defaults.

    Equal code objects say nothing about defaults, so a definition that differs from ``fn``'s only
    there would otherwise answer for it. A default that is not a literal cannot be compared and
    never matches.
    """
    args = node.args
    keyword_nodes = {a.arg: d for a, d in zip(args.kwonlyargs, args.kw_defaults, strict=True) if d is not None}
    defaults = getattr(fn, "__defaults__", None) or ()
    kwdefaults = getattr(fn, "__kwdefaults__", None) or {}
    if len(args.defaults) != len(defaults) or keyword_nodes.keys() != kwdefaults.keys():
        return False
    pairs = [*zip(args.defaults, defaults, strict=True), *((d, kwdefaults[name]) for name, d in keyword_nodes.items())]
    try:
        return all(_same_literal(ast.literal_eval(d), value) for d, value in pairs)
    except Exception:  # a non-literal default, or a default whose repr fails
        return False


def console_function_source(fn: Any) -> str | None:
    """The text of a function or lambda defined in a console, from a running or a recorded selection.

    Only functions compiled under a console filename are looked up. The running selections come
    first, then the recorded fragments newest first, and one counts only when recompiling it (under
    the function's own ``from __future__ import annotations`` flag, which an earlier fragment may
    have set) yields a code object equal to ``fn``'s and its default values are literals equal to
    ``fn``'s, so a redefinition or an unrelated fragment never answers for it. A function's text runs
    from its first decorator to its last line, as with ``inspect.getsource``; a lambda's is the lambda
    expression. ``None`` when no fragment matches, when several lambdas on the line could be ``fn``,
    or when the hook was never installed and ``fn``'s selection is no longer running.
    """
    target = inspect.unwrap(fn)
    code = getattr(target, "__code__", None)
    if not isinstance(code, types.CodeType) or code.co_filename not in _FRAGMENTS:
        return None
    recorded = reversed(tuple(_FRAGMENTS[code.co_filename]))
    for fragment in (*_running_sources(code.co_filename), *recorded):
        try:
            text = fragment.decode()
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")  # the console already showed this fragment's warnings
                tree = ast.parse(text)
                candidates = _candidates(tree, code)
                if not candidates:
                    continue
                flags = code.co_flags & __future__.annotations.compiler_flag
                compiled = compile(tree, code.co_filename, "exec", flags=flags, dont_inherit=True)
        except (SyntaxError, ValueError):
            continue
        if not any(nested == code for nested in _code_objects(compiled)):
            continue
        if len(candidates) != 1:
            return None
        node = candidates[0]
        if not _defaults_match(node, target):
            continue
        if isinstance(node, ast.Lambda):
            return ast.get_source_segment(text, node)
        return "\n".join(text.split("\n")[code.co_firstlineno - 1 : node.end_lineno]) + "\n"
    return None


def console_class_source(cls: type) -> str | None:
    """The whole text of the console selection that defined ``cls``, with the imports and classes beside it.

    A fragment answers when it defines a top-level class of the same name whose body assigns only
    literals equal to ``cls``'s values (pydantic fields by their default) and, for a class with
    methods of its own, recompiling it (under the methods' ``from __future__ import annotations``
    flag) yields every method's code object, with literal defaults equal to the method's (as for a
    function). A class without methods (a ``NodeSettings`` declaration) is checked by its literals
    alone, and only when it was defined in a console namespace (``__main__``, ``__console__``), since
    nothing else ties a fragment to it; a non-literal value cannot be compared, so a redefinition
    that differs only there still answers. Running selections come first, then the
    recorded fragments newest first. ``None`` when the methods come from a file or an unlisted
    console, or when no fragment matches.
    """
    # A base's metaclass may add functions from its own file (pydantic's model_post_init); only ours identify cls.
    methods = [value for value in vars(cls).values() if isinstance(value, types.FunctionType)]
    own = [inspect.unwrap(m) for m in methods if m.__qualname__.startswith(f"{cls.__qualname__}.")]
    codes = [fn.__code__ for fn in own]
    filenames = {code.co_filename for code in codes}
    if len(filenames) > 1 or not filenames <= _FRAGMENTS.keys():
        return None
    if not codes and cls.__module__ not in _CONSOLE_MODULES:
        return None
    flags = codes[0].co_flags & __future__.annotations.compiler_flag if codes else 0
    for filename in filenames or _FRAGMENTS:
        for fragment in (*_running_sources(filename), *reversed(tuple(_FRAGMENTS[filename]))):
            try:
                text = fragment.decode()
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    tree = ast.parse(text)
                    nodes = [stmt for stmt in tree.body if isinstance(stmt, ast.ClassDef) and stmt.name == cls.__name__]
                    if not any(_literals_match(node, cls) for node in nodes):
                        continue
                    compiled = compile(tree, filename, "exec", flags=flags, dont_inherit=True)
            except (SyntaxError, ValueError):
                continue
            nested = list(_code_objects(compiled))
            if all(any(candidate == code for candidate in nested) for code in codes) and all(
                len(found := _candidates(tree, fn.__code__)) == 1 and _defaults_match(found[0], fn) for fn in own
            ):
                return text
    return None


def _literals_match(node: ast.ClassDef, cls: type) -> bool:
    """Whether every literal ``node``'s body assigns to a name equals ``cls``'s value of it, where that is known."""
    fields = getattr(cls, "model_fields", None)
    fields = fields if isinstance(fields, dict) else {}
    for stmt in node.body:
        if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name) and stmt.value is not None:
            name = stmt.target.id
        elif isinstance(stmt, ast.Assign) and len(stmt.targets) == 1 and isinstance(stmt.targets[0], ast.Name):
            name = stmt.targets[0].id
        else:
            continue
        if name in fields:
            current = fields[name].default
        elif name in vars(cls):
            current = vars(cls)[name]
        else:
            continue
        try:
            literal = ast.literal_eval(stmt.value)
        except Exception:  # not a literal, so nothing to compare
            continue
        if not _same_literal(literal, current):
            return False
    return True


def _bound_value(stmt: ast.Import | ast.ImportFrom, alias: ast.alias) -> tuple[str, Any]:
    """The name ``alias`` binds and the loaded object it binds to, ``_MISSING`` when that is not loaded."""
    if isinstance(stmt, ast.Import):
        if alias.asname is not None:
            return alias.asname, sys.modules.get(alias.name, _MISSING)
        top = alias.name.split(".")[0]
        return top, sys.modules.get(top, _MISSING)
    module = sys.modules.get(stmt.module or "") if not stmt.level else None
    value = getattr(module, alias.name, _MISSING) if module is not None else _MISSING
    if value is _MISSING and module is not None:
        value = sys.modules.get(f"{stmt.module}.{alias.name}", _MISSING)
    return alias.asname or alias.name, value


def console_import_for(name: str, value: Any) -> str | None:
    """The import statement a console selection bound ``name`` with, when it still binds ``value``.

    Running selections come first, then the recorded fragments newest first, each read from its last
    top-level statement up. Only absolute imports of already loaded modules resolve, so nothing is
    imported. The statement is rebuilt for ``name`` alone, so ``from a import b, c`` gives
    ``from a import c`` for ``c``. ``None`` when no console import of ``name`` binds ``value``.
    """
    for filename in _FRAGMENTS:
        for fragment in (*_running_sources(filename), *reversed(tuple(_FRAGMENTS[filename]))):
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    tree = ast.parse(fragment.decode())
            except (SyntaxError, ValueError):
                continue
            for stmt in reversed(tree.body):
                if not isinstance(stmt, ast.Import | ast.ImportFrom):
                    continue
                for alias in stmt.names:
                    bound, bound_to = _bound_value(stmt, alias)
                    if bound != name or bound_to is not value:
                        continue
                    if isinstance(stmt, ast.Import):
                        return ast.unparse(ast.Import(names=[alias]))
                    return ast.unparse(ast.ImportFrom(module=stmt.module, names=[alias], level=0))
    return None


if _in_console():
    install_hook()
