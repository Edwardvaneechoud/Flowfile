"""Source recovery for functions defined in a console that does not keep what it ran.

``inspect.getsource`` reads a function's text from ``linecache``. PyCharm's Python console and
the ``code`` module's consoles compile each executed selection under a pseudo-filename (``<input>``,
``<console>``) and never cache the text, so ``inspect.getsource`` raises ``OSError`` for anything
defined there. The parser raises the ``compile`` audit event with the source it is given, so a hook
records the fragments compiled under those names that could define a function, and
:func:`console_function_source` finds a function's definition among them. A selection compiled
before the hook existed (one that imports flowfile itself) is still read while it runs, from the
``code.InteractiveInterpreter.runsource`` call on the stack that both consoles run it through.

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
_hooked: bool = globals().get("_hooked", False)  # kept across a reload, which must not add a second hook


def _record(event: str, args: tuple[Any, ...]) -> None:
    """Audit hook, called for every audit event: keep each console source that could define a function."""
    if event != "compile" or len(args) != 2:
        return
    source, filename = args
    fragments = _FRAGMENTS.get(filename) if isinstance(filename, str) else None
    if (
        fragments is not None
        and isinstance(source, bytes)
        and (b"def " in source or b"lambda" in source)
        and (not fragments or fragments[-1] != source)
    ):
        fragments.append(source)


def install_hook() -> None:
    """Record the fragments a console compiles from now on; a no-op when the hook is already installed."""
    global _hooked
    if not _hooked:
        _hooked = True
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


if _in_console():
    install_hook()
