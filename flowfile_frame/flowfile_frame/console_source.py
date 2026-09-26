"""Source recovery for functions defined in a console that does not keep what it ran.

``inspect.getsource`` reads a function's text from ``linecache``. PyCharm's Python console and
the ``code`` module's consoles compile each executed selection under a pseudo-filename (``<input>``,
``<console>``) and never cache the text, so ``inspect.getsource`` raises ``OSError`` for anything
defined there. The parser raises the ``compile`` audit event with the source it is given, so a hook
installed when flowfile_frame is imported records the fragments compiled under those names, and
:func:`console_function_source` finds a function's definition among them. A selection compiled
before the hook existed (one that imports flowfile itself) is still read while it runs, from the
``code.InteractiveInterpreter.runsource`` call on the stack that both consoles run it through.

The allow-list is deliberately short. The plain REPL's ``<stdin>`` raises the event without the
text, and ``<string>`` is where ``exec`` puts generated code (dataclasses, namedtuple, pydantic),
whose text is not the user's. The fragments are not put in ``linecache`` either: ``inspect`` picks a
function there by line number alone, so a stale fragment would silently return the wrong one.
"""

import __future__

import ast
import inspect
import sys
import types
import warnings
from code import InteractiveInterpreter
from collections import deque
from collections.abc import Iterator
from typing import Any

_FRAGMENTS: dict[str, deque[bytes]] = {name: deque(maxlen=500) for name in ("<input>", "<console>")}
_RUNSOURCE = InteractiveInterpreter.runsource.__code__


def _record(event: str, args: tuple[Any, ...]) -> None:
    """Audit hook, called for every audit event: keep each source compiled under a console filename."""
    if event != "compile" or len(args) != 2:
        return
    source, filename = args
    fragments = _FRAGMENTS.get(filename) if isinstance(filename, str) else None
    if fragments is not None and isinstance(source, bytes) and (not fragments or fragments[-1] != source):
        fragments.append(source)


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


def console_function_source(fn: Any) -> str | None:
    """The text of a function or lambda defined in a console, from a running or a recorded selection.

    Only functions compiled under a console filename are looked up. The running selections come
    first, then the recorded fragments newest first, and one counts only when recompiling it (under
    the function's own ``from __future__ import annotations`` flag, which an earlier fragment may
    have set) yields a code object equal to ``fn``'s, so a redefinition or an unrelated fragment
    never answers for it. A function's text runs from its first decorator to its last line, as with
    ``inspect.getsource``; a lambda's is the lambda expression. ``None`` when no fragment matches, or
    when several lambdas on the line could be ``fn``.
    """
    code = getattr(inspect.unwrap(fn), "__code__", None)
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
        if isinstance(node, ast.Lambda):
            return ast.get_source_segment(text, node)
        return "\n".join(text.split("\n")[code.co_firstlineno - 1 : node.end_lineno]) + "\n"
    return None


if "_hooked" not in globals():  # audit hooks cannot be removed, so a reload must not add a second
    _hooked = True
    sys.addaudithook(_record)
