"""The single seam where a spawned child records why its task failed.

Every worker task reports failure the same way: write a description into the shared
error ``Array`` and flip ``progress`` to ``-1``. That description is the only thing
core ever sees of the child's exception — the exception object itself dies with the
process — so core wraps it in a bare ``Exception`` and, with a bare ``str(e)`` on the
wire, every worker-offloaded failure looked like class ``Exception``. Leading the
description with the class name lets core recover the real one
(``flowfile_core/flowfile/flow_node/models.py::recover_error_class``) and shows the
user ``ComputeError: unable to find column ...`` instead of a naked message.

Stdlib-only by contract: ``funcs``, ``custom_node_runner`` and every pool member
import this module, and each spawn pays for what it imports.
"""

from multiprocessing import Array, Value

DEFAULT_ERROR_LIMIT = 1024


def describe_exception(exc: BaseException) -> str:
    """Format *exc* as ``ClassName: message``.

    Core splits the description on its first colon and keeps the head only when it is a
    plain identifier, so the class name must lead and must stay undotted — a module path
    would fail ``str.isidentifier`` and recover nothing. A message that already opens
    with its own class name is returned untouched instead of being prefixed twice.
    """
    name = type(exc).__name__
    message = str(exc)
    if not message:
        return f"{name}:"
    if message.startswith(f"{name}:"):
        return message
    return f"{name}: {message}"


def _write(error_message: Array, description: str, limit: int) -> bytes:
    payload = description.encode()[:limit]
    with error_message.get_lock():
        error_message[: len(payload)] = payload
    return payload


def record_task_failure(
    error_message: Array, progress: Value, exc: BaseException, limit: int = DEFAULT_ERROR_LIMIT
) -> bytes:
    """Record *exc* class-prefixed and signal the parent that the task failed.

    The description is written before ``progress`` flips, because the parent reads the
    buffer the moment it observes ``-1``. Callers catch ``BaseException`` so a pyo3
    ``PanicException`` is described too, and immediately re-raise anything that is not an
    ``Exception`` — the child then dies exactly as it does today, only no longer silently.
    """
    payload = _write(error_message, describe_exception(exc), limit)
    with progress.get_lock():
        progress.value = -1
    return payload


def record_task_failure_text(
    error_message: Array, progress: Value, description: str, limit: int = DEFAULT_ERROR_LIMIT
) -> bytes:
    """Record a pre-formatted *description* verbatim, with no class prefix.

    For the two catalog-edit tasks whose ``STALE_WRITE:``/``EDIT_INVALID:`` sentinels the
    parent route parses by prefix, and for the custom-node dry run, which prefixes the
    class itself and then appends a traceback.
    """
    payload = _write(error_message, description, limit)
    with progress.get_lock():
        progress.value = -1
    return payload
