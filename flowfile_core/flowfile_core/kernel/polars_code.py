"""Polars Code on a multi-user server: the code runs on a kernel the person owns, never in a server process.

The Polars Code sandbox (``shared.polars_code_parser``) cannot hold code that means to leave it, so on a server the
node's builder sends the code to a kernel container instead of the worker. ``kernel_script`` wraps the code in a
script that runs that same sandbox inside the kernel, so code is accepted, refused and run there as on the desktop.
``owner_kernel`` finds the person's kernel, id ``managed_kernel_id(user_id)``, and creates it on their first run.
"""

from __future__ import annotations

import asyncio
import inspect
import os
import threading
from collections.abc import Callable, Coroutine
from concurrent.futures import ThreadPoolExecutor
from functools import cache
from typing import TYPE_CHECKING, Any

from flowfile_core.kernel.models import ImageFlavour, KernelConfig, managed_kernel_id
from shared import polars_code_parser as sandbox

if TYPE_CHECKING:
    from flowfile_core.kernel.manager import KernelManager

KERNEL_NAME = "Polars Code"
CPU_VARIABLE = "FLOWFILE_POLARS_CODE_KERNEL_CPU"
MEMORY_VARIABLE = "FLOWFILE_POLARS_CODE_KERNEL_MEMORY_GB"
DEFAULT_CPU_CORES = 2.0
DEFAULT_MEMORY_GB = 4.0

_SCRIPT = """def _flowfile_polars_code():
    import types

    parser = types.ModuleType("flowfile_polars_code_parser")
    exec(compile({source!r}, "<polars_code_parser>", "exec"), parser.__dict__)
    frames = flowfile_ctx.read_inputs().get("main", [])
    result = parser.polars_code_parser.get_executable({code!r}, num_inputs=len(frames))(*frames)
    if not isinstance(result, (parser.pl.LazyFrame, parser.pl.DataFrame)):
        raise TypeError(f"Polars Code must produce a Polars DataFrame or LazyFrame, not {{type(result).__name__}}")
    flowfile_ctx.publish_output(result, name="main")


try:
    _flowfile_polars_code()
finally:
    del _flowfile_polars_code
"""


@cache
def _sandbox_source() -> str:
    """The sandbox module's source. The kernel image ships no ``shared`` package, so the script carries it."""
    return inspect.getsource(sandbox)


def kernel_script(code: str) -> str:
    """The script a kernel runs for a Polars Code node holding *code*.

    It builds the sandbox module from its source, runs *code* through ``get_executable`` over the node's inputs in
    wiring order, and publishes the result as the output ``main``. The source and *code* are embedded as string
    literals, so no text of *code* is ever part of the script itself. A kernel's environment has no
    ``FLOWFILE_MODE``, so the sandbox applies the desktop's rules there.
    """
    return _SCRIPT.format(source=_sandbox_source(), code=code)


_locks: dict[int, threading.Lock] = {}
_locks_guard = threading.Lock()


def _lock_for(user_id: int) -> threading.Lock:
    with _locks_guard:
        return _locks.setdefault(user_id, threading.Lock())


def _size(variable: str, default: float) -> float:
    """The positive number *variable* holds, else *default*."""
    try:
        value = float(os.environ.get(variable, ""))
    except ValueError:
        return default
    return value if value > 0 else default


def _wait(coroutine: Coroutine[Any, Any, Any]) -> Any:
    """Run *coroutine* to its end on a thread of its own, which works from a thread that runs an event loop too."""
    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, coroutine).result()


def owner_kernel(user_id: int | None, get_manager: Callable[[], KernelManager]) -> str:
    """The id of the kernel *user_id*'s Polars Code runs on, created when they have none yet.

    *get_manager* answers the process's kernel manager. A kernel another process created, as a scheduled run's
    does, is found through the catalog before one is created. Creating pulls the image the first time; starting
    is left to ``KernelManager.execute_sync``.

    Raises:
        RuntimeError: The step has no owner, Docker is not available, or a kernel with the person's id belongs to
            someone else.
    """
    if user_id is None:
        raise RuntimeError("This Polars Code step has no owner, so there is no kernel to run it on")
    try:
        manager = get_manager()
    except Exception as e:
        raise RuntimeError(f"On this server Polars Code runs on a kernel, and Docker is not available: {e}") from e
    kernel_id = managed_kernel_id(user_id)
    with _lock_for(user_id):
        if manager.get_kernel_sync(kernel_id) is None:
            manager.reconcile_configs_from_db(user_id)
        if manager.get_kernel_sync(kernel_id) is None:
            config = KernelConfig(
                id=kernel_id,
                name=KERNEL_NAME,
                image_flavour=ImageFlavour.LITE,
                persistence_enabled=False,
                cpu_cores=_size(CPU_VARIABLE, DEFAULT_CPU_CORES),
                memory_gb=_size(MEMORY_VARIABLE, DEFAULT_MEMORY_GB),
            )
            _wait(manager.create_kernel(config, user_id))
        if manager.get_kernel_owner(kernel_id) != user_id:
            raise RuntimeError(
                f"Kernel '{kernel_id}' is reserved for this person's Polars Code but belongs to another user; "
                "an administrator can delete it"
            )
    return kernel_id
