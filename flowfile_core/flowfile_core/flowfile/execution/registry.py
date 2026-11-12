"""Compute backends, known by id.

A backend is made by its factory once per process; ``backend`` hands out that instance from then on. Factories come
from:

- ``_BUILTIN``: core's own (``local``, ``worker``), registered on the first lookup of an id nothing registered.
- ``register_backend``, in process.
- The ``flowfile.compute_backends`` entry-point group, scanned once on the first lookup: the entry point's name is the
  backend id and it loads to the factory. An in-process registration wins a name clash, and an entry point never
  shadows a built-in id.

A configured worker, ``worker:<name>``, is a row of core's ``compute_backends`` table (``ConfiguredBackend``): its URL,
its bearer token as secret-manager ciphertext, and an optional CA certificate. Its first lookup reads the row and makes
a ``WorkerBackend`` from it. No row is a ``LookupError``. ``forget`` drops the cached instance, so the next lookup
reads the row again. A row filled from a remote (``remote_id`` set) stores no token: it signs with a short-lived one
the remote mints (``remote_servers.token``). On a server joined with keys of its own, the row's user is only whoever
last synced, so each call is signed for the person it is made for (``acting``) and a run with no such person is refused.

A process that serves (the desktop sidecar, the Docker image, the pip server) is marked by ``mark_serving``. Its
compute is the worker beside it, so its default backend is never ``local`` and a run placed on ``local`` is refused.
The pip server has no worker beside it: in single-file mode the built-in ``worker`` is the worker embedded in this
process. A headless runner (``flowfile run flow``, the ``--run-flow`` verb) runs its flow inside ``headless_run``,
where the worker embedded in it runs each job in a child.

This is the one module outside ``configs/settings.py`` that reads ``OFFLOAD_TO_WORKER`` and ``WORKER_URL``. Every
other module asks the registry (``default_backend_id``, ``worker_url``, ``child_env``) or the backend it holds.
"""

from __future__ import annotations

import importlib.metadata
import os
import signal
import sys
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from functools import partial
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

from flowfile_core.configs import settings
from shared import server_extension

if TYPE_CHECKING:
    from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
    from flowfile_core.flowfile.execution.protocol import ComputeBackend

BackendFactory = Callable[[], "ComputeBackend"]

ENTRY_POINT_GROUP = "flowfile.compute_backends"
BACKEND_ENV_VAR = "FLOWFILE_COMPUTE_BACKEND"
LEGACY_OFFLOAD_ENV_VAR = "FLOWFILE_OFFLOAD_TO_WORKER"
LOCAL_BACKEND_ID = "local"
WORKER_BACKEND_ID = "worker"
CONFIGURED_WORKER_PREFIX = "worker:"
HEADLESS_POOL_SIZE = 1
"""Warm children a headless runner keeps: one, so a scheduled run holds at most one idle interpreter. Jobs the pool
does not take, or that start while its member is busy, spawn a child of their own."""


def _local_backend() -> ComputeBackend:
    """The worker embedded in this process.

    In a process that neither serves nor runs a flow headless (a flow built in code, a test) its jobs run in this
    process (``spawner.in_process``). In one that serves they run in the worker's pool or a child, never in the serving
    process, and so do a headless runner's (``headless_run``), its pool cut to ``HEADLESS_POOL_SIZE`` unless
    ``FLOWFILE_WORKER_POOL_SIZE`` sets one; the size saved on the Backends page is the server's.
    """
    from flowfile_core.flowfile.execution.backends.embedded import EmbeddedWorkerBackend
    from flowfile_worker import pool, spawner

    if _headless:
        if not pool.env_override_active():
            pool.task_pool.resize(HEADLESS_POOL_SIZE)
    elif not _serving:
        spawner.in_process = True
    return EmbeddedWorkerBackend()


def _worker_backend() -> ComputeBackend:
    """The worker at ``settings.WORKER_URL``; in single-file mode, the worker embedded in this process.

    With neither ``FLOWFILE_WORKER_URL`` nor ``WORKER_HOST`` set it is the worker beside core, which sees core's files,
    so it is made with ``shares_core_disk``. At a configured address the worker declares that itself.
    """
    if settings.SINGLE_FILE_MODE:
        embedded = _local_backend()
        embedded.id = WORKER_BACKEND_ID
        return embedded
    from flowfile_core.flowfile.execution.backends.worker import WorkerBackend

    return WorkerBackend(url=settings.WORKER_URL, shares_core_disk=not settings.WORKER_ADDRESS_CONFIGURED)


def _configured_worker_backend(backend_id: str, hub_token: str | None = None) -> ComputeBackend:
    """The ``WorkerBackend`` the ``compute_backends`` row of ``backend_id`` configures.

    It signs with the row's token, or for a remote-filled row with a minted one, trusts the row's CA certificate, and
    offers ``file`` when the row is marked ``shares_core_disk``. A remote-filled row is another machine, so it is never
    offered ``file`` on its own word (``another_machine``).

    Raises:
        LookupError: No row configures ``backend_id``, its token no longer decrypts (the master key changed), or no
            token could be minted for a remote-filled row.
    """
    from cryptography.fernet import InvalidToken

    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import ConfiguredBackend
    from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
    from flowfile_core.secret_manager.secret_manager import decrypt_secret

    with get_db_context() as db:
        row = db.get(ConfiguredBackend, backend_id)
        if row is None:
            raise LookupError(f"Unknown compute backend {backend_id!r}; no compute_backends row has that id")
        url, ciphertext, ca_cert, shares_core_disk = row.url, row.token, row.ca_cert, bool(row.shares_core_disk)
        hub_row = (row.account_user_id, row.remote_id) if row.remote_id is not None else None
    token_source = None
    if hub_row is not None:
        through_hub = server_extension.joined() is not None
        token = hub_token or _hub_token(backend_id, *hub_row, through_hub=through_hub)
        _hub_rows[backend_id] = hub_row
        if through_hub:
            _through_hub.add(backend_id)
            token_source = partial(_hub_token, backend_id, *hub_row, through_hub=True)
    else:
        try:
            token = decrypt_secret(ciphertext).get_secret_value()
        except (InvalidToken, ValueError) as e:
            raise LookupError(
                f"Compute backend {backend_id!r} cannot be used: its stored token does not decrypt with this core's "
                "master key. Remove it and add it again on the Backends page"
            ) from e
    return WorkerBackend(
        url=url,
        backend_id=backend_id,
        token=token,
        ca_cert=ca_cert,
        shares_core_disk=shares_core_disk,
        token_source=token_source,
        another_machine=hub_row is not None,
    )


def _hub_token(backend_id: str, account_user_id: int, remote_id: int, *, through_hub: bool = False) -> str:
    """The token minted for ``backend_id``'s server: the row's user's, or with ``through_hub`` the caller's."""
    name = backend_id.removeprefix(CONFIGURED_WORKER_PREFIX)
    try:
        return remote_servers.token(name, account_user_id, remote_id, through_hub)
    except LookupError as e:
        raise LookupError(f"Compute backend {backend_id!r} cannot be used: {e}") from e


def _replacement_token(backend_id: str, instance: ComputeBackend) -> str | None:
    """The current minted token for a remote-filled backend when ``instance`` signs with another, else ``None``.

    A backend that signs each call for its person (``_through_hub``) is never made anew; asking here only makes a
    lookup for a person who can mint none raise.
    """
    hub_row = _hub_rows.get(backend_id)
    if hub_row is None:
        return None
    if backend_id in _through_hub:
        _hub_token(backend_id, *hub_row, through_hub=True)
        return None
    token = _hub_token(backend_id, *hub_row)
    return None if getattr(instance, "token", None) == token else token


_BUILTIN: dict[str, BackendFactory] = {
    LOCAL_BACKEND_ID: _local_backend,
    WORKER_BACKEND_ID: _worker_backend,
}
"""Backend id -> factory of the backends core ships; each imports its backend's module on first use."""

_lock = threading.RLock()
_factories: dict[str, BackendFactory] = {}
_instances: dict[str, ComputeBackend] = {}
_hub_rows: dict[str, tuple[int, int]] = {}
_through_hub: set[str] = set()
_acting: ContextVar[int | None] = ContextVar("acting_user", default=None)
_scheduled: ContextVar[bool] = ContextVar("scheduled_run", default=False)
remote_servers = SimpleNamespace(
    refresh=lambda backend_ids: None,
    sync=lambda user_id: [],
    server_for=lambda user_id: None,
    run_refused=lambda on_servers: None,
)
"""What the remote that fills ``compute_backends`` rows answers, each set by its client at import: ``token(name,
account_user_id, remote_id, through)`` a call's token for its server ``name``; ``refresh(backend_ids)`` makes the rows
follow its list before a run is placed; ``sync(user_id)`` makes them follow ``user_id``'s list before they are listed,
answering the ids it wrote; ``server_for(user_id)`` the backend id of the server a moved step goes to;
``run_refused(on_servers)`` the run's nodes on its servers that it mints no token for, each with its sentence."""
_entry_points_scanned = False
_default_override: str | None = None
_serving = False
_headless = False


@contextmanager
def acting_for(user_id: int | None) -> Iterator[None]:
    """Make ``user_id`` the person the calls inside the block are made for."""
    token = _acting.set(user_id)
    try:
        yield
    finally:
        _acting.reset(token)


def acting() -> int | None:
    """The person ``acting_for`` set for the calls made now; ``None`` outside such a block."""
    return _acting.get()


@contextmanager
def for_run_record(run_id: int | None) -> Iterator[None]:
    """Mark a schedule's run (``scheduled``): no person is behind it to mint a remote's tokens with."""
    from shared.run_completion import get_run_schedule_id

    token = _scheduled.set(run_id is not None and get_run_schedule_id(run_id) is not None)
    try:
        yield
    finally:
        _scheduled.reset(token)


def scheduled() -> bool:
    """Whether the calls made now are a schedule's run (``for_run_record``)."""
    return _scheduled.get()


class Refused(LookupError):
    """No token for a call to one of the remote's servers; ``shown`` is the sentence a refused call shows."""


def refusal(error: BaseException) -> str | None:
    """The sentence of the ``Refused`` somewhere in ``error``'s chain; else ``None``."""
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        if isinstance(current, Refused):
            return current.shown
        seen.add(id(current))
        current = current.__cause__ or current.__context__
    return None


def register_backend(backend_id: str, factory: BackendFactory) -> None:
    """Make ``backend_id`` resolve through ``factory``.

    Registering an id again replaces its factory and drops the instance the previous one made.
    """
    with _lock:
        _factories[backend_id] = factory
        _instances.pop(backend_id, None)


def backend(backend_id: str) -> ComputeBackend:
    """The process's instance of ``backend_id``, made by its factory on first use.

    A ``worker:`` id with no factory is made from its ``compute_backends`` row, and made again when its minted token
    was replaced (``_replacement_token``).

    Raises:
        LookupError: No factory is known for ``backend_id`` (the message lists the known ids), or a ``worker:`` id
            has no row, a token that no longer decrypts, or no minted token.
    """
    with _lock:
        instance = _instances.get(backend_id)
        if instance is not None:
            replacement = _replacement_token(backend_id, instance)
            if replacement is None:
                return instance
            instance = _instances[backend_id] = _configured_worker_backend(backend_id, replacement)
            return instance
        if backend_id not in _factories and backend_id in _BUILTIN:
            _factories[backend_id] = _BUILTIN[backend_id]
        _scan_entry_points()
        factory = _factories.get(backend_id)
        if factory is not None:
            instance = factory()
        elif backend_id.startswith(CONFIGURED_WORKER_PREFIX):
            instance = _configured_worker_backend(backend_id)
        else:
            known = ", ".join(repr(known_id) for known_id in sorted(_factories.keys() | _BUILTIN.keys())) or "none"
            raise LookupError(f"Unknown compute backend {backend_id!r}; known backends: {known}")
        _instances[backend_id] = instance
        return instance


def forget(backend_id: str) -> None:
    """Drop the process's instance of ``backend_id``; the next lookup makes it again, or fails once its row is gone."""
    with _lock:
        _instances.pop(backend_id, None)
        _hub_rows.pop(backend_id, None)
        _through_hub.discard(backend_id)


def hub_server(backend_id: str | None) -> str | None:
    """The hub's name of the server ``backend_id`` was made from, when this process made it from a hub-filled row."""
    with _lock:
        if backend_id is None or backend_id not in _hub_rows:
            return None
    return backend_id.removeprefix(CONFIGURED_WORKER_PREFIX)


def server_name(backend_id: str) -> str | None:
    """The name the remote gives the server ``backend_id`` runs on, which it seals a run's credentials to; ``None``
    when the backend is not one of the remote's servers.

    On a server joined with keys of its own, the embedded ``local`` and the built-in ``worker`` are this server, by
    the name it joined with; a ``worker:<name>`` row filled from the remote is its server ``<name>``.
    """
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import ConfiguredBackend

    if backend_id in (LOCAL_BACKEND_ID, WORKER_BACKEND_ID):
        joined = server_extension.joined()
        return joined.name if joined is not None else None
    if not backend_id.startswith(CONFIGURED_WORKER_PREFIX):
        return None
    with get_db_context() as db:
        row = db.get(ConfiguredBackend, backend_id)
        filled = row is not None and row.remote_id is not None
    return backend_id.removeprefix(CONFIGURED_WORKER_PREFIX) if filled else None


def default_backend_id() -> str:
    """The id of the default backend, the one a ``"remote"`` flow and a call without a graph use.

    The override ``set_default_override`` set, when there is one; else ``FLOWFILE_COMPUTE_BACKEND`` when set to a
    non-empty value; else ``"worker"`` while ``OFFLOAD_TO_WORKER`` is on and ``"local"`` when it is off. In a process
    that serves (``mark_serving``) it is ``"worker"`` wherever that order gives ``"local"``.
    """
    backend_id = _default_override
    if backend_id is None:
        backend_id = environment_backend_id() or (WORKER_BACKEND_ID if settings.OFFLOAD_TO_WORKER else LOCAL_BACKEND_ID)
    if _serving and backend_id == LOCAL_BACKEND_ID:
        return WORKER_BACKEND_ID
    return backend_id


def default_backend() -> ComputeBackend:
    """The instance of the default backend."""
    return backend(default_backend_id())


def environment_backend_id() -> str | None:
    """The backend id ``FLOWFILE_COMPUTE_BACKEND`` names, or ``None`` when it is unset or empty.

    The headless runners read it after ``--backend`` and the flow's saved backend id and before ``local``, so a
    deployment that sets it for core, as the compose does, sends its scheduled runs to that backend.
    """
    return os.environ.get(BACKEND_ENV_VAR, "").strip() or None


def fallback_backend_id() -> str:
    """The id of the backend a node goes to when the one its settings name cannot run it.

    The default backend in a process that serves. In one that does not it is ``"local"``, since a headless runner
    makes its run's own backend the default (``set_default_override``).
    """
    return default_backend_id() if _serving else LOCAL_BACKEND_ID


def set_default_override(backend_id: str | None) -> str | None:
    """Make ``backend_id`` this process's default backend, ahead of ``FLOWFILE_COMPUTE_BACKEND`` and the flag.

    The headless flow runners set it for their run; ``None`` removes it. Returns the override this call replaced
    (``None`` for none), which passed back undoes the call.
    """
    global _default_override
    with _lock:
        previous, _default_override = _default_override, backend_id
    return previous


def default_override() -> str | None:
    """The override ``set_default_override`` set, or ``None`` when the default follows the environment and the flag."""
    return _default_override


def mark_serving() -> None:
    """Mark this process as one that serves: the desktop sidecar, the Docker image, the pip server.

    Its entry point calls it before the server starts. From then on the default backend is never ``"local"``, a run
    placed on ``local`` is refused (``FlowGraph._place_run``), and the ``local`` factory leaves the embedded worker's
    jobs to its pool. Nothing unmarks a process.
    """
    global _serving
    _serving = True


def serving() -> bool:
    """Whether this process serves (``mark_serving``)."""
    return _serving


@contextmanager
def headless_run() -> Iterator[None]:
    """Run a flow headless; ``flowfile run flow`` and the ``--run-flow`` verb enter it before any backend is looked up.

    Inside it the worker embedded in this process runs each job in a child (``_local_backend``), so a job has the task
    time limit, a node can be cancelled and a child that runs out of memory fails its node, not the run. The children
    post no node log to core (``flow_logger.HEADLESS_RUN_ENV_VAR``): a run outside core has none to post to, and core
    does not know the run's flow ids; each line still goes to the runner's stderr, which is a scheduled run's log.

    A SIGTERM, which is how ``FlowRunService.cancel_run`` cancels a scheduled run, terminates the run's children and
    ends the process at once, as it ended it before. On any other way out the pool is shut and every child the run
    started is terminated, as the worker's lifespan does. The mark, the variable and the SIGTERM handler are put back
    when it ends, so a process that called a runner (a test) is as it was.
    """
    global _headless
    from flowfile_worker import flow_logger, mp_context

    started_before = set(mp_context.active_children())
    previous_headless, _headless = _headless, True
    previous_variable = os.environ.get(flow_logger.HEADLESS_RUN_ENV_VAR)
    os.environ[flow_logger.HEADLESS_RUN_ENV_VAR] = "1"
    on_main_thread = threading.current_thread() is threading.main_thread()
    if on_main_thread:
        previous_handler = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, lambda signum, frame: _end_on_signal(signum, started_before))
    try:
        yield
    finally:
        pool = sys.modules.get("flowfile_worker.pool")
        if pool is not None:
            pool.task_pool.shutdown()
        _terminate_children(started_before)
        if on_main_thread:
            signal.signal(signal.SIGTERM, previous_handler)
        if previous_variable is None:
            os.environ.pop(flow_logger.HEADLESS_RUN_ENV_VAR, None)
        else:
            os.environ[flow_logger.HEADLESS_RUN_ENV_VAR] = previous_variable
        _headless = previous_headless


def headless() -> bool:
    """Whether this process is running a flow headless (``headless_run``)."""
    return _headless


def _terminate_children(started_before: set) -> None:
    """Terminate and join every multiprocessing child alive now that ``started_before`` does not hold."""
    from flowfile_worker import mp_context

    children = set(mp_context.active_children()) - started_before
    for child in children:
        child.terminate()
    for child in children:
        child.join(timeout=5)


def _end_on_signal(signum: int, started_before: set) -> None:
    """End a headless run on ``signum``: its children first, then the process, with no cleanup that could wait on them.

    A stage's node threads wait on their children, so unwinding first would wait out every job still running.
    """
    _terminate_children(started_before)
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(128 + signum)


def child_env() -> dict[str, str]:
    """The environment variables a child process needs to run on the worker embedded in its own process.

    ``FLOWFILE_COMPUTE_BACKEND`` names it; the legacy ``FLOWFILE_OFFLOAD_TO_WORKER=0`` says the same to a release
    that only knows that flag.
    """
    return {BACKEND_ENV_VAR: LOCAL_BACKEND_ID, LEGACY_OFFLOAD_ENV_VAR: "0"}


def worker_url() -> str:
    """The base URL of the worker the registry's worker backend runs on, for a caller that sends work there itself.

    In single-file mode that worker is embedded in this process and the app mounts its routes at
    ``settings.WORKER_URL``, which ``start_server`` sets; no backend is made for the answer.
    """
    if settings.SINGLE_FILE_MODE:
        return settings.WORKER_URL
    return cast("WorkerBackend", backend(WORKER_BACKEND_ID)).url


def reset() -> None:
    """Forget every registration, every instance and the entry-point scan, for tests; the default override stays."""
    global _entry_points_scanned
    with _lock:
        _factories.clear()
        _instances.clear()
        _hub_rows.clear()
        _through_hub.clear()
        _entry_points_scanned = False


def _scan_entry_points() -> None:
    global _entry_points_scanned
    if _entry_points_scanned:
        return
    for entry_point in importlib.metadata.entry_points(group=ENTRY_POINT_GROUP):
        if entry_point.name not in _BUILTIN:
            _factories.setdefault(entry_point.name, _entry_point_factory(entry_point))
    _entry_points_scanned = True


def _entry_point_factory(entry_point: importlib.metadata.EntryPoint) -> BackendFactory:
    """A factory that imports the entry point's target only when the backend is first made."""

    def make() -> ComputeBackend:
        return entry_point.load()()

    return make
