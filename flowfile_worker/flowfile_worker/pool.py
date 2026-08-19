"""Warm process pool for worker task execution.

Opt-in via ``FLOWFILE_WORKER_POOL_SIZE`` (int, default ``0`` = off): keeps up to N
spawned children alive between tasks so each task skips the interpreter boot and
import chain (~0.2s on macOS, ~0.4-0.7s on Windows). The wire protocol, the funcs
task signatures, and the spawn-per-task path are untouched - when the pool is off,
saturated, or the operation is not poolable, callers fall back to today's spawn.

Lifecycle rules:

- A member is reusable only when its task's completion envelope was received (the
  child puts one after every task, even for ops that put no result), it is still
  alive, and it is under its task/RSS budgets. Cancel, timeout, crash, or a wedged
  queue all retire it - a suspect child is never a pool member.
- Cancellation stays kill-based: ``/cancel_task`` terminates the member like any
  task process; the next acquire spawns a replacement on demand.
- Members are disposed outside the pool lock; only bookkeeping happens inside.
- An idle-TTL reaper drains the pool to zero when unused, so an idle desktop does
  not keep warm interpreters around.

Every pool child imports this module (it holds ``member_loop``), so module-level
imports must stay light: no models/pydantic, no funcs, no polars.
``tests/test_import_purity.py`` pins this.
"""

import os
import queue as _queue
import sys
import threading
from dataclasses import dataclass, field
from multiprocessing import Process
from multiprocessing.queues import Queue
from pathlib import Path
from time import monotonic
from typing import Any

from flowfile_worker import mp_context
from flowfile_worker.configs import logger
from shared.storage_config import storage

# Ops dispatched via spawner.start_process / streaming that are safe to reuse a
# child for. Deliberately excluded: custom nodes (exec user code - never pool),
# fuzzy (positional-arg spawn + heavy pl_fuzzy_frame_match import), train/apply
# model (huge allocator footprints), generic_task connectors (state not audited).
POOLABLE_OPERATIONS = frozenset(
    {
        "store",
        "store_sample",
        "calculate_schema",
        "calculate_number_of_records",
        "write_output",
        "write_to_database",
        "write_to_cloud_storage",
        "write_parquet",
        "write_delta",
        "merge_delta",
        "scd2_delta",
    }
)

_ERROR_BUFFER_SIZE = 1024  # must match the Array("c", 1024) used by spawner/streaming
_TASK_DONE = "__flowfile_pool_task_done__"
_DISPOSE_JOIN_TIMEOUT = 1.0


class _ResultSlot:
    """In-child stand-in for the result queue a funcs target put()s into.

    Collecting the payload in memory lets ``member_loop`` put one uniform
    completion envelope afterwards, so the parent's drain never has to know
    which ops produce a result and a >64KB payload can never wedge a member
    (the parent always drains exactly one envelope per task).
    """

    __slots__ = ("item",)

    def __init__(self):
        self.item = None

    def put(self, item):
        self.item = item


def _start_orphan_watch(poll_interval: float = 5.0) -> None:
    """Exit the member when the worker dies without running its shutdown ladder.

    Per-task children die naturally at task end; a member blocked on task_q.get()
    would outlive a SIGKILLed worker forever. multiprocessing.parent_process()
    is the worker on every platform.
    """
    import multiprocessing
    from time import sleep

    parent = multiprocessing.parent_process()

    def watch() -> None:
        while parent.is_alive():
            sleep(poll_interval)
        os._exit(1)

    threading.Thread(target=watch, daemon=True, name="pool-orphan-watch").start()


def member_loop(task_q: Queue, result_q: Queue, progress, error_message) -> None:
    """Child entrypoint: run funcs tasks until the ``None`` shutdown sentinel.

    The heavy import chain is paid once here. The shared primitives are inherited
    at spawn (a Windows requirement: sharedctypes cannot travel through queues)
    and injected into every task call; the parent resets them before each submit.
    An uncaught exception ends the process - the parent treats a dead member like
    any dead task child and replaces it on the next acquire.
    """
    from flowfile_worker import funcs

    _start_orphan_watch()
    while True:
        message = task_q.get()
        if message is None:
            return
        operation, kwargs = message
        slot = _ResultSlot()
        getattr(funcs, operation)(**kwargs, progress=progress, error_message=error_message, queue=slot)
        result_q.put((_TASK_DONE, slot.item))
        # Release the task payload and result while idling, else they pin RSS until the next task.
        del message, kwargs, slot


def unwrap_envelope(raw: Any) -> tuple[bool, Any]:
    """Split a drained pool result into (envelope_received, payload).

    Anything other than a well-formed envelope means the member crashed, was
    terminated, or wedged - the caller must not reuse it.
    """
    if isinstance(raw, tuple) and len(raw) == 2 and raw[0] == _TASK_DONE:
        return True, raw[1]
    return False, None


@dataclass
class PoolMember:
    process: Process
    task_q: Queue
    result_q: Queue
    progress: Any
    error_message: Any
    tasks_served: int = 0
    last_used_at: float = field(default_factory=monotonic)

    def submit(self, operation: str, kwargs: dict) -> None:
        """Reset the shared primitives and hand the member its next task.

        The parent resets progress/error before the put: the child only touches
        them once the task runs, so the monitor can never observe the previous
        task's terminal values. The error buffer is fully zeroed because funcs
        targets write without a terminator past their own message.
        """
        with self.progress.get_lock():
            self.progress.value = 0
        with self.error_message.get_lock():
            self.error_message[:] = b"\x00" * _ERROR_BUFFER_SIZE
        self.tasks_served += 1
        self.last_used_at = monotonic()
        self.task_q.put((operation, kwargs))


class TaskPool:
    """Fixed-capacity registry of warm members, spawned on demand up to *size*.

    There is no background refill: a retired member simply frees a slot and the
    next acquire spawns into it, so a cancel storm degrades to spawn-per-task
    and self-heals. Modeled on VizSessionRegistry's budgets and teardown.
    """

    def __init__(
        self,
        size: int,
        max_tasks_per_member: int,
        idle_ttl_seconds: float,
        rss_limit_mb: int,
        reap_interval_seconds: float = 30.0,
    ):
        self._size = size
        self._max_tasks = max_tasks_per_member
        self._idle_ttl = idle_ttl_seconds
        self._rss_limit_mb = rss_limit_mb
        self._reap_interval = reap_interval_seconds
        self._idle: list[PoolMember] = []
        self._leased: dict[int, PoolMember] = {}
        self._total = 0
        self._tasks_completed = 0
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._reaper: threading.Thread | None = None

    def acquire(self, operation: str) -> PoolMember | None:
        """Lease a member for *operation*, or None to signal spawn-per-task.

        Prefers an idle member; otherwise spawns into a free slot (the new
        child's first task pays the import chain, which is no worse than the
        fallback). Returns None when the pool is off, the op is not poolable,
        or every slot is busy.
        """
        if self._size <= 0 or operation not in POOLABLE_OPERATIONS:
            return None
        self._ensure_reaper()
        dead: list[PoolMember] = []
        member: PoolMember | None = None
        with self._lock:
            while self._idle:
                candidate = self._idle.pop()
                if candidate.process.is_alive():
                    member = candidate
                    break
                dead.append(candidate)
                self._total -= 1
            if member is None and self._total < self._size:
                member = self._spawn_member()
                self._total += 1
            if member is not None:
                self._leased[member.process.pid] = member
        for zombie in dead:
            self._dispose(zombie)
        return member

    def checkin(self, member: PoolMember, reusable: bool) -> None:
        """Return a leased member: back to the idle set, or retired.

        *reusable* is the caller's verdict that the task ended cleanly and the
        completion envelope was drained - the structural never-reuse-a-wedged-
        child rule lives in this single gate. A member over the (possibly just
        resized) capacity is retired even when healthy.
        """
        with self._lock:
            self._leased.pop(member.process.pid, None)
            self._tasks_completed += 1
        if reusable and member.process.is_alive() and self._under_budget(member):
            with self._lock:
                if self._total <= self._size:
                    member.last_used_at = monotonic()
                    self._idle.append(member)
                    return
        with self._lock:
            self._total -= 1
        self._dispose(member)

    def resize(self, size: int) -> None:
        """Change capacity at runtime; shrinking retires excess idle members now.

        Busy members over the new capacity are retired at their next checkin.
        Growing prewarms the new slots immediately - an admin enabling a *warm*
        pool should see warm members, not lazily-filled capacity.
        """
        excess: list[PoolMember] = []
        with self._lock:
            self._size = max(0, size)
            while len(self._idle) > 0 and self._total > self._size:
                excess.append(self._idle.pop())
                self._total -= 1
        for member in excess:
            self._dispose(member)
        logger.info(f"Worker pool resized to {self._size} (retired {len(excess)} idle member(s))")
        self.prewarm()

    def prewarm(self) -> None:
        """Fill every slot at startup so even the first task runs warm."""
        if self._size <= 0:
            return
        self._ensure_reaper()
        with self._lock:
            while self._total < self._size:
                self._idle.append(self._spawn_member())
                self._total += 1
        logger.info(f"Worker pool prewarmed to {self._size} member(s)")

    def shutdown(self) -> None:
        """Stop the reaper and dispose every idle member (lifespan hook)."""
        self._stop.set()
        with self._lock:
            members, self._idle = self._idle, []
            self._total -= len(members)
        for member in members:
            self._dispose(member)

    def stats(self) -> dict:
        with self._lock:
            return {"size": self._size, "idle": len(self._idle), "total": self._total}

    def describe(self) -> dict:
        """Full state + config + per-member detail for the admin dashboard."""
        now = monotonic()
        with self._lock:
            snapshot = [(m, "idle") for m in self._idle] + [(m, "busy") for m in self._leased.values()]
            size, idle_count, total, tasks_completed = self._size, len(self._idle), self._total, self._tasks_completed
        members = [
            {
                "pid": member.process.pid,
                "state": state,
                "tasks_served": member.tasks_served,
                "idle_seconds": round(now - member.last_used_at, 1) if state == "idle" else 0.0,
                "rss_mb": _rss_mb(member.process.pid),
            }
            for member, state in snapshot
        ]
        env_override = env_override_active()
        return {
            "enabled": size > 0,
            "size": size,
            "idle": idle_count,
            "busy": total - idle_count,
            "tasks_completed": tasks_completed,
            "members": members,
            "max_tasks_per_member": self._max_tasks,
            "idle_ttl_seconds": self._idle_ttl,
            "rss_limit_mb": self._rss_limit_mb,
            "platform_default_size": default_pool_size(),
            "env_override": env_override,
            # True when the current size is what the next boot will use.
            "persisted": not env_override and load_persisted_size() == size,
        }

    def _spawn_member(self) -> PoolMember:
        task_q = mp_context.Queue(maxsize=1)
        result_q = mp_context.Queue(maxsize=1)
        progress = mp_context.Value("i", 0)
        error_message = mp_context.Array("c", _ERROR_BUFFER_SIZE)
        process = mp_context.Process(
            target=member_loop,
            args=(task_q, result_q, progress, error_message),
            name="flowfile-pool-member",
            # Daemonic: multiprocessing's atexit TERMINATES daemons but JOINS
            # non-daemons, and an idle member blocks in task_q.get() forever -
            # a process that never ran the lifespan shutdown (pytest, embedding)
            # would hang at interpreter exit. Poolable funcs spawn no children,
            # so the daemon no-children restriction cannot bite.
            daemon=True,
        )
        process.start()
        logger.info(f"Worker pool member spawned (pid={process.pid})")
        return PoolMember(
            process=process, task_q=task_q, result_q=result_q, progress=progress, error_message=error_message
        )

    def _under_budget(self, member: PoolMember) -> bool:
        if member.tasks_served >= self._max_tasks:
            return False
        rss_mb = _rss_mb(member.process.pid)
        return rss_mb is None or rss_mb <= self._rss_limit_mb

    def _dispose(self, member: PoolMember) -> None:
        """Tear a member down; it is already off the books. Never called under the lock."""
        process = member.process
        if process.is_alive():
            try:
                # Non-blocking: a wedged member that never consumed its task leaves the
                # maxsize=1 queue full, and a blocking put would hang this thread.
                member.task_q.put_nowait(None)
                process.join(timeout=_DISPOSE_JOIN_TIMEOUT)
            except _queue.Full:
                pass
            if process.is_alive():
                process.terminate()
        process.join()
        _close_queue(member.task_q)
        _close_queue(member.result_q)
        logger.info(f"Worker pool member disposed (pid={process.pid}, tasks_served={member.tasks_served})")

    def _ensure_reaper(self) -> None:
        with self._lock:
            if self._reaper is not None:
                return
            self._reaper = threading.Thread(target=self._reap_loop, daemon=True, name="worker-pool-reaper")
            self._reaper.start()

    def _reap_loop(self) -> None:
        while not self._stop.wait(self._reap_interval):
            self._reap_idle()

    def _reap_idle(self) -> None:
        cutoff = monotonic() - self._idle_ttl
        with self._lock:
            expired = [m for m in self._idle if m.last_used_at < cutoff]
            self._idle = [m for m in self._idle if m.last_used_at >= cutoff]
            self._total -= len(expired)
        for member in expired:
            self._dispose(member)


def _rss_mb(pid: int) -> float | None:
    """Resident set size in MB, or None when psutil is unavailable (budget skipped)."""
    try:
        import psutil

        return psutil.Process(pid).memory_info().rss / 1e6
    except Exception:
        return None


def _close_queue(q: Queue) -> None:
    try:
        q.close()
        q.cancel_join_thread()
    except (OSError, ValueError):
        pass


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return max(0, int(raw))
    except ValueError:
        logger.warning(f"Ignoring invalid {name}={raw!r}; using {default}")
        return default


def default_pool_size() -> int:
    """Platform default when neither the env var nor a saved UI setting exists.

    Windows pays 0.4-0.7s (desktop) to 11s+ (CI runners) per spawn, so the pool
    is on by default there; elsewhere spawn is cheap enough to stay opt-in.
    """
    return 4 if sys.platform == "win32" else 0


def _settings_file() -> Path:
    return storage.base_directory / "worker_pool.yaml"


def env_override_active() -> bool:
    """True when FLOWFILE_WORKER_POOL_SIZE pins the size, beating the saved setting."""
    return os.environ.get("FLOWFILE_WORKER_POOL_SIZE") is not None


def load_persisted_size() -> int | None:
    """The UI-saved pool size, or None when never saved / unreadable."""
    import yaml

    try:
        data = yaml.safe_load(_settings_file().read_text(encoding="utf-8"))
        return max(0, min(int(data["size"]), 32))
    except (OSError, KeyError, TypeError, ValueError, yaml.YAMLError):
        return None


def persist_size(size: int) -> bool:
    """Save the pool size so it survives restarts; atomic, never raises.

    Deliberately YAML with a comment header: the file is hand-editable, diffs
    cleanly, and a future project git-projection can copy it verbatim.
    """
    target = _settings_file()
    content = (
        "# Warm worker pool - managed from the Flowfile UI (Kernel Manager / Admin).\n"
        "# FLOWFILE_WORKER_POOL_SIZE, when set, overrides this value at boot.\n"
        f"size: {size}\n"
    )
    try:
        tmp = target.with_suffix(".yaml.tmp")
        tmp.write_text(content, encoding="utf-8")
        os.replace(tmp, target)
        return True
    except OSError as exc:
        logger.warning(f"Could not persist worker pool size to {target}: {exc}")
        return False


def initial_pool_size() -> int:
    """Boot-time size: env var (explicit operator override) > saved UI setting > platform default."""
    if env_override_active():
        return _int_env("FLOWFILE_WORKER_POOL_SIZE", default_pool_size())
    saved = load_persisted_size()
    if saved is not None:
        return saved
    return default_pool_size()


task_pool = TaskPool(
    size=initial_pool_size(),
    max_tasks_per_member=_int_env("FLOWFILE_WORKER_POOL_MAX_TASKS", 100),
    idle_ttl_seconds=_int_env("FLOWFILE_WORKER_POOL_IDLE_TTL", 300),
    rss_limit_mb=_int_env("FLOWFILE_WORKER_POOL_RSS_MB", 2048),
)
