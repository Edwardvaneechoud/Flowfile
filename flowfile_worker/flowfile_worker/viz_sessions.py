"""Registry of long-lived spawned child processes that hold viz LazyFrames.

The FastAPI process keeps only a thin index ``session_key -> SessionHandle``;
all polars / polars-gw imports and dataset memory live inside the spawned
child entry point in :mod:`flowfile_worker.viz_session_worker`.
"""

from __future__ import annotations

import gc
import logging
import queue as _queue_mod
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from fastapi import HTTPException

from flowfile_worker import models, mp_context

try:
    import psutil as _psutil
except ImportError:
    _psutil = None

logger = logging.getLogger(__name__).getChild("viz")


HTTP_TIMEOUT_SECONDS = 120
REQUEST_TIMEOUT_SECONDS = HTTP_TIMEOUT_SECONDS - 5
SHUTDOWN_GRACE_SECONDS = 10
REQUEST_QUEUE_MAXSIZE = 8


@dataclass
class SessionHandle:
    key: str
    process: Any
    request_q: Any
    response_q: Any
    started_at: float = field(default_factory=time.time)
    last_used_at: float = field(default_factory=time.time)
    requests_served: int = 0
    pid: int = -1


class VizSessionRegistry:
    IDLE_TTL_SECONDS = 300
    MAX_SESSIONS = 32
    REAP_INTERVAL_SECONDS = 30
    MAX_REQUESTS_PER_CHILD = 500
    MAX_CHILD_LIFETIME_SECONDS = 1800

    def __init__(self) -> None:
        self._sessions: dict[str, SessionHandle] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._reaper = threading.Thread(target=self._reap_loop, daemon=True, name="viz-session-reaper")
        self._reaper.start()

    # -- public API ---------------------------------------------------------

    def execute(
        self,
        source: models.VizWorkerSource,
        op: str,
        payload: dict | None,
        max_rows: int | None,
    ) -> tuple[Any, bool]:
        handle, cache_hit = self._get_or_spawn(source)
        request_id = uuid.uuid4().hex
        msg: dict[str, Any] = {"op": op, "request_id": request_id}
        if op == "execute":
            msg["payload"] = payload
            msg["max_rows"] = max_rows if max_rows is not None else 100_000
        try:
            handle.request_q.put(msg, timeout=0.5)
        except _queue_mod.Full as exc:
            raise HTTPException(
                status_code=503,
                detail=f"viz session busy: queue full for session_key={source.session_key}",
            ) from exc
        handle.last_used_at = time.time()
        handle.requests_served += 1
        logger.debug(
            "serve key=%s pid=%d op=%s requests=%d",
            handle.key,
            handle.pid,
            op,
            handle.requests_served,
        )
        response = self._await_response(handle, request_id)
        if response.get("fatal"):
            self._evict_handle(handle)
            raise HTTPException(
                status_code=502,
                detail=f"viz worker failed to load source: {response.get('error', 'unknown')}",
            )
        if not response.get("ok"):
            err_type = response.get("type")
            err_msg = response.get("error", "unknown error")
            if err_type == "ValueError":
                raise ValueError(err_msg)
            raise RuntimeError(err_msg)
        result = response["result"]
        if op == "execute":
            return {**result, "cache_hit": cache_hit}, cache_hit
        return result, cache_hit

    def evict(self, session_key: str) -> None:
        with self._lock:
            h = self._sessions.pop(session_key, None)
        if h is not None:
            self._kill_handle(h)

    def evict_all(self) -> None:
        with self._lock:
            handles = list(self._sessions.values())
            self._sessions.clear()
        for h in handles:
            self._kill_handle(h)

    def stats(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        now = time.time()
        with self._lock:
            handles = list(self._sessions.values())
        for h in handles:
            rss = -1
            if _psutil is not None:
                try:
                    rss = _psutil.Process(h.pid).memory_info().rss
                except (_psutil.NoSuchProcess, _psutil.AccessDenied):
                    pass
                except Exception:
                    pass
            out.append(
                {
                    "session_key": h.key,
                    "pid": h.pid,
                    "alive": h.process.is_alive(),
                    "rss_bytes": rss,
                    "requests_served": h.requests_served,
                    "age_seconds": round(now - h.started_at, 1),
                    "idle_seconds": round(now - h.last_used_at, 1),
                }
            )
        return out

    def shutdown(self) -> None:
        self._stop.set()
        with self._lock:
            handles = list(self._sessions.values())
            self._sessions.clear()
        logger.info("shutdown sessions=%d", len(handles))
        for h in handles:
            try:
                h.request_q.put_nowait({"op": "shutdown"})
            except Exception:
                pass
        deadline = time.time() + SHUTDOWN_GRACE_SECONDS
        for h in handles:
            remaining = max(0, deadline - time.time())
            h.process.join(timeout=remaining)
        for h in handles:
            if h.process.is_alive():
                h.process.terminate()
                h.process.join(timeout=2)
                if h.process.is_alive():
                    h.process.kill()
                    h.process.join()
            self._drain_queue(h.request_q)
            self._drain_queue(h.response_q)
            self._cancel_and_close(h.request_q)
            self._cancel_and_close(h.response_q)

    # -- internals ----------------------------------------------------------

    def _get_or_spawn(self, source: models.VizWorkerSource) -> tuple[SessionHandle, bool]:
        key = source.session_key
        with self._lock:
            existing = self._sessions.get(key)
            if existing is not None and self._is_child_healthy(existing):
                return existing, True
            stale = existing
            if stale is not None:
                self._sessions.pop(key, None)

        if stale is not None:
            reason = self._unhealthy_reason(stale)
            logger.info(
                "kill key=%s pid=%d reason=%s requests=%d age=%.0fs",
                stale.key,
                stale.pid,
                reason,
                stale.requests_served,
                time.time() - stale.started_at,
            )
            threading.Thread(target=self._kill_handle, args=(stale,), daemon=True).start()

        handle = self._spawn_child(source)

        with self._lock:
            winner = self._sessions.get(key)
            if winner is not None and self._is_child_healthy(winner):
                # Lost a race. Kill our newly spawned child outside the lock.
                threading.Thread(target=self._kill_handle, args=(handle,), daemon=True).start()
                return winner, True
            self._sessions[key] = handle
            self._enforce_lru_limit_locked()
            return handle, False

    def _is_child_healthy(self, h: SessionHandle) -> bool:
        if not h.process.is_alive():
            return False
        if h.requests_served >= self.MAX_REQUESTS_PER_CHILD:
            return False
        if time.time() - h.started_at >= self.MAX_CHILD_LIFETIME_SECONDS:
            return False
        return True

    def _unhealthy_reason(self, h: SessionHandle) -> str:
        if not h.process.is_alive():
            return "died"
        if h.requests_served >= self.MAX_REQUESTS_PER_CHILD:
            return "request_budget"
        if time.time() - h.started_at >= self.MAX_CHILD_LIFETIME_SECONDS:
            return "lifetime"
        return "unknown"

    def _spawn_child(self, source: models.VizWorkerSource) -> SessionHandle:
        from flowfile_worker import viz_session_worker

        request_q = mp_context.Queue(maxsize=REQUEST_QUEUE_MAXSIZE)
        response_q = mp_context.Queue()
        src_dict = source.model_dump()
        p = mp_context.Process(
            target=viz_session_worker.viz_session_main,
            kwargs={"source": src_dict, "request_q": request_q, "response_q": response_q},
            name=f"viz-{source.session_key[:32]}",
            daemon=True,
        )
        p.start()
        handle = SessionHandle(
            key=source.session_key,
            process=p,
            request_q=request_q,
            response_q=response_q,
            pid=p.pid or -1,
        )
        logger.info("spawn key=%s pid=%d", source.session_key, p.pid)
        return handle

    def _await_response(self, handle: SessionHandle, request_id: str) -> dict[str, Any]:
        deadline = time.time() + REQUEST_TIMEOUT_SECONDS
        while True:
            remaining = max(0.0, deadline - time.time())
            try:
                response = handle.response_q.get(timeout=remaining if remaining > 0 else 0.1)
            except _queue_mod.Empty as exc:
                if not handle.process.is_alive():
                    self._evict_handle(handle)
                    raise HTTPException(status_code=502, detail="viz worker died unexpectedly") from exc
                raise HTTPException(
                    status_code=504,
                    detail=f"viz compute timed out after {REQUEST_TIMEOUT_SECONDS}s",
                ) from exc
            rid = response.get("request_id")
            if response.get("fatal") or rid is None or rid == request_id:
                return response
            logger.warning(
                "stale response key=%s expected=%s got=%s; draining",
                handle.key,
                request_id,
                rid,
            )

    def _evict_handle(self, handle: SessionHandle) -> None:
        with self._lock:
            cur = self._sessions.get(handle.key)
            if cur is handle:
                self._sessions.pop(handle.key, None)
        threading.Thread(target=self._kill_handle, args=(handle,), daemon=True).start()

    def _kill_handle(self, h: SessionHandle) -> None:
        try:
            h.request_q.put_nowait({"op": "shutdown"})
        except Exception:
            pass
        h.process.join(timeout=SHUTDOWN_GRACE_SECONDS)
        if h.process.is_alive():
            logger.warning(
                "kill key=%s pid=%d (terminate after %ds grace)",
                h.key,
                h.pid,
                SHUTDOWN_GRACE_SECONDS,
            )
            h.process.terminate()
            h.process.join(timeout=2)
            if h.process.is_alive():
                h.process.kill()
                h.process.join()
        self._drain_queue(h.request_q)
        self._drain_queue(h.response_q)
        self._cancel_and_close(h.request_q)
        self._cancel_and_close(h.response_q)
        del h
        gc.collect()

    @staticmethod
    def _drain_queue(q: Any) -> None:
        try:
            while not q.empty():
                try:
                    q.get_nowait()
                except Exception:
                    break
        except Exception:
            pass

    @staticmethod
    def _cancel_and_close(q: Any) -> None:
        try:
            q.cancel_join_thread()
        except Exception:
            pass
        try:
            q.close()
        except Exception:
            pass

    def _enforce_lru_limit_locked(self) -> None:
        if len(self._sessions) <= self.MAX_SESSIONS:
            return
        excess = len(self._sessions) - self.MAX_SESSIONS
        ordered = sorted(self._sessions.items(), key=lambda kv: kv[1].last_used_at)
        for key, h in ordered[:excess]:
            self._sessions.pop(key, None)
            threading.Thread(target=self._kill_handle, args=(h,), daemon=True).start()

    def _reap_loop(self) -> None:
        while not self._stop.wait(self.REAP_INTERVAL_SECONDS):
            cutoff = time.time() - self.IDLE_TTL_SECONDS
            with self._lock:
                stale = [(k, h) for k, h in self._sessions.items() if h.last_used_at < cutoff]
                for k, _ in stale:
                    self._sessions.pop(k, None)
            for k, h in stale:
                logger.info(
                    "reap key=%s pid=%d age=%.0fs reason=idle",
                    k,
                    h.pid,
                    time.time() - h.started_at,
                )
                self._kill_handle(h)


viz_session_registry = VizSessionRegistry()
