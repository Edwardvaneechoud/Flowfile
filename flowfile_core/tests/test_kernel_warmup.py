"""Kernel-manager warm-up: singleton construction stays single under contention,
and a failing warm-up never breaks app startup. Docker-free (stubbed manager)."""
import threading
import time

from fastapi.testclient import TestClient

import flowfile_core.kernel as kernel_pkg
from flowfile_core import main


def test_get_kernel_manager_constructs_once_under_contention(monkeypatch):
    constructions: list[int] = []

    class StubManager:
        def __init__(self, shared_volume_path=None):
            constructions.append(1)
            time.sleep(0.05)

    monkeypatch.setattr(kernel_pkg, "KernelManager", StubManager)
    monkeypatch.setattr(kernel_pkg, "_manager", None)

    results: list = []
    barrier = threading.Barrier(8)

    def grab():
        barrier.wait()
        results.append(kernel_pkg.get_kernel_manager())

    threads = [threading.Thread(target=grab) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(constructions) == 1, "racing callers must share one construction"
    assert len({id(r) for r in results}) == 1


def test_startup_survives_warmup_failure(monkeypatch):
    class BoomManager:
        def __init__(self, shared_volume_path=None):
            raise RuntimeError("docker unavailable")

    monkeypatch.setattr(kernel_pkg, "KernelManager", BoomManager)
    monkeypatch.setattr(kernel_pkg, "_manager", None)
    monkeypatch.setenv("FLOWFILE_KERNEL_WARMUP", "1")

    with TestClient(main.app) as client:
        assert client.get("/docs").status_code == 200
        # Let the warm-up daemon thread hit the stub while it is still patched.
        time.sleep(0.2)
    assert kernel_pkg._manager is None, "a failed warm-up must leave the 503 path intact"
