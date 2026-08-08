"""Where the core suite should point SecureStorage.

Imported by conftest before anything reaches flowfile_core, so it must stay
dependency-light (stdlib only) and free of import-time side effects.
"""

from __future__ import annotations

import os
import socket
import tempfile
from collections.abc import Mapping
from pathlib import Path

DEFAULT_STORE_DIRNAME = "flowfile_test_secure_storage"


def worker_is_listening(host: str = "127.0.0.1", port: int | None = None, timeout: float = 0.25) -> bool:
    """Fast probe for an externally started flowfile_worker."""
    if port is None:
        port = int(os.environ.get("FLOWFILE_WORKER_PORT", 63579))
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def resolve_test_secure_storage_path(worker_running: bool, env: Mapping[str, str]) -> str | None:
    """Return the store path to pin, or None to leave the store as it is.

    Isolating keeps jwt_secret / master_key / internal_token out of the developer's
    real ~/.config/flowfile. But core encrypts ``$ffsec$`` secrets with the master
    key in that dir and the worker re-derives it independently, and the suite
    *reuses* an already-running worker — which resolved its own store at its own
    import, before any test env existed. Isolating only core in that case splits
    the two keys and every worker-offloaded secret decrypt fails. So when a worker
    is already up we stay on the real store: consistent on both sides, and exactly
    the pre-existing behaviour.
    """
    if env.get("FLOWFILE_SECURE_STORAGE_PATH"):
        return None
    if worker_running:
        return None
    return str(Path(tempfile.gettempdir()) / DEFAULT_STORE_DIRNAME)
