"""F2: core and worker must resolve a quoted/whitespace FLOWFILE_MASTER_KEY to the same key.

``/setup/generate-key`` instructs operators to write ``FLOWFILE_MASTER_KEY="<key>"``
into ``.env``. If the surrounding quotes survive into the env value, core and worker
must agree on the resulting key or the worker can't decrypt what core encrypted.
"""

import os
import sys

from cryptography.fernet import Fernet

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

from flowfile_core.auth.secrets import get_docker_secret_key as core_get_docker_secret_key
from flowfile_worker.secrets import get_docker_secret_key as worker_get_docker_secret_key


def test_quoted_master_key_resolves_identically_in_core_and_worker(monkeypatch):
    valid_key = Fernet.generate_key().decode()
    monkeypatch.setenv("FLOWFILE_MASTER_KEY", f'"{valid_key}"')

    core_key = core_get_docker_secret_key()
    worker_key = worker_get_docker_secret_key()

    assert core_key == valid_key
    assert worker_key == valid_key
    assert core_key == worker_key
