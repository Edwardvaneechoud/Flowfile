"""Import-weight contract for the programmatic API.

`import flowfile_frame` / `import flowfile` build ETL graphs in-process and must
stay lightweight: they must NOT drag in the FastAPI server stack, Alembic, the
cloud/data heavyweights (boto3, deltalake, gcsfs, pyarrow), worker/HTTP clients
(requests, httpx, websockets), Docker/Kafka/Excel/YAML/crypto libraries, or
faker, and importing them must NOT create or migrate the catalog DB on disk
(that is deferred to first actual DB access / server startup — see
database/connection.ensure_db_initialized).

Each check runs in a fresh subprocess because sys.modules is process-global: by
the time the pytest session reaches this file, other test modules have already
imported FastAPI, so an in-process assertion would be meaningless.
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path

# Server/heavy deps that must stay off the dataframe-API import path.
BANNED = [
    # server stack
    "fastapi",
    "uvicorn",
    "alembic",
    # cloud/data heavyweights
    "boto3",
    "botocore",
    "deltalake",
    "gcsfs",
    "pyarrow",
    # worker/HTTP clients (only needed when talking to worker/kernel/web)
    "requests",
    "httpx",
    "websockets",
    "docker",
    # format/source specifics (load on first use of the matching node)
    "openpyxl",
    "fastexcel",
    "confluent_kafka",
    "yaml",
    # secrets crypto (loads on first encrypt/decrypt)
    "cryptography",
    "passlib",
    # sample-data generator
    "faker",
]


def _run(script: str) -> str:
    """Run a snippet in a fresh interpreter with a throwaway catalog DB path.

    PYTHONPATH is seeded from the parent's ``sys.path`` so the subprocess imports
    the exact same packages this test session does (the package-under-test), not
    whatever a bare interpreter's site config happens to resolve.
    """
    import os

    with tempfile.TemporaryDirectory() as tmp:
        env = {
            **os.environ,
            "FLOWFILE_DB_PATH": str(Path(tmp) / "catalog.db"),
            "PYTHONPATH": os.pathsep.join(p for p in sys.path if p),
        }
        proc = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            env=env,
        )
        assert proc.returncode == 0, f"subprocess failed:\nstdout={proc.stdout}\nstderr={proc.stderr}"
        return proc.stdout


def test_import_flowfile_frame_is_lightweight() -> None:
    out = _run(
        "import os, sys\n"
        f"banned = {BANNED!r}\n"
        "import flowfile_frame\n"
        "leaked = [m for m in banned if m in sys.modules]\n"
        "assert not leaked, f'import flowfile_frame leaked heavy modules: {leaked}'\n"
        "assert not os.path.exists(os.environ['FLOWFILE_DB_PATH']), 'import created the catalog DB on disk'\n"
        "print('ok')\n"
    )
    assert "ok" in out


def test_import_flowfile_is_lightweight() -> None:
    out = _run(
        "import os, sys\n"
        f"banned = {BANNED!r}\n"
        "import flowfile\n"
        "leaked = [m for m in banned if m in sys.modules]\n"
        "assert not leaked, f'import flowfile leaked heavy modules: {leaked}'\n"
        "assert not os.path.exists(os.environ['FLOWFILE_DB_PATH']), 'import created the catalog DB on disk'\n"
        # The web-UI entry points stay resolvable (lazily) without being loaded at import.
        "assert callable(flowfile.start_web_ui)\n"
        "assert callable(flowfile.open_graph_in_editor)\n"
        "print('ok')\n"
    )
    assert "ok" in out
