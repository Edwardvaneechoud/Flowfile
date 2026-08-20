"""Public-surface contract for the PEP 562 lazy re-exports in `shared` and `flowfile_worker`.

Deferring an import must not shrink what a caller can reach. Before lazification the eager
`from .sql_utils import ...` / `from flowfile_worker.models import Status` chains also bound
sibling submodules as package attributes, and `from flowfile_worker import *` picked them up;
these tests pin that surface so a future deferral can't quietly drop a name.

Each test runs in a fresh interpreter -- module-level laziness is unobservable once another
test in the same process has already touched the attribute.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

# Bound as attributes of `shared` by the pre-lazification eager re-exports.
SHARED_SUBMODULE_ATTRS = ("db_dialects", "delta_utils", "sql_utils")

# Bound as attributes of `flowfile_worker` by the pre-lazification `models` import chain.
WORKER_LAZY_ATTRS = ("Status", "configs", "external_sources", "models", "secrets")

# Exactly what `from flowfile_worker import *` bound before __all__ existed.
WORKER_STAR_SURFACE = {
    "CACHE_DIR",
    "CACHE_EXPIRATION_TIME",
    "PROCESS_MEMORY_USAGE",
    "Status",
    "configs",
    "external_sources",
    "get_context",
    "get_version",
    "importlib",
    "models",
    "mp_context",
    "multiprocessing",
    "process_dict",
    "process_dict_lock",
    "secrets",
    "status_dict",
    "status_dict_lock",
    "storage",
    "sys",
    "threading",
}


def _run(body: str, tmp_path):
    """Run *body* in a fresh spawn-shaped interpreter; return its parsed RESULT line."""
    env = os.environ.copy()
    env["TEST_MODE"] = "1"
    env["FLOWFILE_SKIP_STARTUP_MIGRATION"] = "1"
    env["FLOWFILE_STORAGE_DIR"] = str(tmp_path / "storage")
    env["FLOWFILE_USER_DATA_DIR"] = str(tmp_path / "user_data")
    env["PYTHONPATH"] = os.pathsep.join([str(REPO_ROOT / "flowfile_worker"), str(REPO_ROOT)])
    code = "\n".join(
        [
            "import json, multiprocessing, sys",
            'multiprocessing.current_process().name = "Worker-1"',
            body,
        ]
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env=env,
        # cwd must not be a repo root: '' leads sys.path for -c and would shadow `shared`.
        cwd=str(tmp_path),
        timeout=120,
    )
    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"
    line = [x for x in result.stdout.splitlines() if x.startswith("RESULT")]
    return json.loads(line[-1][6:]) if line else None


@pytest.mark.worker
@pytest.mark.parametrize("attr", SHARED_SUBMODULE_ATTRS)
def test_shared_submodule_attribute_survives_bare_import(attr, tmp_path):
    got = _run(
        f"import shared\nprint('RESULT' + json.dumps(shared.{attr}.__name__))",
        tmp_path,
    )
    assert got == f"shared.{attr}"


@pytest.mark.worker
def test_dir_shared_still_lists_submodules(tmp_path):
    listed = _run("import shared\nprint('RESULT' + json.dumps(dir(shared)))", tmp_path)
    missing = sorted(set(SHARED_SUBMODULE_ATTRS) - set(listed))
    assert missing == [], f"dir(shared) lost: {missing}"


@pytest.mark.worker
def test_shared_public_names_all_resolve(tmp_path):
    # A name in __all__ but not in _LAZY_EXPORTS breaks `from shared import X` for every
    # caller, and the import machinery gives no warning.
    unresolved = _run(
        "import shared\n"
        "bad = []\n"
        "for _n in shared.__all__:\n"
        "    try:\n"
        "        getattr(shared, _n)\n"
        "    except AttributeError:\n"
        "        bad.append(_n)\n"
        "print('RESULT' + json.dumps(bad))",
        tmp_path,
    )
    assert unresolved == []


@pytest.mark.worker
def test_shared_rejects_unknown_attribute(tmp_path):
    assert _run(
        "import shared\n"
        "try:\n"
        "    shared.definitely_not_exported\n"
        "    out = 'NO RAISE'\n"
        "except AttributeError:\n"
        "    out = 'AttributeError'\n"
        "print('RESULT' + json.dumps(out))",
        tmp_path,
    ) == "AttributeError"


@pytest.mark.worker
@pytest.mark.parametrize("attr", WORKER_LAZY_ATTRS)
def test_worker_attribute_survives_bare_import(attr, tmp_path):
    got = _run(
        f"import flowfile_worker\nprint('RESULT' + json.dumps(getattr(flowfile_worker, {attr!r}).__name__))",
        tmp_path,
    )
    assert got in (attr, f"flowfile_worker.{attr}")


@pytest.mark.worker
def test_worker_star_import_surface_is_unchanged(tmp_path):
    # PEP 562: star-import consults __getattr__ for __all__ names absent from globals().
    ns = _run(
        "ns = {}\n"
        "exec('from flowfile_worker import *', ns)\n"
        "print('RESULT' + json.dumps(sorted(n for n in ns if not n.startswith('_'))))",
        tmp_path,
    )
    missing = sorted(WORKER_STAR_SURFACE - set(ns))
    assert missing == [], f"`from flowfile_worker import *` lost: {missing}"


@pytest.mark.worker
def test_worker_status_is_the_models_status(tmp_path):
    assert _run(
        "import flowfile_worker\n"
        "print('RESULT' + json.dumps(flowfile_worker.models.Status is flowfile_worker.Status))",
        tmp_path,
    )


@pytest.mark.worker
def test_worker_rejects_unknown_attribute(tmp_path):
    assert _run(
        "import flowfile_worker\n"
        "try:\n"
        "    flowfile_worker.definitely_not_exported\n"
        "    out = 'NO RAISE'\n"
        "except AttributeError:\n"
        "    out = 'AttributeError'\n"
        "print('RESULT' + json.dumps(out))",
        tmp_path,
    ) == "AttributeError"


@pytest.mark.worker
def test_get_type_hints_on_worker_package_does_not_raise(tmp_path):
    # A `dict[str, Status]` annotation left in runtime __annotations__ while Status is only
    # a TYPE_CHECKING import makes this raise NameError. Declaring the annotation inside the
    # TYPE_CHECKING block instead leaves status_dict un-hinted at runtime -- absent, which is
    # fine, rather than broken. Assert only that resolving the module's hints succeeds.
    outcome = _run(
        "import typing, flowfile_worker\n"
        "try:\n"
        "    typing.get_type_hints(flowfile_worker)\n"
        "    out = 'ok'\n"
        "except Exception as e:\n"
        "    out = f'{type(e).__name__}: {e}'\n"
        "print('RESULT' + json.dumps(out))",
        tmp_path,
    )
    assert outcome == "ok", f"get_type_hints(flowfile_worker) raised {outcome}"
