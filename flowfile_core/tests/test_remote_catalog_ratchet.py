"""Generic modules that import the hub's client: a number that only goes down.

A catalog elsewhere is reached through ``catalog.table_sources``, which its client registers with, so a generic module
has no reason to import ``hub_client`` or ``shared.hub_*``. The modules that still do are counted here; lower ``LIMIT``
when one stops, never raise it.
"""

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
PACKAGES = (
    "flowfile_core/flowfile_core",
    "flowfile_worker/flowfile_worker",
    "flowfile_scheduler/flowfile_scheduler",
    "flowfile_frame/flowfile_frame",
    "shared",
)
IMPORTS = re.compile(
    r"^\s*(from|import) (flowfile_core\.hub_client|shared\.hub_|shared import hub_)"
    r"|from flowfile_core import hub_client|from shared import .*hub_",
    re.MULTILINE,
)
LIMIT = 20


def _is_the_clients_own(path: Path) -> bool:
    relative = path.relative_to(REPO).as_posix()
    return "/hub_client/" in relative or "/tests/" in relative or "routes/hub_" in relative or "shared/hub_" in relative


def test_no_more_generic_modules_import_the_hub_client_than_before():
    importing = sorted(
        path.relative_to(REPO).as_posix()
        for package in PACKAGES
        for path in (REPO / package).rglob("*.py")
        if not _is_the_clients_own(path) and IMPORTS.search(path.read_text())
    )
    assert len(importing) <= LIMIT, "\n".join(importing)
