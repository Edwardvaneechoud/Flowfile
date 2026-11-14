"""Generic modules that import the hub's client: a number that only goes down.

A catalog elsewhere is reached through ``catalog.table_sources``, which its client registers with, so a generic module
has no reason to import ``hub_client`` or ``shared.hub_*``. The modules that still do are counted here; lower ``LIMIT``
when one stops, never raise it.

What makes a server one of a hub's own (the join, its keys, the heartbeat, the hub's runs, sign-in through the hub, the
checks of what the hub signs and seals) is no part of Flowfile: a server's extension brings it
(``shared.server_extension``), so no module here names any of it.
"""

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
PACKAGES = (
    "flowfile_core/flowfile_core",
    "flowfile_worker/flowfile_worker",
    "flowfile_scheduler/flowfile_scheduler",
    "flowfile_frame/flowfile_frame",
    "flowfile/flowfile",
    "shared",
)
IMPORTS = re.compile(
    r"^\s*(from|import) (flowfile_core\.hub_client|shared\.hub_|shared import hub_)"
    r"|from flowfile_core import hub_client|from shared import .*hub_",
    re.MULTILINE,
)
LIMIT = 4
A_HUB_S_SERVER = re.compile(
    r"flowfile_hub_node|\bhub_node\b|hub_node_key|hub_tokens|hub_sealed|issuer_tokens|issuer_accounts|issuer_signin"
    r"|FLOWFILE_HUB_JOIN_TOKEN|/hub/dispatch|hub/nodes/join|hub/nodes/heartbeat|\"--join\""
)


def _is_the_clients_own(path: Path) -> bool:
    relative = path.relative_to(REPO).as_posix()
    return "/hub_client/" in relative or "/tests/" in relative or "routes/hub_" in relative or "shared/hub_" in relative


def test_no_more_generic_modules_import_the_hub_client_than_before():
    importing = sorted(
        path.relative_to(REPO).as_posix()
        for package in PACKAGES
        for path in (REPO / package).rglob("*.py")
        if not _is_the_clients_own(path) and IMPORTS.search(path.read_text(encoding="utf-8"))
    )
    assert len(importing) <= LIMIT, "\n".join(importing)


def test_no_module_names_what_makes_a_server_one_of_a_hub_s_own():
    naming = sorted(
        path.relative_to(REPO).as_posix()
        for package in PACKAGES
        for path in (REPO / package).rglob("*.py")
        if "/tests/" not in path.relative_to(REPO).as_posix() and A_HUB_S_SERVER.search(path.read_text(encoding="utf-8"))
    )
    assert naming == []
