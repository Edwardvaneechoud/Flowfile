"""Install receipts for community nodes.

A single ``community_receipts.json`` sidecar lives inside
``storage.user_defined_nodes_directory`` (next to the installed ``<id>.py``
files). Modeled on ``user_defined/mounts.py``: a corrupt or missing file
degrades to an empty set; writes are atomic (tmp + ``os.replace``).

The receipt is the *only* thing that marks a ``.py`` in the nodes directory as
community-managed — the installer/uninstaller consult it so they never touch a
user's own hand-authored nodes.
"""

import json
import logging
import os
from pathlib import Path

from flowfile_core.flowfile.community_nodes.models import InstallReceipt
from shared import storage

logger = logging.getLogger(__name__)

RECEIPTS_FILE_NAME = "community_receipts.json"


def receipts_file_path() -> Path:
    return storage.user_defined_nodes_directory / RECEIPTS_FILE_NAME


def load_receipts() -> dict[str, InstallReceipt]:
    """Read all receipts keyed by node_id; missing/corrupt files degrade to empty."""
    path = receipts_file_path()
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return {}
    except OSError as e:
        logger.warning("Cannot read community receipts %s: %s", path, e)
        return {}
    try:
        data = json.loads(raw)
    except ValueError as e:
        logger.warning("Community receipts file %s is not valid JSON: %s", path, e)
        return {}
    if not isinstance(data, dict):
        logger.warning("Community receipts file %s has an unexpected shape; ignoring", path)
        return {}
    receipts: dict[str, InstallReceipt] = {}
    for node_id, payload in data.items():
        try:
            receipts[node_id] = InstallReceipt.model_validate(payload)
        except ValueError as e:
            logger.warning("Dropping invalid community receipt %r: %s", node_id, e)
    return receipts


def save_receipts(receipts: dict[str, InstallReceipt]) -> None:
    path = receipts_file_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {node_id: receipt.model_dump() for node_id, receipt in receipts.items()}
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def list_receipts() -> list[InstallReceipt]:
    return list(load_receipts().values())


def get_receipt(node_id: str) -> InstallReceipt | None:
    return load_receipts().get(node_id)


def put_receipt(receipt: InstallReceipt) -> None:
    receipts = load_receipts()
    receipts[receipt.node_id] = receipt
    save_receipts(receipts)


def remove_receipt(node_id: str) -> InstallReceipt | None:
    receipts = load_receipts()
    removed = receipts.pop(node_id, None)
    if removed is not None:
        save_receipts(receipts)
    return removed
