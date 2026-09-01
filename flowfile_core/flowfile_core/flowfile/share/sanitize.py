"""Strip everything machine-local from the settings that travel in a link.

A share link is a flow, never an environment: absolute paths, catalog and
namespace ids, registration ids and named connection references all describe the
sender's install and mean nothing (or the wrong thing) to the recipient. Secrets
never appear here in the first place — settings carry ``$ffsec$`` references,
not values — but dropping the connection *names* keeps the link from advertising
the sender's infrastructure either.
"""

import re
from pathlib import Path

_MACHINE_LOCAL_KEYS = frozenset(
    {
        "abs_file_path",
        "analysis_file_available",
        "catalog_namespace_id",
        "catalog_table_id",
        "directory",
        "flow_path",
        "flow_uuid",
        "id",
        "namespace_id",
        "source_registration_id",
        "status",
        "user_id",
    }
)

# database_connection_name, ga_connection_name, kafka_connection_id, ...
_CONNECTION_KEY_RE = re.compile(r"(?:^|_)connection_(?:name|id)$")

# Format-specific reader/writer options; their keys are a stable vocabulary, so
# pruning nulls there would only remove settings the browser reads.
_KEEP_NULLS_KEYS = frozenset({"table_settings"})

_WASM_FLOW_SETTINGS = {
    "execution_mode": "Development",
    "execution_location": "local",
    "auto_save": True,
    "show_detailed_progress": False,
}


def _is_machine_local(key: str) -> bool:
    return key in _MACHINE_LOCAL_KEYS or bool(_CONNECTION_KEY_RE.search(key))


def scrub_settings(value):
    """Recursively drop machine-local and connection-reference keys."""
    if isinstance(value, dict):
        return {k: scrub_settings(v) for k, v in value.items() if not _is_machine_local(k)}
    if isinstance(value, list):
        return [scrub_settings(item) for item in value]
    return value


def rewrite_read_path(received_file: dict) -> tuple[dict, bool]:
    """``(received_file, needs_local_file)`` with local paths reduced to a basename.

    An http(s) source travels verbatim — the browser refetches it. Anything else
    is a path on the sender's machine: only the file name survives, and the
    recipient has to supply the file.
    """
    rewritten = dict(received_file)
    path = rewritten.get("path") or ""
    if isinstance(path, str) and path.lower().startswith(("http://", "https://")):
        return rewritten, False
    rewritten["path"] = Path(path).name if path else path
    return rewritten, True


def wasm_flow_settings(description: str | None) -> dict:
    """The flow settings the browser build always runs with."""
    return {"description": description or "", **_WASM_FLOW_SETTINGS}


def _prune(value, depth: int):
    if depth < 0 or not isinstance(value, dict):
        return value
    return {
        key: (item if key in _KEEP_NULLS_KEYS else _prune(item, depth - 1))
        for key, item in value.items()
        if item is not None
    }


def drop_nulls(node: dict) -> dict:
    """Prune ``None`` values from a node and one level into its ``setting_input``."""
    pruned = {key: value for key, value in node.items() if value is not None}
    settings = pruned.get("setting_input")
    if isinstance(settings, dict):
        pruned["setting_input"] = _prune(settings, 1)
    return pruned
