"""Check a clean run that came back from a notebook kernel before a push reconciles it.

The kernel ran the cells as Python in its own container, so its result is data from outside core: it
must fit the size bounds, name only the request's cells, parse as a ``FlowfileData``, and its file
paths were kept as written there (``input_schema.keep_paths_as_written``), so a path written as the kernel
sees it is turned back into the host path and absolute paths are recomputed here, on the host.
``plan_push`` then applies the same refusals as for any other runner.
"""

from __future__ import annotations

import copy
import json

from pydantic import ValidationError

from flowfile_core.configs import logger
from flowfile_core.kernel.notebook_mounts import host_side
from flowfile_core.notebook.allowlist import BOUNDS
from flowfile_core.notebook.bridge import CleanRunRequest, CleanRunResult
from flowfile_core.schemas.input_schema import OutputSettings, ReceivedTable
from flowfile_core.schemas.schemas import FlowfileData

MAX_PAYLOAD_BYTES = 4 * BOUNDS["bytes_per_request"]


def _refused(message: str) -> CleanRunResult:
    return CleanRunResult(error=message, kind="refused")


def _path_slots(node: dict, settings: dict) -> list[tuple[dict, str]]:
    """(dict, key) of each local path ``node`` stores: a read's file, a write's target, a cloud path, a folder."""
    slots = [
        (settings.get("received_file"), "path"),
        (settings.get("output_settings"), "directory"),
        (settings.get("cloud_storage_settings"), "resource_path"),
    ]
    if node.get("type") == "list_files":
        slots.append((settings, "path"))
    return [(holder, key) for holder, key in slots if isinstance(holder, dict) and isinstance(holder.get(key), str)]


def host_file_paths(flowfile_data: dict, folders: dict[str, str] | None = None) -> dict:
    """A copy of ``flowfile_data`` whose file readers and writers carry the absolute path the host resolves.

    A path under one of ``folders`` (kernel folder -> host folder, ``KernelManager.host_folders``) is
    turned back into the host path first: the canvas derives ``abs_file_path`` from the stored path.
    """
    data = copy.deepcopy(flowfile_data)
    for node in data.get("nodes") or []:
        settings = node.get("setting_input")
        if not isinstance(settings, dict):
            continue
        if folders:
            for holder, key in _path_slots(node, settings):
                holder[key] = host_side(holder[key], folders) or holder[key]
        received = settings.get("received_file")
        output = settings.get("output_settings")
        try:
            if isinstance(received, dict) and received.get("path"):
                received["abs_file_path"] = ReceivedTable.model_validate(
                    {**received, "abs_file_path": None}
                ).abs_file_path
            if isinstance(output, dict) and output.get("directory"):
                output["abs_file_path"] = OutputSettings.model_validate(output).abs_file_path
        except (ValidationError, OSError, ValueError):
            logger.debug(f"notebook push: kept the kernel's path of node {node.get('id')}", exc_info=True)
    return data


def validate_clean_run(
    result: CleanRunResult, request: CleanRunRequest, folders: dict[str, str] | None = None
) -> CleanRunResult:
    """``result`` with host file paths (``folders`` as in :func:`host_file_paths`), or a ``refused`` result when
    it is too large or malformed."""
    if result.error is not None:
        return result
    if len(json.dumps(result.flowfile_data, default=str)) > MAX_PAYLOAD_BYTES:
        return _refused(f"The kernel returned a flow larger than {MAX_PAYLOAD_BYTES} bytes")
    if len(result.flowfile_data.get("nodes") or []) > BOUNDS["nodes_per_request"]:
        return _refused(f"The kernel returned more than {BOUNDS['nodes_per_request']} nodes")
    if not set(result.node_ids_by_cell) <= {cell_id for cell_id, _ in request.cells}:
        return _refused("The kernel returned nodes for cells this notebook does not hold")
    try:
        FlowfileData.model_validate(result.flowfile_data)
    except ValidationError as exc:
        return _refused(f"The kernel returned a flow that cannot be read ({exc.error_count()} errors)")
    return result.model_copy(update={"flowfile_data": host_file_paths(result.flowfile_data, folders)})
