"""Converters API endpoints.

Imports a foreign workflow format as a Flowfile flow. Today that is Alteryx `.yxmd`:
the upload is converted to a `FlowfileData`, written into the flows directory as YAML
and opened through the regular import path, so the caller gets a flow id it can hand
straight to the designer plus a report of everything that needs finishing by hand.
"""

import io
import os
import re
from pathlib import Path

import yaml
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from pydantic import BaseModel

from flowfile_core import flow_file_handler
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.configs import logger
from flowfile_core.flowfile.converters.alteryx import ConversionReport, YxmdParseError, convert_yxmd
from flowfile_core.routes.file_manager import _open_unique
from shared.storage_config import storage

router = APIRouter(dependencies=[Depends(get_current_active_user)])

ALLOWED_EXTENSIONS = {"yxmd", "xml"}
MAX_YXMD_SIZE = 20 * 1024 * 1024  # 20 MB
CHUNK_SIZE = 1024 * 1024

# The flow name is a free-form label; only the derived filename must be filesystem-safe.
_FLOW_STEM_DISALLOWED_RE = re.compile(r"[^A-Za-z0-9_-]+")


class AlteryxImportResponse(BaseModel):
    flow_id: int
    flow_path: str
    report: ConversionReport


async def _read_upload(file: UploadFile) -> bytes:
    """Buffer the upload in memory, rejecting it the moment it grows past MAX_YXMD_SIZE."""
    buffer = io.BytesIO()
    size = 0
    while chunk := await file.read(CHUNK_SIZE):
        size += len(chunk)
        if size > MAX_YXMD_SIZE:
            raise HTTPException(400, f"File too large. Maximum size is {MAX_YXMD_SIZE // (1024 * 1024)} MB")
        buffer.write(chunk)
    return buffer.getvalue()


@router.post("/alteryx", response_model=AlteryxImportResponse)
async def import_alteryx_workflow(
    file: UploadFile = File(...), current_user=Depends(get_current_active_user)
) -> AlteryxImportResponse:
    """Convert an uploaded Alteryx workflow into a Flowfile flow and open it."""
    if not file.filename:
        raise HTTPException(400, "No filename provided")

    safe_name = Path(file.filename).name
    if not safe_name or ".." in safe_name:
        raise HTTPException(400, "Invalid filename")

    suffix = Path(safe_name).suffix.lstrip(".").lower()
    if suffix not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            400,
            f"File type '.{suffix}' not allowed. Allowed types: {', '.join(sorted(ALLOWED_EXTENSIONS))}",
        )

    data = await _read_upload(file)

    try:
        result = convert_yxmd(data, source_name=safe_name)
    except YxmdParseError as exc:
        raise HTTPException(400, str(exc)) from exc

    flows_dir = storage.flows_directory
    flows_dir.mkdir(parents=True, exist_ok=True)
    stem = _FLOW_STEM_DISALLOWED_RE.sub("_", Path(safe_name).stem).strip("_-") or "imported_alteryx_flow"
    flow_path, fd = _open_unique(flows_dir, f"{stem}.yaml")

    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            yaml.dump(
                result.flow_data.model_dump(mode="json"),
                handle,
                default_flow_style=False,
                sort_keys=False,
                allow_unicode=True,
            )
    except BaseException:
        flow_path.unlink(missing_ok=True)
        raise

    try:
        flow_id = flow_file_handler.import_flow(flow_path, user_id=current_user.id if current_user else None)
    except Exception as exc:
        flow_path.unlink(missing_ok=True)
        logger.exception("Opening the converted Alteryx flow failed (source=%s)", safe_name)
        raise HTTPException(502, f"Opening the converted flow failed: {type(exc).__name__}: {exc}") from exc

    return AlteryxImportResponse(flow_id=flow_id, flow_path=str(flow_path), report=result.report)
