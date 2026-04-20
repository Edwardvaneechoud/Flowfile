"""Worker-side HTTP client for the GlobalArtifact prepare → upload → finalize flow.

Mirrors the shape of ``kernel_runtime.flowfile_client.publish_global`` /
``get_global`` but lives entirely under ``flowfile_core`` so the worker
can import it without taking a dependency on the kernel runtime.

Authentication: the worker sends ``X-Internal-Token`` from the
``FLOWFILE_INTERNAL_TOKEN`` env var, which Core verifies via
``get_user_or_internal_service``.
"""

from __future__ import annotations

import hashlib
import os
import pickle
from pathlib import Path
from typing import Any

import httpx


class ArtefactIOError(RuntimeError):
    """Raised when artefact prepare/upload/finalize/download fails."""


def _core_url() -> str:
    return os.environ.get("FLOWFILE_CORE_URL", "http://localhost:63578").rstrip("/")


def _auth_headers() -> dict[str, str]:
    token = os.environ.get("FLOWFILE_INTERNAL_TOKEN")
    if not token:
        raise ArtefactIOError(
            "FLOWFILE_INTERNAL_TOKEN is not set; the worker cannot authenticate "
            "with Core for artefact publish/download."
        )
    return {"X-Internal-Token": token}


def _serialize_to_bytes(obj: Any) -> tuple[bytes, str]:
    """Pickle ``obj`` and return ``(blob, sha256)``."""
    blob = pickle.dumps(obj)
    return blob, hashlib.sha256(blob).hexdigest()


def publish(
    *,
    name: str,
    obj: Any,
    source_registration_id: int,
    source_flow_id: int | None = None,
    source_node_id: int | None = None,
    output_schema: list[dict] | None = None,
    description: str | None = None,
    tags: list[str] | None = None,
) -> int:
    """Publish ``obj`` as a versioned global artefact and return the artefact id."""
    python_type = f"{type(obj).__module__}.{type(obj).__name__}"
    python_module = type(obj).__module__

    headers = _auth_headers()
    with httpx.Client(timeout=60.0, headers=headers) as client:
        resp = client.post(
            f"{_core_url()}/artifacts/prepare-upload",
            json={
                "name": name,
                "source_registration_id": source_registration_id,
                "serialization_format": "pickle",
                "description": description,
                "tags": tags or [],
                "namespace_id": None,
                "source_flow_id": source_flow_id,
                "source_node_id": source_node_id,
                "source_kernel_id": None,
                "python_type": python_type,
                "python_module": python_module,
            },
        )
        resp.raise_for_status()
        target = resp.json()

        blob, sha256 = _serialize_to_bytes(obj)
        size_bytes = len(blob)

        if target["method"] == "file":
            staging_path = Path(target["path"])
            staging_path.parent.mkdir(parents=True, exist_ok=True)
            staging_path.write_bytes(blob)
        else:
            upload_resp = client.put(
                target["path"],
                content=blob,
                headers={"Content-Type": "application/octet-stream"},
                timeout=600.0,
            )
            upload_resp.raise_for_status()

        finalize_body = {
            "artifact_id": target["artifact_id"],
            "storage_key": target["storage_key"],
            "sha256": sha256,
            "size_bytes": size_bytes,
            "output_schema": output_schema,
        }
        resp = client.post(
            f"{_core_url()}/artifacts/finalize",
            json=finalize_body,
        )
        if resp.status_code >= 400:
            raise ArtefactIOError(
                f"Artifact finalize failed ({resp.status_code}): {resp.text}. "
                f"Request body: {finalize_body}"
            )

    return int(target["artifact_id"])


def get_artefact_metadata(name: str, version: int | None = None) -> dict:
    """Return artefact metadata (incl. ``output_schema`` and ``download_source``)."""
    headers = _auth_headers()
    params = {}
    if version is not None:
        params["version"] = version
    with httpx.Client(timeout=30.0, headers=headers) as client:
        resp = client.get(f"{_core_url()}/artifacts/by-name/{name}", params=params)
        if resp.status_code == 404:
            raise KeyError(f"Artifact '{name}' not found")
        resp.raise_for_status()
        return resp.json()


def download_artifact(name: str, version: int | None = None) -> bytes:
    """Fetch the raw bytes of the named artefact (Core handles auth + lookup)."""
    meta = get_artefact_metadata(name, version)
    download = meta.get("download_source") or {}
    method = download.get("method")
    path = download.get("path")
    if not method or not path:
        raise ArtefactIOError(f"Artifact '{name}' has no download_source: {meta}")

    if method == "file":
        return Path(path).read_bytes()

    headers = _auth_headers()
    with httpx.Client(timeout=600.0, headers=headers) as client:
        resp = client.get(path)
        resp.raise_for_status()
        return resp.content


def load_estimator(blob: bytes) -> Any:
    """Inverse of :func:`_serialize_to_bytes`."""
    return pickle.loads(blob)
