"""Pydantic models for Delta Lake metadata shared across packages."""

from __future__ import annotations

from pydantic import BaseModel


class DeltaVersionCommit(BaseModel):
    """A single version entry from a Delta table's transaction log."""

    version: int
    timestamp: str | None = None
    operation: str | None = None
    parameters: dict | None = None
