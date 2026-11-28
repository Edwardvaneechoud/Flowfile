"""Request and response models of the Delta Sharing management routes (``/delta-sharing/manage``)."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

NAME_PATTERN = r"^[A-Za-z0-9_][A-Za-z0-9_-]{0,127}$"


class DeltaShareCreate(BaseModel):
    name: str = Field(pattern=NAME_PATTERN)
    description: str | None = None


class DeltaShareUpdate(BaseModel):
    name: str | None = Field(default=None, pattern=NAME_PATTERN)
    description: str | None = None


class DeltaShareTableAdd(BaseModel):
    table_id: int
    schema_name: str | None = Field(default=None, pattern=NAME_PATTERN)
    table_name: str | None = Field(default=None, pattern=NAME_PATTERN)
    share_history: bool = True


class DeltaShareTableUpdate(BaseModel):
    schema_name: str | None = Field(default=None, pattern=NAME_PATTERN)
    table_name: str | None = Field(default=None, pattern=NAME_PATTERN)
    share_history: bool | None = None


class DeltaShareGrant(BaseModel):
    consumer_id: int


class DeltaShareProfileCreate(BaseModel):
    consumer_id: int
    endpoint: str | None = Field(default=None, description="Base URL the recipient reaches core's API at")
    key_name: str | None = None
    expires_in_days: int | None = Field(default=None, ge=1, le=3650)


class DeltaShareTableOut(BaseModel):
    id: int
    table_id: int
    catalog_name: str
    schema_name: str
    table_name: str
    share_history: bool
    served: bool
    refusal: str | None = None


class DeltaShareRecipientOut(BaseModel):
    consumer_id: int
    name: str
    enabled: bool


class DeltaShareOut(BaseModel):
    id: int
    name: str
    description: str | None = None
    owner_id: int
    uuid: str
    created_at: datetime
    can_manage: bool
    tables: list[DeltaShareTableOut] = []
    recipients: list[DeltaShareRecipientOut] = []


class DeltaShareEligibleTableOut(BaseModel):
    table_id: int
    catalog_name: str
    default_schema_name: str
    default_table_name: str
    refusal: str | None = None


class DeltaShareProfileOut(BaseModel):
    profile: dict[str, Any]
    file_name: str
    key_prefix: str
    table_urls: list[str]


class DeltaSharingStatus(BaseModel):
    enabled: bool
    endpoint: str | None = None
