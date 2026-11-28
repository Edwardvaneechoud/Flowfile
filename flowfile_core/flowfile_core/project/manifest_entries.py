"""Pydantic v2 models for the on-disk manifest entry contract.

Used by both projection (build → dump) and the importer (parse → validate), so a malformed or
hand-edited manifest file is rejected per-entry (skip-and-continue) instead of raising a bare
``KeyError``/``TypeError`` that aborts the whole import. ``extra="ignore"`` keeps forward
compatibility with fields a newer version may add.
"""

from __future__ import annotations

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from flowfile_core.project.secrets_resolver import placeholder_name


class _Entry(BaseModel):
    model_config = ConfigDict(extra="ignore")


class DatabaseConnectionEntry(_Entry):
    connection_name: str
    database_type: str = "postgresql"
    host: str | None = None
    port: int | None = None
    database: str | None = None
    username: str = ""
    ssl_enabled: bool = False
    password: str | None = None
    auth_method: str | None = None
    role: str | None = None
    warehouse: str | None = None


class SharePointConnectionEntry(_Entry):
    connection_uuid: UUID
    connection_name: str = Field(min_length=1, max_length=255)
    tenant_id: UUID
    client_id: UUID
    auth_method: Literal["client_secret", "certificate"]
    default_site_url: str | None = Field(default=None, max_length=2048)
    credential: str

    @field_validator("credential")
    @classmethod
    def require_reference(cls, value: str) -> str:
        if placeholder_name(value) is None:
            raise ValueError("Expected a secret reference")
        return value


class CloudConnectionEntry(_Entry):
    connection_name: str
    storage_type: str
    auth_method: str
    aws_region: str | None = None
    aws_access_key_id: str | None = None
    aws_role_arn: str | None = None
    aws_allow_unsafe_html: bool | None = None
    aws_profile: str | None = None
    azure_account_name: str | None = None
    azure_tenant_id: str | None = None
    azure_client_id: str | None = None
    gcs_project_id: str | None = None
    endpoint_url: str | None = None
    verify_ssl: bool = True
    aws_secret_access_key: str | None = None
    aws_session_token: str | None = None
    azure_account_key: str | None = None
    azure_client_secret: str | None = None
    azure_sas_token: str | None = None
    gcs_service_account_key: str | None = None


class SchemaColumn(_Entry):
    name: str
    dtype: str
