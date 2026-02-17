"""Pydantic models for Unity Catalog REST API objects.

These models mirror the Unity Catalog OSS API data structures for catalogs,
schemas, tables, credentials, and credential vending responses.
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, SecretStr


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class TableType(str, Enum):
    MANAGED = "MANAGED"
    EXTERNAL = "EXTERNAL"


class DataSourceFormat(str, Enum):
    DELTA = "DELTA"
    CSV = "CSV"
    JSON = "JSON"
    AVRO = "AVRO"
    PARQUET = "PARQUET"
    ORC = "ORC"
    TEXT = "TEXT"
    ICEBERG = "ICEBERG"


class VolumeType(str, Enum):
    MANAGED = "MANAGED"
    EXTERNAL = "EXTERNAL"


class CredentialOperation(str, Enum):
    READ = "READ"
    READ_WRITE = "READ_WRITE"


# ---------------------------------------------------------------------------
# UC Object Models
# ---------------------------------------------------------------------------


class ColumnInfo(BaseModel):
    name: str
    type_name: str
    type_text: str
    type_json: str | None = None
    position: int
    comment: str | None = None
    nullable: bool = True
    partition_index: int | None = None
    type_precision: int | None = None
    type_scale: int | None = None


class CatalogInfo(BaseModel):
    name: str
    comment: str | None = None
    properties: dict[str, str] | None = None
    owner: str | None = None
    created_at: int | None = None
    created_by: str | None = None
    updated_at: int | None = None
    updated_by: str | None = None
    id: str | None = None


class SchemaInfo(BaseModel):
    name: str
    catalog_name: str
    comment: str | None = None
    properties: dict[str, str] | None = None
    full_name: str | None = None
    owner: str | None = None
    created_at: int | None = None
    created_by: str | None = None
    updated_at: int | None = None
    updated_by: str | None = None
    schema_id: str | None = None


class TableInfo(BaseModel):
    name: str
    catalog_name: str
    schema_name: str
    table_type: TableType
    data_source_format: DataSourceFormat
    columns: list[ColumnInfo] = Field(default_factory=list)
    storage_location: str | None = None
    comment: str | None = None
    properties: dict[str, str] | None = None
    owner: str | None = None
    created_at: int | None = None
    created_by: str | None = None
    updated_at: int | None = None
    updated_by: str | None = None
    table_id: str | None = None

    @property
    def full_name(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.name}"


class VolumeInfo(BaseModel):
    name: str
    catalog_name: str
    schema_name: str
    volume_type: VolumeType
    storage_location: str | None = None
    full_name: str | None = None
    comment: str | None = None
    owner: str | None = None
    created_at: int | None = None
    volume_id: str | None = None


# ---------------------------------------------------------------------------
# Create / Update request bodies
# ---------------------------------------------------------------------------


class CreateCatalog(BaseModel):
    name: str
    comment: str | None = None
    properties: dict[str, str] | None = None


class CreateSchema(BaseModel):
    name: str
    catalog_name: str
    comment: str | None = None
    properties: dict[str, str] | None = None


class CreateTable(BaseModel):
    name: str
    catalog_name: str
    schema_name: str
    table_type: TableType = TableType.EXTERNAL
    data_source_format: DataSourceFormat = DataSourceFormat.DELTA
    columns: list[ColumnInfo] = Field(default_factory=list)
    storage_location: str
    comment: str | None = None
    properties: dict[str, str] | None = None


# ---------------------------------------------------------------------------
# List response wrappers
# ---------------------------------------------------------------------------


class CatalogList(BaseModel):
    catalogs: list[CatalogInfo] = Field(default_factory=list)


class SchemaList(BaseModel):
    schemas: list[SchemaInfo] = Field(default_factory=list)


class TableList(BaseModel):
    tables: list[TableInfo] = Field(default_factory=list)


class VolumeList(BaseModel):
    volumes: list[VolumeInfo] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Credential Vending
# ---------------------------------------------------------------------------


class AwsTempCredentials(BaseModel):
    access_key_id: str
    secret_access_key: str
    session_token: str


class AzureUserDelegationSas(BaseModel):
    sas_token: str


class GcpOAuthToken(BaseModel):
    oauth_token: str


class TemporaryCredentialResponse(BaseModel):
    aws_temp_credentials: AwsTempCredentials | None = None
    azure_user_delegation_sas: AzureUserDelegationSas | None = None
    gcp_oauth_token: GcpOAuthToken | None = None
    expiration_time: int | None = None


class TemporaryTableCredentialRequest(BaseModel):
    table_id: str
    operation: CredentialOperation = CredentialOperation.READ


class TemporaryPathCredentialRequest(BaseModel):
    url: str
    operation: CredentialOperation = CredentialOperation.READ


# ---------------------------------------------------------------------------
# Connection settings (stored in Flowfile DB)
# ---------------------------------------------------------------------------


class UnityCatalogConnectionInput(BaseModel):
    """User-facing input for creating/updating a UC connection."""

    connection_name: str = Field(..., min_length=1)
    server_url: str = Field(..., description="UC server base URL, e.g. http://localhost:8080")
    auth_token: SecretStr | None = Field(
        None, description="Bearer token for UC server authentication"
    )
    default_catalog: str | None = Field(
        None, description="Default catalog to browse when opening the connection"
    )
    credential_vending_enabled: bool = Field(
        True, description="Use UC credential vending for storage access (recommended)"
    )


class UnityCatalogConnectionInterface(BaseModel):
    """Safe-for-frontend representation (no secrets)."""

    connection_name: str
    server_url: str
    default_catalog: str | None = None
    credential_vending_enabled: bool = True


# ---------------------------------------------------------------------------
# Node settings (used in flow graph)
# ---------------------------------------------------------------------------


class UnityCatalogTableRef(BaseModel):
    """Reference to a table in Unity Catalog."""

    catalog_name: str = ""
    schema_name: str = ""
    table_name: str = ""

    @property
    def full_name(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.table_name}"


class UnityCatalogReadSettings(BaseModel):
    """Settings for the UC table reader node."""

    connection_name: str | None = None
    table_ref: UnityCatalogTableRef = Field(default_factory=UnityCatalogTableRef)
    # Resolved at execution time by credential vending:
    resolved_storage_location: str | None = None
    resolved_format: DataSourceFormat | None = None


class UnityCatalogWriteSettings(BaseModel):
    """Settings for the UC table writer node."""

    connection_name: str | None = None
    table_ref: UnityCatalogTableRef = Field(default_factory=UnityCatalogTableRef)
    data_source_format: DataSourceFormat = DataSourceFormat.DELTA
    write_mode: str = "overwrite"  # overwrite | append
    register_table: bool = True
    table_comment: str | None = None
