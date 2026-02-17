"""Credential provider that bridges Unity Catalog credential vending to Polars storage options.

When reading or writing tables managed by Unity Catalog, this module:
1. Resolves the table's storage location from UC metadata
2. Requests temporary credentials via UC credential vending
3. Returns storage_options dicts compatible with Polars / object_store
"""

from __future__ import annotations

import logging
from typing import Any

from flowfile_core.unity_catalog.client import UnityCatalogClient, UnityCatalogError
from flowfile_core.unity_catalog.schemas import (
    CredentialOperation,
    DataSourceFormat,
    TableInfo,
    TemporaryCredentialResponse,
)

logger = logging.getLogger(__name__)


def _build_storage_options_from_creds(
    creds: TemporaryCredentialResponse,
) -> dict[str, Any]:
    """Convert a UC TemporaryCredentialResponse into Polars-compatible storage_options."""
    if creds.aws_temp_credentials:
        return {
            "aws_access_key_id": creds.aws_temp_credentials.access_key_id,
            "aws_secret_access_key": creds.aws_temp_credentials.secret_access_key,
            "aws_session_token": creds.aws_temp_credentials.session_token,
        }
    if creds.azure_user_delegation_sas:
        return {
            "sas_token": creds.azure_user_delegation_sas.sas_token,
        }
    if creds.gcp_oauth_token:
        return {
            "token": creds.gcp_oauth_token.oauth_token,
        }
    return {}


class ResolvedTable:
    """A fully-resolved UC table with storage location, format, and credentials."""

    def __init__(
        self,
        table_info: TableInfo,
        storage_options: dict[str, Any],
        temp_credentials: TemporaryCredentialResponse | None = None,
    ):
        self.table_info = table_info
        self.storage_location = table_info.storage_location or ""
        self.data_source_format = table_info.data_source_format
        self.storage_options = storage_options
        self.temp_credentials = temp_credentials
        self.columns = table_info.columns

    @property
    def polars_format(self) -> str:
        """Map UC data_source_format to the string Polars uses."""
        mapping = {
            DataSourceFormat.DELTA: "delta",
            DataSourceFormat.PARQUET: "parquet",
            DataSourceFormat.CSV: "csv",
            DataSourceFormat.JSON: "json",
            DataSourceFormat.ICEBERG: "iceberg",
        }
        return mapping.get(self.data_source_format, "parquet")


def resolve_uc_table(
    client: UnityCatalogClient,
    catalog_name: str,
    schema_name: str,
    table_name: str,
    operation: CredentialOperation = CredentialOperation.READ,
) -> ResolvedTable:
    """Resolve a UC table reference to a storage location with credentials.

    This is the main entry point for reading UC tables. It:
    1. Fetches table metadata (storage_location, format, columns)
    2. Attempts credential vending for temporary storage credentials
    3. Falls back to empty storage_options if vending is unavailable

    Args:
        client: An authenticated UnityCatalogClient instance.
        catalog_name: The catalog containing the table.
        schema_name: The schema containing the table.
        table_name: The table name.
        operation: READ or READ_WRITE.

    Returns:
        A ResolvedTable with everything needed to read/write via Polars.
    """
    full_name = f"{catalog_name}.{schema_name}.{table_name}"
    logger.info("Resolving UC table: %s", full_name)

    table_info = client.get_table(full_name)

    if not table_info.storage_location:
        raise UnityCatalogError(
            f"Table {full_name} has no storage_location. "
            "Only EXTERNAL tables with a storage location are supported."
        )

    # Attempt credential vending
    storage_options: dict[str, Any] = {}
    temp_creds: TemporaryCredentialResponse | None = None

    if table_info.table_id:
        try:
            temp_creds = client.get_temporary_table_credentials(
                table_id=table_info.table_id,
                operation=operation,
            )
            storage_options = _build_storage_options_from_creds(temp_creds)
            logger.info(
                "Obtained temporary credentials for table %s (expires: %s)",
                full_name,
                temp_creds.expiration_time,
            )
        except UnityCatalogError as exc:
            logger.warning(
                "Credential vending unavailable for %s, proceeding without: %s",
                full_name,
                exc,
            )

    return ResolvedTable(
        table_info=table_info,
        storage_options=storage_options,
        temp_credentials=temp_creds,
    )


def build_client_from_connection(
    server_url: str,
    auth_token: str | None = None,
) -> UnityCatalogClient:
    """Create a UnityCatalogClient from connection parameters."""
    return UnityCatalogClient(
        server_url=server_url,
        auth_token=auth_token,
    )
