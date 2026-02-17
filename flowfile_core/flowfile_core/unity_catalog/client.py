"""Unity Catalog REST API client.

Provides a synchronous Python client for interacting with the Unity Catalog
OSS REST API (base path: /api/2.1/unity-catalog/).
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

from flowfile_core.unity_catalog.schemas import (
    CatalogInfo,
    CatalogList,
    ColumnInfo,
    CreateCatalog,
    CreateSchema,
    CreateTable,
    CredentialOperation,
    DataSourceFormat,
    SchemaInfo,
    SchemaList,
    TableInfo,
    TableList,
    TableType,
    TemporaryCredentialResponse,
    TemporaryPathCredentialRequest,
    TemporaryTableCredentialRequest,
    VolumeInfo,
    VolumeList,
)

logger = logging.getLogger(__name__)

API_PREFIX = "/api/2.1/unity-catalog"


class UnityCatalogError(Exception):
    """Raised when a UC API call fails."""

    def __init__(self, message: str, status_code: int | None = None):
        self.status_code = status_code
        super().__init__(message)


class UnityCatalogClient:
    """Client for the Unity Catalog OSS REST API.

    Args:
        server_url: Base URL of the UC server (e.g. ``http://localhost:8080``).
        auth_token: Optional bearer token for authentication.
        timeout: HTTP timeout in seconds.
    """

    def __init__(
        self,
        server_url: str,
        auth_token: str | None = None,
        timeout: float = 30.0,
    ):
        self.server_url = server_url.rstrip("/")
        self.base_url = f"{self.server_url}{API_PREFIX}"
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"
        self._client = httpx.Client(
            base_url=self.base_url,
            headers=headers,
            timeout=timeout,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> UnityCatalogClient:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        try:
            resp = self._client.request(method, path, params=params, json=json_body)
            resp.raise_for_status()
            if resp.status_code == 204:
                return {}
            return resp.json()
        except httpx.HTTPStatusError as exc:
            detail = ""
            try:
                detail = exc.response.json().get("message", exc.response.text)
            except Exception:
                detail = exc.response.text
            raise UnityCatalogError(
                f"UC API error ({exc.response.status_code}): {detail}",
                status_code=exc.response.status_code,
            ) from exc
        except httpx.RequestError as exc:
            raise UnityCatalogError(f"UC connection error: {exc}") from exc

    def _get(self, path: str, **params: Any) -> dict[str, Any]:
        return self._request("GET", path, params={k: v for k, v in params.items() if v is not None})

    def _post(self, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        return self._request("POST", path, json_body=body)

    def _patch(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        return self._request("PATCH", path, json_body=body)

    def _delete(self, path: str) -> dict[str, Any]:
        return self._request("DELETE", path)

    # ------------------------------------------------------------------
    # Catalogs
    # ------------------------------------------------------------------

    def list_catalogs(self) -> list[CatalogInfo]:
        data = self._get("/catalogs")
        return CatalogList(**data).catalogs

    def get_catalog(self, name: str) -> CatalogInfo:
        data = self._get(f"/catalogs/{name}")
        return CatalogInfo(**data)

    def create_catalog(self, catalog: CreateCatalog) -> CatalogInfo:
        data = self._post("/catalogs", catalog.model_dump(exclude_none=True))
        return CatalogInfo(**data)

    def delete_catalog(self, name: str) -> None:
        self._delete(f"/catalogs/{name}")

    # ------------------------------------------------------------------
    # Schemas
    # ------------------------------------------------------------------

    def list_schemas(self, catalog_name: str) -> list[SchemaInfo]:
        data = self._get("/schemas", catalog_name=catalog_name)
        return SchemaList(**data).schemas

    def get_schema(self, full_name: str) -> SchemaInfo:
        data = self._get(f"/schemas/{full_name}")
        return SchemaInfo(**data)

    def create_schema(self, schema: CreateSchema) -> SchemaInfo:
        data = self._post("/schemas", schema.model_dump(exclude_none=True))
        return SchemaInfo(**data)

    def delete_schema(self, full_name: str) -> None:
        self._delete(f"/schemas/{full_name}")

    # ------------------------------------------------------------------
    # Tables
    # ------------------------------------------------------------------

    def list_tables(self, catalog_name: str, schema_name: str) -> list[TableInfo]:
        data = self._get("/tables", catalog_name=catalog_name, schema_name=schema_name)
        return TableList(**data).tables

    def get_table(self, full_name: str) -> TableInfo:
        data = self._get(f"/tables/{full_name}")
        return TableInfo(**data)

    def create_table(self, table: CreateTable) -> TableInfo:
        data = self._post("/tables", table.model_dump(exclude_none=True))
        return TableInfo(**data)

    def delete_table(self, full_name: str) -> None:
        self._delete(f"/tables/{full_name}")

    # ------------------------------------------------------------------
    # Volumes
    # ------------------------------------------------------------------

    def list_volumes(self, catalog_name: str, schema_name: str) -> list[VolumeInfo]:
        data = self._get("/volumes", catalog_name=catalog_name, schema_name=schema_name)
        return VolumeList(**data).volumes

    # ------------------------------------------------------------------
    # Credential Vending
    # ------------------------------------------------------------------

    def get_temporary_table_credentials(
        self,
        table_id: str,
        operation: CredentialOperation = CredentialOperation.READ,
    ) -> TemporaryCredentialResponse:
        req = TemporaryTableCredentialRequest(table_id=table_id, operation=operation)
        data = self._post("/temporary-table-credentials", req.model_dump())
        return TemporaryCredentialResponse(**data)

    def get_temporary_path_credentials(
        self,
        url: str,
        operation: CredentialOperation = CredentialOperation.READ,
    ) -> TemporaryCredentialResponse:
        req = TemporaryPathCredentialRequest(url=url, operation=operation)
        data = self._post("/temporary-path-credentials", req.model_dump())
        return TemporaryCredentialResponse(**data)

    # ------------------------------------------------------------------
    # Convenience / high-level helpers
    # ------------------------------------------------------------------

    def test_connection(self) -> bool:
        """Return True if the UC server is reachable and responds to /catalogs."""
        try:
            self.list_catalogs()
            return True
        except UnityCatalogError:
            return False

    def resolve_table(
        self,
        full_name: str,
    ) -> tuple[TableInfo, TemporaryCredentialResponse | None]:
        """Fetch table metadata and (optionally) temporary credentials.

        Returns a tuple of (table_info, temp_credentials).
        If credential vending is not available, temp_credentials will be None.
        """
        table = self.get_table(full_name)
        creds: TemporaryCredentialResponse | None = None
        if table.table_id:
            try:
                creds = self.get_temporary_table_credentials(table.table_id)
            except UnityCatalogError as exc:
                logger.warning(
                    "Credential vending not available for table %s: %s", full_name, exc
                )
        return table, creds

    def register_external_table(
        self,
        catalog_name: str,
        schema_name: str,
        table_name: str,
        storage_location: str,
        columns: list[ColumnInfo],
        data_source_format: DataSourceFormat = DataSourceFormat.DELTA,
        comment: str | None = None,
    ) -> TableInfo:
        """Register an external table pointing at an existing storage location."""
        create_req = CreateTable(
            name=table_name,
            catalog_name=catalog_name,
            schema_name=schema_name,
            table_type=TableType.EXTERNAL,
            data_source_format=data_source_format,
            columns=columns,
            storage_location=storage_location,
            comment=comment,
        )
        return self.create_table(create_req)
