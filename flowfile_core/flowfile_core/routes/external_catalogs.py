"""Catalogs kept elsewhere: add an Iceberg REST catalog by its address and sign-in, test it, keep its schemas in line,
and list its tables live.

No route answers 401 for a catalog's refusal, since the frontend reads a 401 as its own session ending: a failed test
is a 200 with ``ok`` false, and every other refusal a 422 with one sentence.
"""

from fastapi import APIRouter, Depends, Query, Response

from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.catalog import CatalogService
from flowfile_core.routes.catalog import get_catalog_service, handle_catalog_exceptions
from flowfile_core.schemas.catalog_schema import (
    ExternalCatalogCreate,
    ExternalCatalogSyncResult,
    ExternalCatalogTest,
    ExternalCatalogTestResult,
    ExternalCatalogUpdate,
    ExternalTableDetail,
    ExternalTableList,
    NamespaceOut,
)

router = APIRouter(prefix="/catalog/external", tags=["catalog"], dependencies=[Depends(get_current_active_user)])


@router.post("/test", response_model=ExternalCatalogTestResult)
@handle_catalog_exceptions()
def test_external_catalog(
    body: ExternalCatalogTest,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    return service.test_external_catalog(body, current_user.id)


@router.post("", response_model=NamespaceOut, status_code=201)
@handle_catalog_exceptions()
def create_external_catalog(
    body: ExternalCatalogCreate,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    return service.create_external_catalog(body, current_user.id)


@router.put("/{namespace_id}", response_model=NamespaceOut)
@handle_catalog_exceptions()
def update_external_catalog(
    namespace_id: int,
    body: ExternalCatalogUpdate,
    service: CatalogService = Depends(get_catalog_service),
):
    return service.update_external_catalog(namespace_id, body)


@router.delete("/{namespace_id}", status_code=204)
@handle_catalog_exceptions()
def delete_external_catalog(namespace_id: int, service: CatalogService = Depends(get_catalog_service)):
    service.delete_external_catalog(namespace_id)
    return Response(status_code=204)


@router.post("/{namespace_id}/sync", response_model=ExternalCatalogSyncResult)
@handle_catalog_exceptions()
def sync_external_catalog(
    namespace_id: int,
    force: bool = Query(False),
    service: CatalogService = Depends(get_catalog_service),
):
    return service.sync_external_catalog(namespace_id, force=force)


@router.get("/{namespace_id}/schemas/{schema_id}/tables", response_model=ExternalTableList)
@handle_catalog_exceptions()
def list_external_tables(
    namespace_id: int,
    schema_id: int,
    refresh: bool = Query(False),
    service: CatalogService = Depends(get_catalog_service),
):
    return service.list_external_tables(namespace_id, schema_id, refresh=refresh)


@router.get("/{namespace_id}/schemas/{schema_id}/table", response_model=ExternalTableDetail)
@handle_catalog_exceptions()
def get_external_table(
    namespace_id: int,
    schema_id: int,
    name: str = Query(...),
    service: CatalogService = Depends(get_catalog_service),
):
    return service.get_external_table(namespace_id, schema_id, name)
