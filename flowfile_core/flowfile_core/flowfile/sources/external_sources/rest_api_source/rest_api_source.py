"""Core-side helpers for the REST API reader.

Unlike the GA4 reader — whose columns can be predicted from the selected
metrics/dimensions without any I/O — a generic REST API's columns are unknown
until a response is fetched. The "Fetch sample" action runs one capped request
through the worker; :func:`infer_schema_from_sample` turns the resulting
(already typed) Polars frame into the ``list[FlowfileColumn]`` the flow uses for
downstream schema introspection.

This module also owns the mapping from the UI/persisted ``NodeRestApiReader``
settings to the pure-data worker wire model, and the inline-secret resolution
shared by ``FlowGraph.add_rest_api_reader`` and the ``/rest_api/sample`` route.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from shared.rest_api.models import AuthConfig, PaginationConfig, RestApiReadSettings

if TYPE_CHECKING:
    from flowfile_core.schemas import input_schema


def infer_schema_from_sample(sample: pl.DataFrame) -> list[FlowfileColumn]:
    """Map a sampled frame's Polars schema to the flow's column representation.

    The worker returns a typed ``pl.DataFrame`` (built via ``pl.json_normalize``),
    so we simply carry each ``(name, dtype)`` over to a ``FlowfileColumn`` —
    nested JSON objects already appear as dotted column names.
    """
    return [
        FlowfileColumn.create_from_polars_dtype(column_name=name, data_type=dtype)
        for name, dtype in sample.schema.items()
    ]


def resolve_auth_secret_encrypted(auth: input_schema.RestApiAuthSettings, user_id: int | None) -> str | None:
    """Resolve the auth credential to an encrypted token for the worker.

    Prefers a stored secret referenced by ``secret_name`` (resolved via the
    user's secret store), so credentials are reusable and never persisted on the
    node. Falls back to an inline plaintext ``secret`` (encrypted with the master
    key) for programmatic use. The worker decrypts whichever token it receives.
    """
    # Deferred import keeps this leaf helper free of any import-cycle risk.
    from flowfile_core.secret_manager.secret_manager import _encrypt_with_master_key, get_encrypted_secret

    if auth.secret_name:
        return get_encrypted_secret(current_user_id=user_id, secret_name=auth.secret_name)
    if auth.secret:
        return _encrypt_with_master_key(auth.secret)
    return None


def build_rest_api_worker_settings(
    node_rest_api_reader: input_schema.NodeRestApiReader,
    secret_encrypted: str | None = None,
    sample_size: int | None = None,
) -> RestApiReadSettings:
    """Map a ``NodeRestApiReader`` to the worker wire model.

    ``secret_encrypted`` is the resolved credential token (see
    :func:`resolve_auth_secret_encrypted`); it is routed into the worker auth
    field that matches ``auth_type``.
    """
    s = node_rest_api_reader.rest_api_settings
    a = s.auth
    enc = secret_encrypted

    worker_auth = AuthConfig(
        auth_type=a.auth_type,
        api_key_name=a.api_key_name,
        api_key_location=a.api_key_location,
        basic_username=a.basic_username,
        api_key_encrypted=enc if a.auth_type == "api_key" else None,
        bearer_token_encrypted=enc if a.auth_type == "bearer" else None,
        basic_password_encrypted=enc if a.auth_type == "basic" else None,
    )
    p = s.pagination
    worker_pagination = PaginationConfig(
        pagination_type=p.pagination_type,
        offset_param=p.offset_param,
        limit_param=p.limit_param,
        page_size=p.page_size,
        page_param=p.page_param,
        start_page=p.start_page,
        cursor_param=p.cursor_param,
        cursor_location=p.cursor_location,
        cursor_response_path=p.cursor_response_path,
        initial_cursor=p.initial_cursor,
        max_pages=p.max_pages,
        max_records=p.max_records,
        page_delay_seconds=p.page_delay_seconds,
    )
    return RestApiReadSettings(
        url=s.url,
        method=s.method,
        headers=s.headers,
        query_params=s.query_params,
        json_body=s.json_body,
        auth=worker_auth,
        pagination=worker_pagination,
        record_path=s.record_path,
        timeout_seconds=s.timeout_seconds,
        max_retries=s.max_retries,
        sample_size=sample_size,
        flowfile_flow_id=node_rest_api_reader.flow_id,
        flowfile_node_id=node_rest_api_reader.node_id,
    )
