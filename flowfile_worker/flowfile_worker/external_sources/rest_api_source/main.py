"""Fetch data from a REST API and materialise it as a Polars DataFrame.

Invoked by the worker's ``start_generic_process`` background task. The returned
``pl.DataFrame`` is serialised to an Arrow IPC file and later streamed back to
the core as a ``pl.LazyFrame``.

The actual HTTP work lives in :mod:`shared.rest_api.fetch` so the core can reuse
it for local execution; this wrapper only decrypts the worker-side credentials
and delegates.
"""

from __future__ import annotations

import polars as pl

from flowfile_worker.configs import logger
from flowfile_worker.external_sources.rest_api_source.models import RestApiReadSettings
from shared.rest_api.fetch import fetch_rest_api
from shared.rest_api.models import AuthType


def read_rest_api(settings: RestApiReadSettings) -> pl.DataFrame:
    """Decrypt the worker-side credential and run the shared fetch engine."""
    logger.info(
        "Starting REST API read: %s %s (pagination=%s, sample=%s)",
        settings.method.value,
        settings.url,
        settings.pagination.pagination_type.value,
        settings.sample_size is not None,
    )

    secret: str | None = None
    auth_type = settings.auth.auth_type
    if auth_type == AuthType.API_KEY:
        secret = settings.get_api_key()
    elif auth_type == AuthType.BEARER:
        secret = settings.get_bearer_token()
    elif auth_type == AuthType.BASIC:
        secret = settings.get_basic_password()

    df = fetch_rest_api(settings, secret=secret)
    logger.info("REST API read finished — %d records collected", df.height)
    return df
