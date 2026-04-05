"""Worker-side API reader — fetches all pages and returns a Polars DataFrame."""

from __future__ import annotations

import polars as pl

from flowfile_core.schemas.api_schemas import ApiReadSettingsWorker


def read_api_source(api_read_settings: ApiReadSettingsWorker) -> pl.DataFrame:
    """Fetch all records from a REST API and return as a DataFrame.

    Called by the worker in a subprocess. Secrets in *api_read_settings* are
    encrypted ($ffsec$ format) and are decrypted by the client module when
    building HTTP requests.
    """
    from flowfile_core.flowfile.sources.external_sources.api_source.client import paginated_iter

    records = list(paginated_iter(api_read_settings))
    if not records:
        return pl.DataFrame()
    return pl.DataFrame(records)
