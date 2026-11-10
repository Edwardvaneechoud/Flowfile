"""Fetch data from a REST API and materialise it as a Polars DataFrame.

Invoked by the worker's ``start_generic_process`` background task. The returned
``pl.DataFrame`` is serialised to an Arrow IPC file and later streamed back to
the core as a ``pl.LazyFrame``.

The actual HTTP work lives in :mod:`shared.rest_api.fetch` so the core can reuse
it for local execution; this wrapper only decrypts the worker-side credentials
and delegates.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import polars as pl

from flowfile_worker.configs import logger as worker_logger
from flowfile_worker.external_sources.rest_api_source.models import RestApiReadSettings
from shared.rest_api.fetch import STOP_REASONS, describe_response, fetch_rest_api, suggest_paths
from shared.rest_api.models import AuthType

_TRACE_ENTRIES = 20
_RESPONSE_TEXT_CHARS = 4000
_EVERY_PAGE_UNTIL = 10
_THEN_EVERY = 10


def _page_line(entry: dict) -> str:
    line = (
        f"Page {entry['page']}: {entry['method']} {entry['url']} answered {entry['status']} "
        f"in {entry['elapsed_ms']} ms, {entry['records']} records"
    )
    return f"{line}, next cursor {entry['next_cursor']}" if entry["next_cursor"] is not None else line


def read_rest_api(
    settings: RestApiReadSettings, debug_file: str | None = None, logger: logging.Logger | None = None
) -> pl.DataFrame:
    """Decrypt the worker-side credential and run the shared fetch engine.

    Every request is logged to *logger* (the flow's log) with its secret masked: each of the first pages, then
    every tenth, and why the read stopped. *debug_file* gets what the drawer shows of a test request: the trace,
    the first response's shape and text, and the paths it suggests.
    """
    log = logger or worker_logger
    log.info(
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

    trace: list[dict] = []
    first: dict[str, Any] = {}

    def on_page(entry: dict, body: Any) -> None:
        page, stopped = entry["page"], entry["stopped"]
        if page == 1:
            text = json.dumps(body, indent=2, ensure_ascii=False, default=str)
            first.update(
                suggested_paths=suggest_paths(body),
                response_tree=describe_response(body),
                response_text=text[:_RESPONSE_TEXT_CHARS],
                response_truncated=len(text) > _RESPONSE_TEXT_CHARS,
            )
        if len(trace) < _TRACE_ENTRIES:
            trace.append(entry)
        if page <= _EVERY_PAGE_UNTIL or page % _THEN_EVERY == 0 or stopped:
            log.info(_page_line(entry))
        if stopped == "max_pages" and settings.sample_size is None and settings.pagination.max_pages > 1:
            log.warning("Stopped after %d pages: %s.", page, STOP_REASONS[stopped])
        elif stopped:
            log.info("Stopped after %d pages: %s.", page, STOP_REASONS[stopped])

    df = fetch_rest_api(settings, secret=secret, on_page=on_page)
    if debug_file:
        Path(debug_file).write_text(json.dumps({**first, "trace": trace}))
    log.info("REST API read finished — %d records collected", df.height)
    return df
