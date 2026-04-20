"""Fetch GA4 reporting data and materialise it as a Polars DataFrame.

Invoked by the worker's ``start_generic_process`` background task. The returned
``pl.DataFrame`` is serialised to an Arrow IPC file and later streamed back to
the core as a ``pl.LazyFrame``.

The compute — including OAuth token refresh, HTTP round-trips, and row
pagination — happens entirely in this process so the core stays responsive.
"""

from __future__ import annotations

import re

import polars as pl

from flowfile_worker.configs import logger
from flowfile_worker.external_sources.google_analytics_source.models import GoogleAnalyticsReadSettings

# One GA4 ``run_report`` call returns at most 100 000 rows — use that as the
# pagination size. The API caps ``limit`` at 250 000 per request, but 100 000 is
# the documented maximum page and matches Google's own examples.
_PAGE_SIZE = 100_000


# Suffix/name heuristics duplicated from the core schema helper (kept local to
# avoid a cross-package import from worker -> core).
_INT_METRIC_EXACT = {
    "sessions",
    "screenPageViews",
    "eventCount",
    "bounces",
    "conversions",
    "itemPurchaseQuantity",
    "itemRefundQuantity",
    "transactions",
    "activeUsers",
    "newUsers",
    "totalUsers",
}
_INT_METRIC_SUFFIXES = ("Users", "Count", "Sessions", "Views", "Clicks", "Impressions", "Quantity")
_FLOAT_METRIC_SUFFIXES = (
    "Rate",
    "Duration",
    "Revenue",
    "Value",
    "PerUser",
    "PerSession",
    "Average",
    "Avg",
    "Ratio",
    "Score",
    "Amount",
    "Price",
)
_DIMENSION_TYPE_OVERRIDES: dict[str, pl.DataType] = {
    "date": pl.Date,
    "dateHour": pl.Datetime,
    "dateHourMinute": pl.Datetime,
    "year": pl.Int64,
    "month": pl.Int64,
    "week": pl.Int64,
    "day": pl.Int64,
    "hour": pl.Int64,
    "minute": pl.Int64,
    "yearMonth": pl.Int64,
    "yearWeek": pl.Int64,
    "nthDay": pl.Int64,
    "nthHour": pl.Int64,
    "nthMinute": pl.Int64,
    "nthMonth": pl.Int64,
    "nthWeek": pl.Int64,
    "nthYear": pl.Int64,
}


def _dimension_dtype(name: str) -> pl.DataType:
    return _DIMENSION_TYPE_OVERRIDES.get(name, pl.String)


def _metric_dtype(name: str) -> pl.DataType:
    if any(name.endswith(sfx) for sfx in _FLOAT_METRIC_SUFFIXES):
        return pl.Float64
    if name in _INT_METRIC_EXACT:
        return pl.Int64
    if any(name.endswith(sfx) for sfx in _INT_METRIC_SUFFIXES):
        return pl.Int64
    return pl.Float64


def _coerce(values: list[str], dtype: pl.DataType) -> pl.Series | list:
    """Cast the GA4 string payload to the column's natural dtype.

    Returns a Series/list that Polars can promote when building the DataFrame.
    """
    if dtype == pl.Int64:
        return [int(v) if v not in ("", None) else None for v in values]
    if dtype == pl.Float64:
        return [float(v) if v not in ("", None) else None for v in values]
    if dtype == pl.Date:
        # GA4 returns dates as "YYYYMMDD".
        def parse(v: str) -> str | None:
            if not v or len(v) != 8 or not v.isdigit():
                return None
            return f"{v[0:4]}-{v[4:6]}-{v[6:8]}"

        return pl.Series(values=[parse(v) for v in values], dtype=pl.String).str.strptime(
            pl.Date, format="%Y-%m-%d", strict=False
        )
    if dtype == pl.Datetime:
        # "YYYYMMDDHH" or "YYYYMMDDHHmm".
        def parse_dt(v: str) -> str | None:
            if not v:
                return None
            v = v.strip()
            if re.fullmatch(r"\d{10}", v):
                return f"{v[0:4]}-{v[4:6]}-{v[6:8]} {v[8:10]}:00:00"
            if re.fullmatch(r"\d{12}", v):
                return f"{v[0:4]}-{v[4:6]}-{v[6:8]} {v[8:10]}:{v[10:12]}:00"
            return None

        return pl.Series(values=[parse_dt(v) for v in values], dtype=pl.String).str.strptime(
            pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False
        )
    return values


def _build_empty_frame(metrics: list[str], dimensions: list[str]) -> pl.DataFrame:
    """Return an empty, typed frame — used when GA returns zero rows so the
    downstream schema still matches the prediction."""
    return pl.DataFrame(
        schema={
            **{name: _dimension_dtype(name) for name in dimensions},
            **{name: _metric_dtype(name) for name in metrics},
        }
    )


def read_google_analytics(ga_read_settings: GoogleAnalyticsReadSettings) -> pl.DataFrame:
    """Execute a GA4 ``run_report`` query and return the full result as a
    typed ``pl.DataFrame``. Called in a background task by the worker."""

    try:
        from google.analytics.data_v1beta import BetaAnalyticsDataClient
        from google.analytics.data_v1beta.types import (
            DateRange,
            Dimension,
            Metric,
            RunReportRequest,
        )
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
    except ImportError as e:
        raise RuntimeError(
            "google-analytics-data is not installed on the worker. "
            "Install the 'google_analytics' extra to use this connector."
        ) from e

    logger.info(
        "Starting GA4 read for property %s (%s .. %s)",
        ga_read_settings.property_id,
        ga_read_settings.start_date,
        ga_read_settings.end_date,
    )

    if not ga_read_settings.oauth_client_id or not ga_read_settings.oauth_client_secret_encrypted:
        raise RuntimeError(
            "OAuth client credentials missing from read settings. "
            "Configure them under Admin → Google OAuth in the Flowfile UI."
        )

    refresh_token = ga_read_settings.get_decrypted_refresh_token()
    client_secret = ga_read_settings.get_decrypted_client_secret()
    creds = Credentials(
        token=None,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=ga_read_settings.oauth_client_id,
        client_secret=client_secret,
        scopes=["https://www.googleapis.com/auth/analytics.readonly"],
    )
    creds.refresh(Request())
    client = BetaAnalyticsDataClient(credentials=creds)

    dim_objs = [Dimension(name=d) for d in ga_read_settings.dimensions]
    met_objs = [Metric(name=m) for m in ga_read_settings.metrics]
    date_range = DateRange(start_date=ga_read_settings.start_date, end_date=ga_read_settings.end_date)

    # Translate user-supplied filter specs into dimension_filter / metric_filter.
    # Done once up front so any bad operator raises immediately, before we
    # start paginating.
    from flowfile_worker.external_sources.google_analytics_source.filters import (
        build_filter_expressions,
    )

    dimension_filter, metric_filter = build_filter_expressions(
        ga_read_settings.filters,
        dimensions=ga_read_settings.dimensions,
        metrics=ga_read_settings.metrics,
    )

    rows_wanted = ga_read_settings.limit  # ``None`` means unlimited.
    offset = 0
    collected_rows: list[dict[str, str]] = []

    while True:
        page_limit = _PAGE_SIZE
        if rows_wanted is not None:
            page_limit = min(_PAGE_SIZE, rows_wanted - len(collected_rows))
            if page_limit <= 0:
                break

        request_kwargs: dict = {
            "property": f"properties/{ga_read_settings.property_id}",
            "dimensions": dim_objs,
            "metrics": met_objs,
            "date_ranges": [date_range],
            "limit": page_limit,
            "offset": offset,
        }
        if dimension_filter is not None:
            request_kwargs["dimension_filter"] = dimension_filter
        if metric_filter is not None:
            request_kwargs["metric_filter"] = metric_filter
        request = RunReportRequest(**request_kwargs)
        response = client.run_report(request)

        if not response.rows:
            break

        for row in response.rows:
            record: dict[str, str] = {}
            for i, dim in enumerate(response.dimension_headers):
                record[dim.name] = row.dimension_values[i].value
            for i, met in enumerate(response.metric_headers):
                record[met.name] = row.metric_values[i].value
            collected_rows.append(record)

        # If GA4 returned fewer rows than the page limit, we've exhausted the
        # report and can stop paginating.
        if len(response.rows) < page_limit:
            break
        offset += len(response.rows)

    logger.info("GA4 read finished — %d rows collected", len(collected_rows))

    if not collected_rows:
        return _build_empty_frame(ga_read_settings.metrics, ga_read_settings.dimensions)

    # Build the DataFrame column-by-column so we can enforce the predicted dtypes.
    columns: dict[str, object] = {}
    for name in ga_read_settings.dimensions:
        columns[name] = _coerce([r.get(name, "") for r in collected_rows], _dimension_dtype(name))
    for name in ga_read_settings.metrics:
        columns[name] = _coerce([r.get(name, "") for r in collected_rows], _metric_dtype(name))

    return pl.DataFrame(columns)
