"""Schema derivation for the Google Analytics 4 reader.

The GA4 Data API accepts ``dimensions`` and ``metrics`` and returns a table where
every dimension value is a string (the API emits them that way on the wire) and
every metric value is a number. For the flow's "predicted schema" we map each
selected dimension/metric to its natural Polars dtype using a small static
rule table — no network call is required, so schema prediction stays lazy.

The mapping is intentionally heuristic: GA4 does not publish a canonical list
of metric/dimension types, and users can add custom dimensions with arbitrary
names. The rules below cover the standard catalogue and safely fall back to
``String`` / ``Float64`` for unknown names.
"""

from __future__ import annotations

import polars as pl

from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn

# --- Dimensions ---------------------------------------------------------------

# Exact-name overrides for dimensions whose natural type is not String.
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
    """Natural Polars dtype for a GA4 dimension. Defaults to String."""
    return _DIMENSION_TYPE_OVERRIDES.get(name, pl.String)


# --- Metrics ------------------------------------------------------------------

# Suffix-based heuristics for metric dtypes. GA4 naming conventions put counts
# in names like ``*Users`` / ``*Count`` / ``sessions`` (integers) and ratios,
# durations, and monetary values in names with suffixes like ``*Rate``, ``*Duration``,
# ``*Value``, ``*Revenue``, ``*PerUser`` (floats).
_INT_METRIC_EXACT: set[str] = {
    "sessions",
    "screenPageViews",
    "eventCount",
    "eventCountPerUser",  # overridden to float below via suffix rule
    "bounces",
    "conversions",
    "itemPurchaseQuantity",
    "itemRefundQuantity",
    "transactions",
    "activeUsers",
    "newUsers",
    "totalUsers",
    "dauPerMau",  # override below
}

_INT_METRIC_SUFFIXES: tuple[str, ...] = (
    "Users",
    "Count",
    "Sessions",
    "Views",
    "Clicks",
    "Impressions",
    "Quantity",
)

_FLOAT_METRIC_SUFFIXES: tuple[str, ...] = (
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


def _metric_dtype(name: str) -> pl.DataType:
    """Natural Polars dtype for a GA4 metric. Falls back to Float64."""
    # Float suffixes win over int suffixes (e.g. ``eventCountPerUser`` is a float).
    if any(name.endswith(sfx) for sfx in _FLOAT_METRIC_SUFFIXES):
        return pl.Float64
    if name in _INT_METRIC_EXACT:
        return pl.Int64
    if any(name.endswith(sfx) for sfx in _INT_METRIC_SUFFIXES):
        return pl.Int64
    return pl.Float64


# --- Public API ---------------------------------------------------------------


def derive_schema(metrics: list[str], dimensions: list[str]) -> list[FlowfileColumn]:
    """Return the predicted output schema of a GA4 query.

    Dimensions appear first (matching the order GA4 returns them in
    ``run_report``), then metrics. Column names mirror the GA4 field names.

    This function performs no I/O — it powers the lazy ``schema_callback`` used
    by downstream nodes for field introspection without triggering a fetch.
    """
    columns: list[FlowfileColumn] = []
    for name in dimensions:
        columns.append(FlowfileColumn.create_from_polars_dtype(column_name=name, data_type=_dimension_dtype(name)))
    for name in metrics:
        columns.append(FlowfileColumn.create_from_polars_dtype(column_name=name, data_type=_metric_dtype(name)))
    return columns


def polars_schema_dict(metrics: list[str], dimensions: list[str]) -> dict[str, pl.DataType]:
    """Return the schema as a plain ``{name: pl.DataType}`` dict.

    Useful on the worker side when coercing the raw response into a
    typed ``pl.DataFrame``.
    """
    schema: dict[str, pl.DataType] = {}
    for name in dimensions:
        schema[name] = _dimension_dtype(name)
    for name in metrics:
        schema[name] = _metric_dtype(name)
    return schema
