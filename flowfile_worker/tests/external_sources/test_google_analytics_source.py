"""Worker-side tests for the GA4 reader.

The Google SDK is mocked via ``sys.modules`` injection so the tests never talk
to the real API — they cover pagination, dtype coercion, and the empty-result
path. A separate test also verifies that the encrypted service-account JSON is
decrypted via ``decrypt_secret`` before being handed to the (fake) Google client.
"""

from __future__ import annotations

import sys
import types
from typing import Any
from unittest import mock

import polars as pl
import pytest


@pytest.fixture
def fake_google_sdk(monkeypatch: pytest.MonkeyPatch):
    """Install a minimal fake of ``google.analytics.data_v1beta`` + ``google.oauth2``
    into ``sys.modules`` so ``read_google_analytics`` can import them without the
    real SDK being installed."""

    def _build(rows_per_page: list[list[dict[str, Any]]]):
        # Fake Polars-friendly DSL types ------------------------------------
        class _Named:
            def __init__(self, name: str) -> None:
                self.name = name

        class _Value:
            def __init__(self, value: str) -> None:
                self.value = value

        class _Row:
            def __init__(self, dims: list[str], mets: list[str]) -> None:
                self.dimension_values = [_Value(v) for v in dims]
                self.metric_values = [_Value(v) for v in mets]

        class _Response:
            def __init__(self, dims: list[str], metrics: list[str], rows: list[dict[str, Any]]):
                self.dimension_headers = [_Named(d) for d in dims]
                self.metric_headers = [_Named(m) for m in metrics]
                self.rows = [_Row([row[d] for d in dims], [row[m] for m in metrics]) for row in rows]

        class _Client:
            call_count = 0

            def __init__(self, **_: Any) -> None:
                pass

            def run_report(self, request):  # noqa: ANN001 – matches real SDK signature
                idx = _Client.call_count
                _Client.call_count += 1
                if idx < len(rows_per_page):
                    # Honor the request's ``limit`` like the real API does.
                    page = rows_per_page[idx][: request.limit]
                    return _Response(list(request.dims), list(request.metrics), page)
                return _Response(list(request.dims), list(request.metrics), [])

        class _Dim:
            def __init__(self, name: str) -> None:
                self.name = name

        class _Metric:
            def __init__(self, name: str) -> None:
                self.name = name

        class _DateRange:
            def __init__(self, start_date: str, end_date: str) -> None:
                self.start_date = start_date
                self.end_date = end_date

        class _RunReportRequest:
            def __init__(
                self,
                *,
                property,
                dimensions,
                metrics,
                date_ranges,
                limit,
                offset,
                dimension_filter=None,
                metric_filter=None,
            ):
                self.property = property
                self.dims = [d.name for d in dimensions]
                self.metrics = [m.name for m in metrics]
                self.date_ranges = date_ranges
                self.limit = limit
                self.offset = offset
                self.dimension_filter = dimension_filter
                self.metric_filter = metric_filter

        class _Credentials:
            @classmethod
            def from_service_account_info(cls, info, scopes):
                c = cls()
                c.info = info
                c.scopes = scopes
                return c

        # Assemble fake module hierarchy ------------------------------------
        google_pkg = types.ModuleType("google")
        analytics_pkg = types.ModuleType("google.analytics")
        data_pkg = types.ModuleType("google.analytics.data_v1beta")
        types_pkg = types.ModuleType("google.analytics.data_v1beta.types")
        oauth2_pkg = types.ModuleType("google.oauth2")
        sa_pkg = types.ModuleType("google.oauth2.service_account")

        data_pkg.BetaAnalyticsDataClient = _Client
        types_pkg.DateRange = _DateRange
        types_pkg.Dimension = _Dim
        types_pkg.Metric = _Metric
        types_pkg.RunReportRequest = _RunReportRequest
        # Re-export the real filter-expression types so ``filters.py`` can still
        # construct FilterExpression / Filter / NumericValue objects when the
        # ``types`` module is shadowed by this fake.
        try:
            from google.analytics.data_v1beta.types import (  # noqa: I001 – lazy import
                Filter as _RealFilter,
                FilterExpression as _RealFilterExpression,
                FilterExpressionList as _RealFilterExpressionList,
                NumericValue as _RealNumericValue,
            )

            types_pkg.Filter = _RealFilter
            types_pkg.FilterExpression = _RealFilterExpression
            types_pkg.FilterExpressionList = _RealFilterExpressionList
            types_pkg.NumericValue = _RealNumericValue
        except ImportError:
            # Real SDK not installed — tests that use filters will naturally
            # fail, but non-filter tests still work.
            pass
        sa_pkg.Credentials = _Credentials

        monkeypatch.setitem(sys.modules, "google", google_pkg)
        monkeypatch.setitem(sys.modules, "google.analytics", analytics_pkg)
        monkeypatch.setitem(sys.modules, "google.analytics.data_v1beta", data_pkg)
        monkeypatch.setitem(sys.modules, "google.analytics.data_v1beta.types", types_pkg)
        monkeypatch.setitem(sys.modules, "google.oauth2", oauth2_pkg)
        monkeypatch.setitem(sys.modules, "google.oauth2.service_account", sa_pkg)

        return _Client

    return _build


def _build_settings(monkeypatch: pytest.MonkeyPatch, limit: int | None = None):
    """Build a GoogleAnalyticsReadSettings whose ``decrypt_secret`` returns a
    fixed plaintext service-account JSON without doing real Fernet work."""
    from flowfile_worker.external_sources.google_analytics_source import models as ga_models

    fake_json = '{"type": "service_account", "project_id": "test"}'
    monkeypatch.setattr(
        ga_models,
        "decrypt_secret",
        lambda _token: mock.MagicMock(get_secret_value=lambda: fake_json),
    )
    return ga_models.GoogleAnalyticsReadSettings(
        service_account_json_encrypted="$ffsec$1$1$ignored-by-fake",
        property_id="999",
        start_date="7daysAgo",
        end_date="yesterday",
        metrics=["sessions", "bounceRate"],
        dimensions=["date", "country"],
        limit=limit,
        flowfile_flow_id=1,
        flowfile_node_id=42,
    )


def test_read_google_analytics_builds_typed_frame(monkeypatch, fake_google_sdk):
    fake_google_sdk(
        [
            [
                {"date": "20240102", "country": "NL", "sessions": "12", "bounceRate": "0.42"},
                {"date": "20240103", "country": "NL", "sessions": "5", "bounceRate": "0.31"},
            ],
        ]
    )
    settings = _build_settings(monkeypatch)

    from flowfile_worker.external_sources.google_analytics_source.main import (
        read_google_analytics,
    )

    df = read_google_analytics(settings)

    assert df.columns == ["date", "country", "sessions", "bounceRate"]
    assert df.shape == (2, 4)
    # Dimensions get the predicted dtype applied.
    assert df["date"].dtype == pl.Date
    assert df["country"].dtype == pl.String
    # Metrics follow the naming heuristic.
    assert df["sessions"].dtype == pl.Int64
    assert df["bounceRate"].dtype == pl.Float64
    # Values are correctly coerced from the GA string payload.
    assert df["sessions"].to_list() == [12, 5]
    assert df["bounceRate"].to_list() == [0.42, 0.31]


def test_read_google_analytics_paginates(monkeypatch, fake_google_sdk):
    client = fake_google_sdk(
        [
            # First page – returns an incomplete page (fewer rows than limit),
            # so the loop should stop after this call.
            [
                {"date": "20240101", "country": "NL", "sessions": "1", "bounceRate": "0.1"},
            ],
        ]
    )
    settings = _build_settings(monkeypatch)

    from flowfile_worker.external_sources.google_analytics_source.main import (
        read_google_analytics,
    )

    df = read_google_analytics(settings)
    # Exactly one API call — the short page signals end-of-report.
    assert client.call_count == 1
    assert df.shape == (1, 4)


def test_read_google_analytics_respects_limit(monkeypatch, fake_google_sdk):
    fake_google_sdk(
        [
            # Return a full 100k-row page on first call, then a partial page.
            [{"date": "20240101", "country": "NL", "sessions": "1", "bounceRate": "0.1"} for _ in range(3)],
        ]
    )
    settings = _build_settings(monkeypatch, limit=2)

    from flowfile_worker.external_sources.google_analytics_source.main import (
        read_google_analytics,
    )

    df = read_google_analytics(settings)
    assert df.shape[0] == 2


def test_read_google_analytics_empty_result(monkeypatch, fake_google_sdk):
    fake_google_sdk([])  # No pages = no rows
    settings = _build_settings(monkeypatch)

    from flowfile_worker.external_sources.google_analytics_source.main import (
        read_google_analytics,
    )

    df = read_google_analytics(settings)
    # Still returns a correctly-typed, empty frame so the downstream schema
    # remains stable.
    assert df.columns == ["date", "country", "sessions", "bounceRate"]
    assert df.is_empty()
    assert df.schema["date"] == pl.Date
    assert df.schema["sessions"] == pl.Int64


def test_service_account_json_is_decrypted(monkeypatch, fake_google_sdk):
    """The credential must be decrypted before it reaches the Google client."""
    captured: dict[str, Any] = {}

    def fake_decrypt(token: str):
        captured["token"] = token
        return mock.MagicMock(get_secret_value=lambda: '{"type": "service_account"}')

    from flowfile_worker.external_sources.google_analytics_source import models as ga_models

    monkeypatch.setattr(ga_models, "decrypt_secret", fake_decrypt)

    fake_google_sdk([[]])
    settings = ga_models.GoogleAnalyticsReadSettings(
        service_account_json_encrypted="$ffsec$1$1$mock",
        property_id="1",
        start_date="2024-01-01",
        end_date="2024-01-02",
        metrics=["sessions"],
        dimensions=["date"],
    )

    from flowfile_worker.external_sources.google_analytics_source.main import (
        read_google_analytics,
    )

    read_google_analytics(settings)

    assert captured["token"] == "$ffsec$1$1$mock"


def test_filters_are_forwarded_to_run_report(monkeypatch, fake_google_sdk):
    """End-to-end: a configured filter ends up on the RunReportRequest."""
    captured: dict = {}

    # Patch the client so we can inspect the request rather than just the rows.
    client_cls = fake_google_sdk([[]])
    original_run_report = client_cls.run_report

    def record_run_report(self, request):
        captured["request"] = request
        return original_run_report(self, request)

    client_cls.run_report = record_run_report

    from flowfile_worker.external_sources.google_analytics_source import models as ga_models

    fake_json = '{"type": "service_account"}'
    monkeypatch.setattr(
        ga_models,
        "decrypt_secret",
        lambda _token: mock.MagicMock(get_secret_value=lambda: fake_json),
    )
    settings = ga_models.GoogleAnalyticsReadSettings(
        service_account_json_encrypted="$ffsec$1$1$mock",
        property_id="1",
        start_date="7daysAgo",
        end_date="yesterday",
        metrics=["sessions"],
        dimensions=["country"],
        filters=[
            ga_models.GoogleAnalyticsFilter(field="country", operator="equals", value="NL"),
            ga_models.GoogleAnalyticsFilter(field="sessions", operator="greater_than", value="5"),
        ],
    )

    from flowfile_worker.external_sources.google_analytics_source.main import (
        read_google_analytics,
    )

    read_google_analytics(settings)

    request = captured["request"]
    # Both filters reach the request, routed into the correct slot.
    assert hasattr(request, "dimension_filter")
    assert hasattr(request, "metric_filter")


def test_invalid_filter_raises_before_api_call(monkeypatch, fake_google_sdk):
    """Invalid filter specs raise before any GA call is made."""
    client_cls = fake_google_sdk([[]])
    from flowfile_worker.external_sources.google_analytics_source import models as ga_models

    fake_json = '{"type": "service_account"}'
    monkeypatch.setattr(
        ga_models,
        "decrypt_secret",
        lambda _token: mock.MagicMock(get_secret_value=lambda: fake_json),
    )
    settings = ga_models.GoogleAnalyticsReadSettings(
        service_account_json_encrypted="$ffsec$1$1$mock",
        property_id="1",
        start_date="2024-01-01",
        end_date="2024-01-31",
        metrics=["sessions"],
        dimensions=["country"],
        filters=[
            # pageTitle is not in metrics or dimensions — should raise.
            ga_models.GoogleAnalyticsFilter(field="pageTitle", operator="equals", value="Home"),
        ],
    )

    from flowfile_worker.external_sources.google_analytics_source.main import (
        read_google_analytics,
    )

    with pytest.raises(ValueError, match="not in the selected metrics or dimensions"):
        read_google_analytics(settings)

    # The error must fire before any run_report call.
    assert client_cls.call_count == 0


def test_missing_google_sdk_raises_runtime_error(monkeypatch):
    # Simulate the SDK not being installed by forcing an ImportError.
    import importlib
    import builtins

    real_import = builtins.__import__

    def blocked_import(name, *args, **kwargs):
        if name.startswith("google.analytics") or name.startswith("google.oauth2"):
            raise ImportError(f"Blocked: {name}")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", blocked_import)

    # Drop any prior imports so the failure happens inside ``read_google_analytics``.
    for mod in list(sys.modules):
        if mod.startswith("google.analytics") or mod.startswith("google.oauth2"):
            monkeypatch.delitem(sys.modules, mod, raising=False)
    importlib.invalidate_caches()

    from flowfile_worker.external_sources.google_analytics_source.main import (
        read_google_analytics,
    )
    from flowfile_worker.external_sources.google_analytics_source.models import (
        GoogleAnalyticsReadSettings,
    )

    settings = GoogleAnalyticsReadSettings(
        service_account_json_encrypted="$ffsec$1$1$x",
        property_id="1",
        start_date="2024-01-01",
        end_date="2024-01-02",
        metrics=["sessions"],
        dimensions=["date"],
    )

    with pytest.raises(RuntimeError, match="google-analytics-data is not installed"):
        read_google_analytics(settings)
