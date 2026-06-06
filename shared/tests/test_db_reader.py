import logging
import time

import polars as pl
import pytest

from shared import db_reader
from shared.db_reader import DatabaseReadCancelledError, read_sql_with_fallback

logger = logging.getLogger("test-db-reader")

CX_DF = pl.DataFrame({"engine": ["connectorx"]})
SA_DF = pl.DataFrame({"engine": ["sqlalchemy"]})


@pytest.fixture(autouse=True)
def clear_state():
    with db_reader._sqlalchemy_first_lock:
        db_reader._sqlalchemy_first.clear()
    yield
    with db_reader._sqlalchemy_first_lock:
        db_reader._sqlalchemy_first.clear()


@pytest.fixture
def calls(monkeypatch):
    counts = {"cx": 0, "sa": 0}

    def fake_cx(query, uri):
        counts["cx"] += 1
        return CX_DF

    def fake_sa(query, uri):
        counts["sa"] += 1
        return SA_DF

    monkeypatch.setattr(db_reader, "_read_connectorx", fake_cx)
    monkeypatch.setattr(db_reader, "_read_sqlalchemy", fake_sa)
    return counts


def test_connectorx_fast_path(calls):
    df = read_sql_with_fallback("SELECT 1", "postgresql://u@h/d", logger, hedge_delay=5)
    assert df.equals(CX_DF)
    assert calls == {"cx": 1, "sa": 0}


def test_hedge_on_hang_returns_sqlalchemy(calls, monkeypatch):
    def hanging_cx(query, uri):
        calls["cx"] += 1
        time.sleep(20)
        return CX_DF

    monkeypatch.setattr(db_reader, "_read_connectorx", hanging_cx)
    start = time.monotonic()
    df = read_sql_with_fallback("SELECT 1", "postgresql://u@h/hang", logger, hedge_delay=0.2)
    assert df.equals(SA_DF)
    assert time.monotonic() - start < 5
    assert calls["sa"] == 1


def test_sqlalchemy_win_marks_uri_connectorx_skipped(calls, monkeypatch):
    def hanging_cx(query, uri):
        calls["cx"] += 1
        time.sleep(20)
        return CX_DF

    monkeypatch.setattr(db_reader, "_read_connectorx", hanging_cx)
    uri = "postgresql://u@h/marked"
    read_sql_with_fallback("SELECT 1", uri, logger, hedge_delay=0.2)
    assert calls["cx"] == 1
    df = read_sql_with_fallback("SELECT 1", uri, logger, hedge_delay=0.2)
    assert df.equals(SA_DF)
    assert calls["cx"] == 1
    assert calls["sa"] == 2


def test_connectorx_error_falls_back_before_hedge_delay(calls, monkeypatch):
    def failing_cx(query, uri):
        calls["cx"] += 1
        raise RuntimeError("protocol error")

    monkeypatch.setattr(db_reader, "_read_connectorx", failing_cx)
    start = time.monotonic()
    df = read_sql_with_fallback("SELECT 1", "postgresql://u@h/err", logger, hedge_delay=30)
    assert df.equals(SA_DF)
    assert time.monotonic() - start < 5


def test_both_fail_raises(calls, monkeypatch):
    monkeypatch.setattr(db_reader, "_read_connectorx", lambda q, u: (_ for _ in ()).throw(RuntimeError("cx boom")))
    monkeypatch.setattr(db_reader, "_read_sqlalchemy", lambda q, u: (_ for _ in ()).throw(RuntimeError("sa boom")))
    with pytest.raises(RuntimeError, match="both connectorx and SQLAlchemy"):
        read_sql_with_fallback("SELECT 1", "postgresql://u@h/boom", logger, hedge_delay=1)


def test_sqlalchemy_error_waits_for_connectorx(calls, monkeypatch):
    def slow_cx(query, uri):
        calls["cx"] += 1
        time.sleep(1.0)
        return CX_DF

    monkeypatch.setattr(db_reader, "_read_connectorx", slow_cx)
    monkeypatch.setattr(db_reader, "_read_sqlalchemy", lambda q, u: (_ for _ in ()).throw(RuntimeError("sa boom")))
    df = read_sql_with_fallback("SELECT 1", "postgresql://u@h/slowcx", logger, hedge_delay=0.1)
    assert df.equals(CX_DF)


def test_cancel_check_aborts_quickly(calls, monkeypatch):
    def hanging_cx(query, uri):
        time.sleep(20)
        return CX_DF

    def hanging_sa(query, uri):
        time.sleep(20)
        return SA_DF

    monkeypatch.setattr(db_reader, "_read_connectorx", hanging_cx)
    monkeypatch.setattr(db_reader, "_read_sqlalchemy", hanging_sa)
    cancelled_at = time.monotonic() + 0.5
    start = time.monotonic()
    with pytest.raises(DatabaseReadCancelledError):
        read_sql_with_fallback(
            "SELECT 1",
            "postgresql://u@h/cancel",
            logger,
            hedge_delay=0.1,
            cancel_check=lambda: time.monotonic() >= cancelled_at,
        )
    assert time.monotonic() - start < 5
