"""Bucket boundaries.

Buckets are the only numeric thing telemetry ever transmits, so each edge is
pinned on both sides: an off-by-one here silently changes what every historical
event means.
"""

from __future__ import annotations

import pytest

from shared.telemetry import (
    DURATION_BUCKETS,
    NODE_COUNT_BUCKETS,
    ROW_BUCKETS,
    bucket_duration_seconds,
    bucket_node_count,
    bucket_rows,
)


@pytest.mark.parametrize(
    ("count", "expected"),
    [
        (-5, "1-3"),
        (0, "1-3"),
        (1, "1-3"),
        (3, "1-3"),
        (4, "4-7"),
        (7, "4-7"),
        (8, "8-15"),
        (15, "8-15"),
        (16, "16-30"),
        (30, "16-30"),
        (31, "31+"),
        (10_000, "31+"),
    ],
)
def test_bucket_node_count(count, expected):
    assert bucket_node_count(count) == expected


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [
        (0, "<1s"),
        (0.9, "<1s"),
        (0.999, "<1s"),
        (1, "1-10s"),
        (9.999, "1-10s"),
        (10, "10-60s"),
        (59.999, "10-60s"),
        (60, "1-5m"),
        (299.999, "1-5m"),
        (300, "5-30m"),
        (1799.999, "5-30m"),
        (1800, "30m+"),
        (86_400, "30m+"),
    ],
)
def test_bucket_duration_seconds(seconds, expected):
    assert bucket_duration_seconds(seconds) == expected


@pytest.mark.parametrize(
    ("count", "expected"),
    [
        (-1, "0"),
        (0, "0"),
        (1, "1-100"),
        (100, "1-100"),
        (101, "101-10k"),
        (10_000, "101-10k"),
        (10_001, "10k-1M"),
        (1_000_000, "10k-1M"),
        (1_000_001, "1M+"),
    ],
)
def test_bucket_rows(count, expected):
    assert bucket_rows(count) == expected


def test_every_bucket_value_is_declared():
    """The frozen value sets are the client-side validation allowlist; nothing may fall outside."""
    node_values = {bucket_node_count(n) for n in range(-2, 60)}
    assert node_values == set(NODE_COUNT_BUCKETS)

    duration_values = {bucket_duration_seconds(s / 2) for s in range(0, 4000)}
    assert duration_values == set(DURATION_BUCKETS)

    row_values = {bucket_rows(n) for n in (-1, 0, 1, 100, 101, 10_000, 10_001, 1_000_000, 1_000_001)}
    assert row_values == set(ROW_BUCKETS)
