from pathlib import Path

from tools.telemetry_collector.funnel import compute_funnel

GOLDEN = Path(__file__).parent / "golden" / "events.jsonl"


def test_golden_funnel_numbers():
    stats = compute_funnel(GOLDEN)
    assert stats.installs == 6
    assert stats.launched == 4
    assert stats.run_attempted == 4
    assert stats.activated == 2
    assert stats.week2_return == 2
    assert stats.malformed == 1


def test_days_filter_restricts_to_recent_events():
    stats = compute_funnel(GOLDEN, days=5)
    assert stats.installs == 2
    assert stats.launched == 2
    assert stats.run_attempted == 0
    assert stats.activated == 0
    assert stats.week2_return == 0
    assert stats.malformed == 1
