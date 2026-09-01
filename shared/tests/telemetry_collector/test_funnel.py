import json
from pathlib import Path

from tools.telemetry_collector.funnel import compute_funnel

GOLDEN = Path(__file__).parent / "golden" / "events.jsonl"
RECEIVED = "2026-08-01T09:00:01Z"


def _line(install_id: str, event: str, ts: str, received_at: str | None = None) -> str:
    raw = {
        "event": event,
        "install_id": install_id,
        "app_version": "0.16.0",
        "platform": "darwin",
        "mode": "electron",
        "ts": ts,
        "props": {},
    }
    if received_at is not None:
        raw["received_at"] = received_at
    return json.dumps(raw, separators=(",", ":"))


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


def test_one_spoofed_timestamp_no_longer_zeroes_the_report(tmp_path):
    """The window comes from the server's received_at, so a client cannot move it."""
    path = tmp_path / "events.jsonl"
    lines = [
        _line(f"3f6b1c2e-8a94-4c50-9d0e-2f7a61b8c4d{index}", "app_started", "2026-08-01T09:00:00Z", RECEIVED)
        for index in range(5)
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    assert compute_funnel(path, days=30).launched == 5

    spoofed = _line("9d4c3b2a-1111-4222-8333-444455556666", "app_started", "9999-01-01T00:00:00Z", RECEIVED)
    lines.append(spoofed)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    stats = compute_funnel(path, days=30)
    assert stats.installs == 6
    assert stats.launched == 6, "the year-9999 ts must not push the cutoff past every genuine event"


def test_lines_without_received_at_still_parse(tmp_path):
    """Pre-collector-stamp lines (and the golden file's older shapes) fall back to ts."""
    path = tmp_path / "events.jsonl"
    path.write_text(
        "\n".join(
            [
                _line("3f6b1c2e-8a94-4c50-9d0e-2f7a61b8c4d1", "app_started", "2026-08-01T09:00:00Z"),
                _line("7c2d9e4f-8a94-4c50-9d0e-2f7a61b8c4d1", "app_started", "2026-08-02T09:00:00Z", RECEIVED),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    stats = compute_funnel(path)
    assert stats.installs == 2
    assert stats.launched == 2
    assert stats.malformed == 0
