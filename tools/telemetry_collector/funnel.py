"""Activation-funnel report over a collector ``events.jsonl`` file.

Frozen funnel definitions:
    installs      = distinct install_id across all events
    launched      = distinct install_id with >=1 app_started
    run_attempted = distinct install_id with >=1 flow_run_started
    activated     = distinct install_id with >=1 activation
    week2_return  = distinct install_id with >=1 event whose ts is in
                    [first_ts + 7 days, first_ts + 14 days), where first_ts
                    is that install's earliest event ts

Usage: python -m tools.telemetry_collector.funnel <path-to-events.jsonl> [--days N]

``--days N`` restricts the computation to events within the last N days from
the maximum timestamp in the file. Malformed lines are skipped, counted, and
reported to stderr. Stdlib only.

Every timestamp comes from the server-stamped ``received_at``, falling back to
the client's ``ts`` only for lines written before the collector stamped one: a
single event claiming to be from the year 9999 would otherwise push the
``--days`` cutoff past every genuine event and report zeros.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path


@dataclass
class FunnelStats:
    installs: int
    launched: int
    run_attempted: int
    activated: int
    week2_return: int
    malformed: int


def _parse_ts(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _parse_line(line: str) -> tuple[str, str, datetime] | None:
    try:
        raw = json.loads(line)
    except ValueError:
        return None
    if not isinstance(raw, dict):
        return None
    install_id = raw.get("install_id")
    event = raw.get("event")
    ts = _parse_ts(raw.get("received_at")) or _parse_ts(raw.get("ts"))
    if not isinstance(install_id, str) or not install_id or not isinstance(event, str) or not event or ts is None:
        return None
    return install_id, event, ts


def compute_funnel(path: Path, days: int | None = None) -> FunnelStats:
    """Parse the JSONL file and compute the frozen funnel numbers."""
    parsed: list[tuple[str, str, datetime]] = []
    malformed = 0
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = _parse_line(line)
            if row is None:
                malformed += 1
            else:
                parsed.append(row)

    if days is not None and parsed:
        cutoff = max(ts for _, _, ts in parsed) - timedelta(days=days)
        parsed = [row for row in parsed if row[2] >= cutoff]

    events_by_install: dict[str, list[tuple[str, datetime]]] = {}
    for install_id, event, ts in parsed:
        events_by_install.setdefault(install_id, []).append((event, ts))

    launched = run_attempted = activated = week2_return = 0
    for events in events_by_install.values():
        names = {event for event, _ in events}
        launched += "app_started" in names
        run_attempted += "flow_run_started" in names
        activated += "activation" in names
        first_ts = min(ts for _, ts in events)
        window_start = first_ts + timedelta(days=7)
        window_end = first_ts + timedelta(days=14)
        week2_return += any(window_start <= ts < window_end for _, ts in events)

    return FunnelStats(
        installs=len(events_by_install),
        launched=launched,
        run_attempted=run_attempted,
        activated=activated,
        week2_return=week2_return,
        malformed=malformed,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Print the Flowfile activation funnel from an events.jsonl file.")
    parser.add_argument("path", type=Path, help="path to events.jsonl")
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help="only count events within the last N days from the max ts in the file",
    )
    args = parser.parse_args(argv)

    stats = compute_funnel(args.path, days=args.days)
    if stats.malformed:
        print(f"skipped {stats.malformed} malformed line(s)", file=sys.stderr)

    def pct(count: int) -> str:
        return f"{count / stats.installs * 100:.1f}%" if stats.installs else "n/a"

    print(f"installs       {stats.installs}")
    print(f"launched       {stats.launched} ({pct(stats.launched)})")
    print(f"run_attempted  {stats.run_attempted} ({pct(stats.run_attempted)})")
    print(f"activated      {stats.activated} ({pct(stats.activated)})")
    print(f"week2_return   {stats.week2_return} ({pct(stats.week2_return)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
