"""``python -m tools.chart_ai_eval``: run the chart cases against a live core and write the reports."""

from __future__ import annotations

import argparse
import signal
import sys
from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path

import httpx

from tools.chart_ai_eval import runner
from tools.chart_ai_eval.report import summary_lines, write_reports
from tools.chart_ai_eval.scoring import StepResult
from tools.chart_ai_eval.tables import TABLE_KEYS


def _parse(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="python -m tools.chart_ai_eval", description=__doc__)
    parser.add_argument("--api", required=True, help="the core's base URL, e.g. http://localhost:63598")
    parser.add_argument("--provider", help="AI provider id sent with every call (the route's default when absent)")
    parser.add_argument("--model", help="model sent with every call")
    parser.add_argument("--out", required=True, type=Path, help="folder for report.md and report.json")
    parser.add_argument("--token", help="a bearer token; by default one is taken from POST /auth/token")
    parser.add_argument("--tables", help=f"comma-separated subset of {','.join(TABLE_KEYS)}")
    parser.add_argument("--limit", type=int, help="run only the first N cases")
    parser.add_argument("--keep", action="store_true", help="leave the seeded tables in the catalog")
    parser.add_argument("--timeout", type=float, default=150.0, help="seconds to wait for one HTTP call")
    args = parser.parse_args(argv)
    if args.tables:
        args.tables = [t.strip() for t in args.tables.split(",") if t.strip()]
        unknown = sorted(set(args.tables) - set(TABLE_KEYS))
        if unknown:
            parser.error(f"unknown table(s): {', '.join(unknown)}")
    return args


def _raise_interrupt(_signum: int, _frame: object) -> None:
    raise KeyboardInterrupt


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse(argv)
    signal.signal(signal.SIGTERM, _raise_interrupt)
    cases = runner.select_cases(runner.load_cases(), args.tables, args.limit)
    keys = [k for k in TABLE_KEYS if any(c["table"] == k for c in cases)]
    total = sum(len(c["steps"]) for c in cases)
    results: list[StepResult] = []
    meta = {"api": args.api, "provider": args.provider, "model": args.model, "started": _now(), "interrupted": False}

    def show(result: StepResult) -> None:
        a = result.answer
        failed = [c for c in result.score.checks if not c.held]
        detail = f"{a.outcome}" + (f" ({a.reason})" if a.reason else "")
        attempts = "?" if a.attempts is None else a.attempts
        print(
            f"[{len(results)}/{total}] {result.case_id} step {result.step}: {detail}, attempts {attempts}, "
            f"{(a.latency_ms or 0) / 1000:.1f} s, "
            f"{len(result.score.checks) - len(failed)}/{len(result.score.checks)} expectations",
            flush=True,
        )

    status = 0
    try:
        with httpx.Client(base_url=args.api.rstrip("/"), timeout=args.timeout) as client:
            core = runner.Core.login(client, args.token)
            with runner.seeded_tables(core, keys, keep=args.keep, log=print) as seeded:
                for case in cases:
                    runner.run_case(
                        core,
                        case,
                        seeded[case["table"]].id,
                        provider=args.provider,
                        model=args.model,
                        on_step=lambda r: (results.append(r), show(r)),
                    )
    except KeyboardInterrupt:
        meta["interrupted"] = True
        status = 130
        print("interrupted; the seeded tables were removed unless --keep was given", file=sys.stderr)
    except (runner.CoreError, httpx.HTTPError) as exc:
        status = 2
        print(f"stopped: {exc if isinstance(exc, runner.CoreError) else type(exc).__name__}", file=sys.stderr)
    meta["finished"] = _now()
    if results:
        report = write_reports(args.out, results, meta)
        print("\n" + "\n".join(summary_lines(report["summary"])))
        print(f"\nwrote {args.out / 'report.md'} and {args.out / 'report.json'}")
    return status


if __name__ == "__main__":
    sys.exit(main())
