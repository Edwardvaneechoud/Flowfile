"""``python -m tools.chart_ai_eval``: run the chart cases against a live core and write the reports."""

from __future__ import annotations

import argparse
import signal
import sys
import tempfile
import threading
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

from tools.chart_ai_eval import runner
from tools.chart_ai_eval.report import build_report, summary_lines, write_reports
from tools.chart_ai_eval.scoring import StepResult
from tools.chart_ai_eval.tables import TABLE_KEYS


def _parse(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="python -m tools.chart_ai_eval", description=__doc__)
    parser.add_argument("--api", required=True, help="the core's base URL, e.g. http://localhost:63578")
    parser.add_argument("--provider", help="AI provider id sent with every call (the route's default when absent)")
    parser.add_argument("--model", help="model sent with every call")
    parser.add_argument("--out", required=True, type=Path, help="folder for report.md and report.json")
    parser.add_argument("--token", help="a bearer token; by default one is taken from POST /auth/token")
    parser.add_argument("--tables", help=f"comma-separated subset of {','.join(TABLE_KEYS)}")
    parser.add_argument("--limit", type=int, help="run only the first N cases")
    parser.add_argument("--keep", action="store_true", help="leave the seeded tables in the catalog")
    parser.add_argument("--timeout", type=float, default=150.0, help="seconds to wait for one HTTP call")
    parser.add_argument(
        "--seed-dir",
        type=Path,
        help="existing folder for the seed files (default: the system temp folder). Core reads them by path, so "
        "it must see the folder at this path, which a core in a container or on another host does not; a core "
        "outside electron mode also takes only a folder under its working, storage or user data directory",
    )
    args = parser.parse_args(argv)
    if args.tables is not None:
        args.tables = [t.strip() for t in args.tables.split(",") if t.strip()]
        if not args.tables:
            parser.error("--tables names no table")
        unknown = sorted(set(args.tables) - set(TABLE_KEYS))
        if unknown:
            parser.error(f"unknown table(s): {', '.join(unknown)}")
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be at least 1")
    if args.seed_dir is not None:
        if not args.seed_dir.is_dir():
            parser.error(f"--seed-dir {args.seed_dir} is not an existing folder")
        args.seed_dir = args.seed_dir.resolve()
    return args


def _raise_interrupt(_signum: int, _frame: object) -> None:
    raise KeyboardInterrupt


@contextmanager
def _sigterm_as_interrupt() -> Iterator[None]:
    """A SIGTERM inside the block is a ``KeyboardInterrupt``, so the tables are removed; the old handler returns."""
    if threading.current_thread() is not threading.main_thread():
        yield
        return
    previous = signal.signal(signal.SIGTERM, _raise_interrupt)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, signal.SIG_DFL if previous is None else previous)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _unwritable(out: Path) -> str | None:
    """Why no report can be written into ``out`` (made when missing); ``None`` when one can."""
    try:
        out.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryFile(dir=out):
            pass
    except OSError as exc:
        return f"cannot write the reports into {out}: {exc.strerror or type(exc).__name__}"
    return None


def _report(out: Path, results: Sequence[StepResult], meta: dict[str, Any]) -> None:
    """Print the summary, then write the files, so a write that fails still leaves the numbers on screen."""
    report = build_report(results, meta)
    print("\n" + "\n".join(summary_lines(report["summary"])))
    write_reports(out, report, results)
    print(f"\nwrote {out / 'report.md'} and {out / 'report.json'}")


def _run(args: argparse.Namespace) -> int:
    cases = runner.select_cases(runner.load_cases(), args.tables, args.limit)
    if not cases:
        print("no case selected", file=sys.stderr)
        return 2
    problem = _unwritable(args.out)
    if problem:
        print(problem, file=sys.stderr)
        return 2
    keys = [k for k in TABLE_KEYS if any(c["table"] == k for c in cases)]
    total = sum(len(c["steps"]) for c in cases)
    results: list[StepResult] = []
    left: list[str] = []
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
            with runner.seeded_tables(
                core, keys, keep=args.keep, log=print, seed_dir=args.seed_dir, left=left
            ) as seeded:
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
        print("interrupted", file=sys.stderr)
    except (runner.CoreError, httpx.HTTPError) as exc:
        status = 2
        print(f"stopped: {exc if isinstance(exc, runner.CoreError) else type(exc).__name__}", file=sys.stderr)
    except Exception:
        meta["interrupted"] = True
        raise
    finally:
        if left:
            print("may still be in the catalog: " + ", ".join(left), file=sys.stderr)
        meta["finished"] = _now()
        if results:
            _report(args.out, results, meta)
    return status or (1 if left else 0)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse(argv)
    with _sigterm_as_interrupt():
        return _run(args)


if __name__ == "__main__":
    sys.exit(main())
