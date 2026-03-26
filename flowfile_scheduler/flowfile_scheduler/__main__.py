"""Standalone entry point for the flow scheduler.

Usage:
    poetry run flowfile_scheduler            # continuous polling
    poetry run flowfile_scheduler --once     # single tick, then exit
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal

from flowfile_scheduler.engine import FlowScheduler


def main() -> None:
    parser = argparse.ArgumentParser(description="Flowfile schedule runner")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single scheduler tick and exit",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    scheduler = FlowScheduler()

    if args.once:
        asyncio.run(scheduler.run_once())
    else:

        async def _run() -> None:
            await scheduler.start()
            stop_event = asyncio.Event()

            loop = asyncio.get_event_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, stop_event.set)

            await stop_event.wait()
            await scheduler.stop()

        asyncio.run(_run())


if __name__ == "__main__":
    main()
