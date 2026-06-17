"""Flow scheduling engine.

The embedded scheduler re-exports ``FlowScheduler`` from the lightweight
``flowfile_scheduler`` package so both embedded and standalone modes use
the same engine code.

The module-level ``_scheduler`` singleton (with ``get_scheduler`` /
``set_scheduler`` accessors) lives here so that both ``main.py`` and route
modules can import it without circular dependencies — the same pattern used
by ``flowfile_core.kernel`` for its ``_manager`` singleton.
"""

from flowfile_scheduler.engine import FlowScheduler

__all__ = ["FlowScheduler", "get_scheduler", "set_scheduler"]

_scheduler: FlowScheduler | None = None


def get_scheduler() -> FlowScheduler | None:
    return _scheduler


def set_scheduler(scheduler: FlowScheduler | None) -> None:
    global _scheduler
    _scheduler = scheduler
