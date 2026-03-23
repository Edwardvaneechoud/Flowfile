"""Flow scheduling engine.

The embedded scheduler re-exports ``FlowScheduler`` from the lightweight
``flowfile_scheduler`` package so both embedded and standalone modes use
the same engine code.
"""

from flowfile_scheduler.flowfile_scheduler.engine import FlowScheduler

__all__ = ["FlowScheduler"]
