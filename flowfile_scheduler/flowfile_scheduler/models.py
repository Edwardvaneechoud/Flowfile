"""Minimal SQLAlchemy models for the scheduler.

These are now re-exported from ``shared.models`` — the single source of
truth for the lightweight model layer.  This module exists purely for
backward compatibility so that existing ``from flowfile_scheduler.models
import …`` statements continue to work.
"""

from shared.models import (
    Base,
    CatalogTable,
    FlowRegistration,
    FlowRun,
    FlowSchedule,
    SchedulerLock,
    ScheduleTriggerTable,
)

__all__ = [
    "Base",
    "CatalogTable",
    "FlowRegistration",
    "FlowRun",
    "FlowSchedule",
    "SchedulerLock",
    "ScheduleTriggerTable",
]
