"""Minimal SQLAlchemy models for the scheduler.

These mirror the tables defined in ``flowfile_core.database.models`` but
are declared independently so that the scheduler package has **no imports
from core**.  Only columns needed for scheduling logic are mapped.
"""

from sqlalchemy import Boolean, Column, DateTime, Float, Integer, String, Text
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class FlowSchedule(Base):
    __tablename__ = "flow_schedules"

    id = Column(Integer, primary_key=True)
    registration_id = Column(Integer, nullable=False)
    owner_id = Column(Integer, nullable=False)
    enabled = Column(Boolean, default=True, nullable=False)
    schedule_type = Column(String, nullable=False)
    interval_seconds = Column(Integer, nullable=True)
    trigger_table_id = Column(Integer, nullable=True)
    last_triggered_at = Column(DateTime, nullable=True)
    last_trigger_table_updated_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)


class FlowRegistration(Base):
    __tablename__ = "flow_registrations"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    flow_path = Column(String, nullable=False)
    owner_id = Column(Integer, nullable=False)


class FlowRun(Base):
    __tablename__ = "flow_runs"

    id = Column(Integer, primary_key=True)
    registration_id = Column(Integer, nullable=True)
    flow_name = Column(String, nullable=False)
    flow_path = Column(String, nullable=True)
    user_id = Column(Integer, nullable=False)
    started_at = Column(DateTime, nullable=False)
    ended_at = Column(DateTime, nullable=True)
    success = Column(Boolean, nullable=True)
    nodes_completed = Column(Integer, default=0)
    number_of_nodes = Column(Integer, default=0)
    duration_seconds = Column(Float, nullable=True)
    run_type = Column(String, nullable=False, default="full_run")
    flow_snapshot = Column(Text, nullable=True)
    node_results_json = Column(Text, nullable=True)


class CatalogTable(Base):
    __tablename__ = "catalog_tables"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    updated_at = Column(DateTime, nullable=True)


class ScheduleTriggerTable(Base):
    __tablename__ = "schedule_trigger_tables"

    id = Column(Integer, primary_key=True)
    schedule_id = Column(Integer, nullable=False)
    table_id = Column(Integer, nullable=False)
    created_at = Column(DateTime, nullable=False)


class SchedulerLock(Base):
    __tablename__ = "scheduler_lock"

    id = Column(Integer, primary_key=True, default=1)
    holder_id = Column(String, nullable=False)
    started_at = Column(DateTime, nullable=False)
    heartbeat_at = Column(DateTime, nullable=False)
