"""Lightweight SQLAlchemy models shared across Flowfile packages.

These models mirror the tables defined in ``flowfile_core.database.models``
but are declared independently so that lightweight consumers (the scheduler,
CLI run-completion, etc.) can talk to the database **without importing
flowfile_core** and its heavy dependency tree (FastAPI, Pydantic, etc.).

Only columns required by non-core consumers are mapped here.
"""

import uuid
from typing import Literal

from sqlalchemy import Boolean, Column, DateTime, Float, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import declarative_base

Base = declarative_base()

RunType = Literal["in_designer_run", "scheduled", "manual", "on_demand"]


class FlowSchedule(Base):
    __tablename__ = "flow_schedules"

    id = Column(Integer, primary_key=True)
    registration_id = Column(Integer, nullable=False)
    owner_id = Column(Integer, nullable=False)
    enabled = Column(Boolean, default=True, nullable=False)
    name = Column(String, nullable=True)  # surfaced in notification payloads
    description = Column(String, nullable=True)
    schedule_type = Column(String, nullable=False)
    interval_seconds = Column(Integer, nullable=True)
    cron_expression = Column(String, nullable=True)  # 5-field cron string, used when schedule_type == "cron"
    cron_timezone = Column(String, nullable=True)  # IANA tz name the cron runs in
    trigger_table_id = Column(Integer, nullable=True)
    last_triggered_at = Column(DateTime, nullable=True)
    last_cron_slot = Column(DateTime, nullable=True)  # naive LOCAL wall-clock cron cursor (NOT UTC); DST-safe
    last_trigger_table_updated_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)


class FlowRegistration(Base):
    __tablename__ = "flow_registrations"

    id = Column(Integer, primary_key=True)
    flow_uuid = Column(String(36), nullable=False, unique=True, default=lambda: str(uuid.uuid4()))
    name = Column(String, nullable=False)
    flow_path = Column(String, nullable=False)
    owner_id = Column(Integer, nullable=False)


class FlowRun(Base):
    __tablename__ = "flow_runs"

    id = Column(Integer, primary_key=True)
    registration_id = Column(Integer, nullable=True)
    # Copied from FlowRegistration.flow_uuid at creation; core's run queries resolve on it.
    flow_uuid = Column(String(36), nullable=True, index=True)
    flow_name = Column(String, nullable=False)
    flow_path = Column(String, nullable=True)
    user_id = Column(Integer, nullable=False)
    started_at = Column(DateTime, nullable=False)
    ended_at = Column(DateTime, nullable=True)
    success = Column(Boolean, nullable=True)
    nodes_completed = Column(Integer, default=0)
    number_of_nodes = Column(Integer, default=0)
    duration_seconds = Column(Float, nullable=True)
    run_type: RunType = Column(String, nullable=False, default="in_designer_run")
    pid = Column(Integer, nullable=True)
    schedule_id = Column(Integer, nullable=True)
    flow_snapshot = Column(Text, nullable=True)
    node_results_json = Column(Text, nullable=True)
    # Stamped once the run has been evaluated for notifications (NULL = pending evaluation);
    # not a record that anything was sent.
    notification_processed_at = Column(DateTime, nullable=True)


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


class NotificationChannel(Base):
    """Webhook destination for run alerts; every channel type is a webhook, only the payload differs."""

    __tablename__ = "notification_channels"

    id = Column(Integer, primary_key=True)
    owner_id = Column(Integer, nullable=False)
    name = Column(String, nullable=False)
    channel_type = Column(String, nullable=False)  # "slack" | "discord" | "teams" | "generic"
    webhook_url_encrypted = Column(Text, nullable=False)  # $ffsec$ token — the URL is a credential
    enabled = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, nullable=True)


class NotificationRule(Base):
    """Which run outcomes reach which channel.

    Scope narrows in order: ``schedule_id`` set ⇒ that schedule's runs; else
    ``registration_id`` set ⇒ that flow's runs; else every run owned by ``owner_id``.
    """

    __tablename__ = "notification_rules"

    id = Column(Integer, primary_key=True)
    owner_id = Column(Integer, nullable=False)
    channel_id = Column(Integer, nullable=False)
    registration_id = Column(Integer, nullable=True)
    schedule_id = Column(Integer, nullable=True)
    on_failure = Column(Boolean, default=True, nullable=False)
    on_success = Column(Boolean, default=False, nullable=False)
    on_recovery = Column(Boolean, default=True, nullable=False)
    enabled = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, nullable=True)


class NotificationOutbox(Base):
    """Transactional outbox row: one pending webhook delivery.

    Enqueued in the transaction that claims a run for evaluation, so "run evaluated"
    and "alert queued" cannot diverge. ``channel_id`` is a snapshot; the channel row is
    re-read at send. ``next_attempt_at`` doubles as the sending lease expiry, so a
    drainer that dies mid-send releases the row.
    """

    __tablename__ = "notification_outbox"

    id = Column(Integer, primary_key=True)
    rule_id = Column(Integer, nullable=False)
    channel_id = Column(Integer, nullable=False)
    run_id = Column(Integer, nullable=True)
    event_type = Column(String, nullable=False)  # run_failed | run_success | run_recovered | run_orphaned
    payload_json = Column(Text, nullable=False)
    status = Column(String, nullable=False, default="pending")  # pending | sending | sent | dead
    attempts = Column(Integer, nullable=False, default=0)
    next_attempt_at = Column(DateTime, nullable=True)
    last_error = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=True)
    sent_at = Column(DateTime, nullable=True)

    __table_args__ = (UniqueConstraint("rule_id", "run_id", "event_type", name="uq_outbox_rule_run_event"),)
