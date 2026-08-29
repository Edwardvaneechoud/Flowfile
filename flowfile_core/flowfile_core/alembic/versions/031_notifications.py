"""Add notification channels, rules, and the transactional outbox.

Alerting on flow-run outcomes. ``notification_channels`` holds webhook targets
(the URL is stored as a ``$ffsec$`` Fernet token, never plaintext),
``notification_rules`` scopes which runs alert to which channel, and
``notification_outbox`` is the transactional outbox: a row is enqueued in the
same transaction that claims a run for evaluation, then drained over HTTP by the
scheduler tick / the finishing CLI subprocess. The unique constraint on
(rule_id, run_id, event_type) is the idempotency key — the run-completion and
reaper paths race to enqueue by design.

``flow_runs.notification_processed_at`` marks a run as *evaluated*, not
*notified*. Existing rows are backfilled from ``ended_at`` so upgrading an
install with run history does not alert on every historical failure.

Revision ID: 031
Revises: 030
Create Date: 2026-08-24
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "031"
down_revision: str | None = "030"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _has_column(table: str, column: str) -> bool:
    return column in {c["name"] for c in inspect(op.get_bind()).get_columns(table)}


def _has_table(table: str) -> bool:
    return table in inspect(op.get_bind()).get_table_names()


def upgrade() -> None:
    if not _has_table("notification_channels"):
        op.create_table(
            "notification_channels",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("owner_id", sa.Integer(), sa.ForeignKey("users.id"), nullable=False),
            sa.Column("name", sa.String(), nullable=False),
            sa.Column("channel_type", sa.String(), nullable=False),
            sa.Column("webhook_url_encrypted", sa.Text(), nullable=False),
            sa.Column("enabled", sa.Boolean(), nullable=False, server_default="1"),
            sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        )

    if not _has_table("notification_rules"):
        op.create_table(
            "notification_rules",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("owner_id", sa.Integer(), sa.ForeignKey("users.id"), nullable=False),
            sa.Column("channel_id", sa.Integer(), sa.ForeignKey("notification_channels.id"), nullable=False),
            sa.Column("registration_id", sa.Integer(), sa.ForeignKey("flow_registrations.id"), nullable=True),
            sa.Column("schedule_id", sa.Integer(), sa.ForeignKey("flow_schedules.id"), nullable=True),
            sa.Column("on_failure", sa.Boolean(), nullable=False, server_default="1"),
            sa.Column("on_success", sa.Boolean(), nullable=False, server_default="0"),
            sa.Column("on_recovery", sa.Boolean(), nullable=False, server_default="1"),
            sa.Column("enabled", sa.Boolean(), nullable=False, server_default="1"),
            sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        )

    if not _has_table("notification_outbox"):
        op.create_table(
            "notification_outbox",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("rule_id", sa.Integer(), sa.ForeignKey("notification_rules.id"), nullable=False),
            sa.Column("channel_id", sa.Integer(), sa.ForeignKey("notification_channels.id"), nullable=False),
            sa.Column("run_id", sa.Integer(), sa.ForeignKey("flow_runs.id"), nullable=True),
            sa.Column("event_type", sa.String(), nullable=False),
            sa.Column("payload_json", sa.Text(), nullable=False),
            sa.Column("status", sa.String(), nullable=False, server_default="pending"),
            sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("next_attempt_at", sa.DateTime(), nullable=True),
            sa.Column("last_error", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
            sa.Column("sent_at", sa.DateTime(), nullable=True),
            sa.UniqueConstraint("rule_id", "run_id", "event_type", name="uq_outbox_rule_run_event"),
        )

    if not _has_column("flow_runs", "notification_processed_at"):
        op.add_column("flow_runs", sa.Column("notification_processed_at", sa.DateTime(), nullable=True))
        # Backfill only on the tick that adds the column: every already-finished run counts
        # as evaluated, otherwise the first drain alert-storms an install's whole history.
        op.execute("UPDATE flow_runs SET notification_processed_at = ended_at WHERE ended_at IS NOT NULL")


def downgrade() -> None:
    if _has_table("notification_outbox"):
        op.drop_table("notification_outbox")
    if _has_table("notification_rules"):
        op.drop_table("notification_rules")
    if _has_table("notification_channels"):
        op.drop_table("notification_channels")
    if _has_column("flow_runs", "notification_processed_at"):
        with op.batch_alter_table("flow_runs") as batch:
            batch.drop_column("notification_processed_at")
