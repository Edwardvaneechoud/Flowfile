"""Lightweight run-completion helper for CLI subprocesses.

This module updates a ``FlowRun`` record directly via SQLAlchemy without
importing anything from ``flowfile_core``, keeping the CLI completion
path fast and free of heavy dependencies (FastAPI, Pydantic, etc.).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from shared.models import FlowRun
from shared.storage_config import get_database_url

logger = logging.getLogger("flowfile.run_completion")


def complete_run(
    run_id: int,
    success: bool,
    nodes_completed: int,
    number_of_nodes: int = 0,
) -> None:
    """Mark a pre-created ``FlowRun`` record as completed.

    Creates a one-shot SQLAlchemy session against the shared database,
    updates the run record, and tears down immediately.
    """
    url = get_database_url()
    connect_args = {"check_same_thread": False} if "sqlite" in url else {}
    engine = create_engine(url, connect_args=connect_args)

    with Session(engine) as session:
        run = session.get(FlowRun, run_id)
        if run is None:
            logger.warning("Run %s not found — skipping completion", run_id)
            return

        now = datetime.now(timezone.utc)
        run.ended_at = now
        run.success = success
        run.nodes_completed = nodes_completed
        if number_of_nodes > 0:
            run.number_of_nodes = number_of_nodes
        if run.started_at:
            started_utc = run.started_at.replace(tzinfo=None)
            now_utc = now.replace(tzinfo=None)
            run.duration_seconds = (now_utc - started_utc).total_seconds()

        session.commit()
        logger.info("Run %s completed: success=%s", run_id, success)
