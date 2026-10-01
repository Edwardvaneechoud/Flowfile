"""The ``--run-flow`` verb: run a saved flow in-process for a pre-created run record.

Frozen builds spawn headless runs as ``[sys.executable, "--run-flow", ...]``; ``main.py`` dispatches here before
its router imports and ``storage.cleanup_directories()``, so a run child never sweeps the cache a live core uses.
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

from flowfile_core import telemetry as core_telemetry

_cli_logger = logging.getLogger("flowfile.run_flow_cli")


def run_flow_cli(flow_path: str, run_id: int) -> int:
    """Execute a flow in-process (used by PyInstaller builds via ``--run-flow``).

    Replicates the logic from ``flowfile/__main__.py:run_flow()`` without
    importing from the top-level ``flowfile`` package.
    """
    # Configure logging early so all messages are captured in the subprocess log file
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    _cli_logger.debug("run_flow_cli started: flow_path=%s, run_id=%s", flow_path, run_id)
    _cli_logger.debug("sys.executable=%s, frozen=%s", sys.executable, getattr(sys, "frozen", False))

    from flowfile_core.configs.settings import OFFLOAD_TO_WORKER

    OFFLOAD_TO_WORKER.set(False)

    from flowfile_core.flowfile.manage.io_flowfile import open_flow

    path = Path(flow_path)
    if not path.exists():
        _cli_logger.error("File not found: %s", flow_path)
        _complete_run(run_id, success=False, nodes_completed=0)
        return 1

    if path.suffix.lower() not in (".yaml", ".yml", ".json"):
        _cli_logger.error("Unsupported file format: %s", path.suffix)
        _complete_run(run_id, success=False, nodes_completed=0)
        return 1

    _cli_logger.debug("Loading flow from: %s", flow_path)
    try:
        from flowfile_core.auth.utils import get_local_user_id
        from shared.run_completion import get_run_user_id

        user_id = get_run_user_id(run_id) if run_id is not None else None
        if user_id is None:
            user_id = get_local_user_id()
        flow = open_flow(path, user_id=user_id)
    except Exception as e:
        _cli_logger.exception("Error loading flow: %s", e)
        _complete_run(run_id, success=False, nodes_completed=0)
        return 1

    flow.execution_location = "local"

    # Remove explore_data nodes — they're UI-only and require a worker service
    explore_data_nodes = [n.node_id for n in flow.nodes if n.node_type == "explore_data"]
    for node_id in explore_data_nodes:
        flow.delete_node(node_id)
    if explore_data_nodes:
        _cli_logger.debug("Skipped %d explore_data node(s) (UI-only)", len(explore_data_nodes))

    flow_name = flow.flow_settings.name or f"Flow {flow.flow_id}"
    _cli_logger.debug("Running flow: %s (id=%s), nodes: %d", flow_name, flow.flow_id, len(flow.nodes))

    # Stamp registration id before running so writes record producer lineage (mirrors canvas).
    from flowfile_core.flowfile.catalog_helpers import resolve_source_registration_id

    resolve_source_registration_id(flow)

    core_telemetry.install_headless()
    try:
        result = flow.run_graph()
    except Exception as e:
        _cli_logger.exception("Error running flow: %s", e)
        _complete_run(run_id, success=False, nodes_completed=0)
        return 1

    if result is None:
        _cli_logger.error("Flow execution returned no result")
        _complete_run(run_id, success=False, nodes_completed=0)
        return 1

    _cli_logger.debug(
        "Flow execution finished: success=%s, nodes_completed=%s/%s",
        result.success,
        result.nodes_completed,
        result.number_of_nodes,
    )

    node_results = None
    try:
        node_results = json.dumps([nr.model_dump(mode="json") for nr in (result.node_step_result or [])])
    except Exception as e:
        _cli_logger.warning("Node results serialization failed: %s", e)

    _complete_run(
        run_id,
        success=result.success,
        nodes_completed=result.nodes_completed,
        number_of_nodes=result.number_of_nodes,
        node_results_json=node_results,
    )

    if result.success:
        duration = ""
        if result.start_time and result.end_time:
            duration = f" in {(result.end_time - result.start_time).total_seconds():.2f}s"
        _cli_logger.debug("Flow completed successfully%s", duration)
        exit_code = 0
    else:
        _cli_logger.error("Flow execution failed")
        for node_result in result.node_step_result:
            if not node_result.success and node_result.error:
                node_name = node_result.node_name or f"Node {node_result.node_id}"
                _cli_logger.error("  - %s: %s", node_name, node_result.error)
        exit_code = 1

    # Process exits right after; whatever misses this budget is spooled for a later run.
    core_telemetry.flush(0.3)
    return exit_code


def _complete_run(
    run_id: int,
    success: bool,
    nodes_completed: int,
    number_of_nodes: int = 0,
    node_results_json: str | None = None,
) -> None:
    """Report results back to a pre-created run record."""
    _cli_logger.debug(
        "Completing run %d: success=%s, nodes_completed=%d, number_of_nodes=%d",
        run_id,
        success,
        nodes_completed,
        number_of_nodes,
    )
    try:
        from shared.run_completion import complete_run

        complete_run(
            run_id=run_id,
            success=success,
            nodes_completed=nodes_completed,
            number_of_nodes=number_of_nodes,
            node_results_json=node_results_json,
        )
    except Exception as e:
        _cli_logger.exception("Failed to update run record %d: %s", run_id, e)

    # Deliver from the finishing subprocess instead of waiting for the next scheduler tick.
    try:
        from shared.notifications.processor import process_pending_notifications

        process_pending_notifications()
    except Exception as e:
        _cli_logger.warning("Notification processing failed: %s", e)


def main(argv: list[str]) -> int:
    """Parse ``--run-flow <path> --run-id <id>`` from ``argv`` and run the flow."""
    idx = argv.index("--run-flow")
    flow_path = argv[idx + 1] if idx + 1 < len(argv) else None
    run_id = None
    if "--run-id" in argv:
        rid_idx = argv.index("--run-id")
        run_id = int(argv[rid_idx + 1]) if rid_idx + 1 < len(argv) else None
    if not flow_path:
        print("Usage: flowfile_core --run-flow <path> --run-id <id>", file=sys.stderr)
        return 1
    if run_id is None:
        print("Error: --run-id is required", file=sys.stderr)
        return 1
    return run_flow_cli(flow_path, run_id)
