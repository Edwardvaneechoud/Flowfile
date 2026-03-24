#  flowfile/__main__.py

import sys
from pathlib import Path


def run_flow(flow_path: str, run_id: int | None = None) -> int:
    """
    Load and execute a flow from a YAML/JSON file.

    Args:
        flow_path: Path to the flow file (.yaml, .yml, or .json)
        run_id: Optional pre-created run ID to report results back to

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    # Disable worker communication for CLI execution
    from flowfile_core.configs.settings import OFFLOAD_TO_WORKER

    OFFLOAD_TO_WORKER.set(False)

    from flowfile_core.flowfile.manage.io_flowfile import open_flow

    path = Path(flow_path)
    if not path.exists():
        print(f"Error: File not found: {flow_path}", file=sys.stderr)
        _complete_run_if_needed(run_id, success=False, nodes_completed=0)
        return 1

    if path.suffix.lower() not in (".yaml", ".yml", ".json"):
        print(f"Error: Unsupported file format: {path.suffix}", file=sys.stderr)
        print("Supported formats: .yaml, .yml, .json", file=sys.stderr)
        _complete_run_if_needed(run_id, success=False, nodes_completed=0)
        return 1

    print(f"Loading flow from: {flow_path}")

    try:
        flow = open_flow(path)
    except Exception as e:
        print(f"Error loading flow: {e}", file=sys.stderr)
        _complete_run_if_needed(run_id, success=False, nodes_completed=0)
        return 1

    # Force local execution for CLI - no worker service needed
    flow.execution_location = "local"

    # Remove explore_data nodes - they're UI-only and require a worker service
    explore_data_nodes = [n.node_id for n in flow.nodes if n.node_type == "explore_data"]
    for node_id in explore_data_nodes:
        flow.delete_node(node_id)
    if explore_data_nodes:
        print(f"Skipping {len(explore_data_nodes)} explore_data node(s) (UI-only)")

    flow_name = flow.flow_settings.name or f"Flow {flow.flow_id}"
    print(f"Running flow: {flow_name} (id={flow.flow_id})")
    print(f"Nodes: {len(flow.nodes)}")
    flow.print_tree()
    print("-" * 40)

    try:
        result = flow.run_graph()
    except Exception as e:
        print(f"Error running flow: {e}", file=sys.stderr)
        _complete_run_if_needed(run_id, success=False, nodes_completed=0)
        return 1

    if result is None:
        print("Error: Flow execution returned no result", file=sys.stderr)
        _complete_run_if_needed(run_id, success=False, nodes_completed=0)
        return 1

    # Report results back to the pre-created run record
    _complete_run_if_needed(
        run_id,
        success=result.success,
        nodes_completed=result.nodes_completed,
        number_of_nodes=result.number_of_nodes,
    )

    # Display results
    print("-" * 40)
    if result.success:
        duration = ""
        if result.start_time and result.end_time:
            duration = f" in {(result.end_time - result.start_time).total_seconds():.2f}s"
        print(f"Flow completed successfully{duration}")
        print(f"Nodes completed: {result.nodes_completed}/{result.number_of_nodes}")
    else:
        print("Flow execution failed", file=sys.stderr)
        for node_result in result.node_step_result:
            if not node_result.success and node_result.error:
                node_name = node_result.node_name or f"Node {node_result.node_id}"
                print(f"  - {node_name}: {node_result.error}", file=sys.stderr)
        return 1

    return 0


def _complete_run_if_needed(
    run_id: int | None,
    success: bool,
    nodes_completed: int,
    number_of_nodes: int = 0,
) -> None:
    """Report results back to a pre-created run record if run_id is provided."""
    if run_id is None:
        return
    try:
        from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
        from flowfile_core.database.connection import get_db_context

        with get_db_context() as db:
            service = CatalogService(SQLAlchemyCatalogRepository(db))
            service.complete_run(
                run_id=run_id,
                success=success,
                nodes_completed=nodes_completed,
                number_of_nodes=number_of_nodes,
            )
    except Exception as e:
        print(f"Warning: Failed to update run record {run_id}: {e}", file=sys.stderr)


def main():
    """
    Display information about FlowFile when run directly as a module.
    """
    import argparse

    parser = argparse.ArgumentParser(description="FlowFile: A visual ETL tool with a Polars-like API")
    parser.add_argument("command", nargs="?", choices=["run"], help="Command to execute")
    parser.add_argument("component", nargs="?", choices=["ui", "core", "worker", "flow"], help="Component to run")
    parser.add_argument("file_path", nargs="?", help="Path to flow file (for 'flow' component)")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind the server to")
    parser.add_argument("--port", type=int, default=63578, help="Port to bind the server to")
    parser.add_argument("--no-browser", action="store_true", help="Don't open a browser window")
    parser.add_argument("--run-id", type=int, default=None, help="Pre-created run ID for scheduled runs")

    # Parse arguments
    args = parser.parse_args()

    if args.command == "run" and args.component:
        if args.component == "ui":
            try:
                from flowfile.web import start_server as start_web_ui

                start_web_ui(open_browser=not args.no_browser)
            except KeyboardInterrupt:
                print("\nFlowFile service stopped.")
        elif args.component == "core":
            # Only for direct core service usage
            from flowfile_core.main import run as run_core

            run_core(host=args.host, port=args.port)
        elif args.component == "worker":
            # Only for direct worker service usage
            from flowfile_worker.main import run as run_worker

            run_worker(host=args.host, port=args.port)
        elif args.component == "flow":
            if not args.file_path:
                print("Error: 'flow' component requires a file path", file=sys.stderr)
                print("Usage: flowfile run flow <path-to-flow-file>", file=sys.stderr)
                sys.exit(1)
            sys.exit(run_flow(args.file_path, run_id=args.run_id))
    else:
        # Default action - show info
        from importlib.metadata import PackageNotFoundError, version

        try:
            __version__ = version("Flowfile")
        except PackageNotFoundError:
            __version__ = "unknown"

        print(f"FlowFile v{__version__}")
        print("A framework combining visual ETL with a Polars-like API")
        print("\nUsage:")
        print("  # Start the FlowFile web UI with integrated services")
        print("  flowfile run ui")
        print("")
        print("  # Run a flow from a file")
        print("  flowfile run flow my_pipeline.yaml")
        print("")
        print("  # Advanced: Run individual components")
        print("  flowfile run core  # Start only the core service")
        print("  flowfile run worker  # Start only the worker service")
        print("")
        print("  # Options")
        print("  flowfile run ui --host 0.0.0.0 --port 8080  # Custom host/port")
        print("  flowfile run ui --no-browser  # Don't open browser")
        print("")
        print("  # Python API usage examples")
        print("  import flowfile as ff")
        print("  df = ff.read_csv('data.csv')")
        print("  result = df.filter(ff.col('value') > 10)")
        print("  ff.open_graph_in_editor(result)")


if __name__ == "__main__":
    main()
