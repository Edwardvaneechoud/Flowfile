#  flowfile/__main__.py

import sys
from pathlib import Path


def _parse_arg_pairs(arg_list: list[str] | None) -> dict[str, str]:
    """Parse --arg key=value pairs into a dict."""
    if not arg_list:
        return {}
    result = {}
    for item in arg_list:
        if "=" not in item:
            print(f"Error: Invalid --arg format: {item!r} (expected key=value)", file=sys.stderr)
            sys.exit(1)
        key, _, value = item.partition("=")
        result[key.strip()] = value.strip()
    return result


def run_flow(flow_path: str, arg_values: dict[str, str] | None = None) -> int:
    """
    Load and execute a flow from a YAML/JSON file.

    Args:
        flow_path: Path to the flow file (.yaml, .yml, or .json)
        arg_values: Optional dict of flow argument values (name → raw value)

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
        return 1

    if path.suffix.lower() not in (".yaml", ".yml", ".json"):
        print(f"Error: Unsupported file format: {path.suffix}", file=sys.stderr)
        print("Supported formats: .yaml, .yml, .json", file=sys.stderr)
        return 1

    print(f"Loading flow from: {flow_path}")

    try:
        flow = open_flow(path)
    except Exception as e:
        print(f"Error loading flow: {e}", file=sys.stderr)
        return 1

    # Force local execution for CLI - no worker service needed
    flow.execution_location = "local"

    # Resolve flow arguments if any are defined or provided
    if arg_values or flow.flow_settings.flow_arguments:
        try:
            flow.resolve_arguments(arg_values or {})
            if arg_values:
                print(f"Flow arguments: {arg_values}")
        except ValueError as e:
            print(f"Error resolving flow arguments: {e}", file=sys.stderr)
            return 1

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
        return 1

    if result is None:
        print("Error: Flow execution returned no result", file=sys.stderr)
        return 1

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


def main():
    """
    Display information about FlowFile when run directly as a module.
    """
    import argparse

    import flowfile

    parser = argparse.ArgumentParser(description="FlowFile: A visual ETL tool with a Polars-like API")
    parser.add_argument("command", nargs="?", choices=["run"], help="Command to execute")
    parser.add_argument(
        "component", nargs="?", choices=["ui", "core", "worker", "flow"], help="Component to run"
    )
    parser.add_argument("file_path", nargs="?", help="Path to flow file (for 'flow' component)")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind the server to")
    parser.add_argument("--port", type=int, default=63578, help="Port to bind the server to")
    parser.add_argument("--no-browser", action="store_true", help="Don't open a browser window")
    parser.add_argument(
        "--arg", action="append", metavar="KEY=VALUE",
        help="Set a flow argument (repeatable, e.g. --arg input_path=/data/file.csv --arg threshold=0.5)",
    )

    # Parse arguments
    args = parser.parse_args()

    if args.command == "run" and args.component:
        if args.component == "ui":
            try:
                flowfile.start_web_ui(open_browser=not args.no_browser)
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
            arg_values = _parse_arg_pairs(args.arg)
            sys.exit(run_flow(args.file_path, arg_values=arg_values))
    else:
        # Default action - show info
        print(f"FlowFile v{flowfile.__version__}")
        print("A framework combining visual ETL with a Polars-like API")
        print("\nUsage:")
        print("  # Start the FlowFile web UI with integrated services")
        print("  flowfile run ui")
        print("")
        print("  # Run a flow from a file")
        print("  flowfile run flow my_pipeline.yaml")
        print("  flowfile run flow my_pipeline.yaml --arg input_path=/data/file.csv --arg threshold=0.5")
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
