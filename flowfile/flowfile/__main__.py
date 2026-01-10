#  flowfile/__main__.py

import sys
from pathlib import Path


def run_flow(flow_path: str) -> int:
    """
    Load and execute a flow from a YAML/JSON file.

    Args:
        flow_path: Path to the flow file (.yaml, .yml, or .json)

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    from flowfile_core.flowfile.manage.io_flowfile import open_flow

    path = Path(flow_path)
    if not path.exists():
        print(f"Error: File not found: {flow_path}")
        return 1

    if path.suffix.lower() not in (".yaml", ".yml", ".json"):
        print(f"Error: Unsupported file format: {path.suffix}")
        print("Supported formats: .yaml, .yml, .json")
        return 1

    print(f"Loading flow from: {flow_path}")

    try:
        flow = open_flow(path)
    except Exception as e:
        print(f"Error loading flow: {e}")
        return 1

    # Force local execution for CLI - no worker service needed
    flow.execution_location = "local"

    flow_name = flow.flow_settings.name or f"Flow {flow.flow_id}"
    print(f"Running flow: {flow_name} (id={flow.flow_id})")
    print(f"Nodes: {len(flow.nodes)}")
    print("-" * 40)

    try:
        result = flow.run_graph()
    except Exception as e:
        print(f"Error running flow: {e}")
        return 1

    if result is None:
        print("Error: Flow execution returned no result")
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
        print("Flow execution failed")
        for node_result in result.node_step_result:
            if not node_result.success and node_result.error:
                node_name = node_result.node_name or f"Node {node_result.node_id}"
                print(f"  - {node_name}: {node_result.error}")
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
                print("Error: 'flow' component requires a file path")
                print("Usage: flowfile run flow <path-to-flow-file>")
                sys.exit(1)
            sys.exit(run_flow(args.file_path))
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
