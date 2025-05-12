#  flowfile/__main__.py

def main():
    """
    Display information about FlowFile when run directly as a module.
    """
    import flowfile
    import argparse

    parser = argparse.ArgumentParser(description="FlowFile: A visual ETL tool with a Polars-like API")
    parser.add_argument("command", nargs="?", choices=["run"], help="Command to execute")
    parser.add_argument("component", nargs="?", choices=["ui", "core", "worker"],
                        help="Component to run (ui, core, or worker)")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind the server to")
    parser.add_argument("--port", type=int, default=63578, help="Port to bind the server to")
    parser.add_argument("--no-browser", action="store_true", help="Don't open a browser window")

    # Parse arguments
    args = parser.parse_args()

    if args.command == "run" and args.component:
        if args.component == "ui":
            try:
                flowfile.start_web_ui(
                    host=args.host,
                    port=args.port,
                    open_browser=not args.no_browser
                )
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
    else:
        # Default action - show info
        print(f"FlowFile v{flowfile.__version__}")
        print("A framework combining visual ETL with a Polars-like API")
        print("\nUsage:")
        print("  # Start the FlowFile web UI with integrated services")
        print("  flowfile run ui")
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