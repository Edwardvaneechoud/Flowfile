"""
Command-line interface for SQL Server test database management.

This module provides command-line functions that can be called via Poetry scripts
to start and stop SQL Server containers with sample data.
"""

import argparse
import logging
import sys

from . import fixtures

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("mssql_commands")


def start_mssql():
    """
    Start SQL Server container with sample data.

    This function is the entry point for the 'start_mssql' Poetry script.
    """
    if not fixtures.is_docker_available():
        logger.warning("Docker is not available. Cannot start SQL Server container.")
        print("\n" + "=" * 50)
        print("SKIPPING: Docker is not available on this system")
        print("Tests requiring Docker will need to be skipped")
        print("=" * 50 + "\n")
        return 0

    parser = argparse.ArgumentParser(description="Start SQL Server container with sample data")
    parser.add_argument("--port", type=int, default=1434, help="Port to expose SQL Server on (default: 1434)")
    parser.add_argument(
        "--container-name", default="test-mssql-sample", help="Docker container name (default: test-mssql-sample)"
    )
    parser.add_argument(
        "--image",
        default="mcr.microsoft.com/mssql/server:2022-latest",
        help="Docker image (default: mcr.microsoft.com/mssql/server:2022-latest)",
    )

    args = parser.parse_args()

    _, success = fixtures.start_mssql_container(args.container_name, args.port, args.image)
    if success:
        fixtures.print_connection_info("localhost", args.port, fixtures.MSSQL_DB, fixtures.MSSQL_USER, args.container_name)
        return 0
    return 1


def stop_mssql():
    """
    Stop SQL Server container.

    This function is the entry point for the 'stop_mssql' Poetry script.
    """
    if not fixtures.is_docker_available():
        logger.warning("Docker is not available. No SQL Server container to stop.")
        return 0

    parser = argparse.ArgumentParser(description="Stop SQL Server container")
    parser.add_argument(
        "--container-name", default="test-mssql-sample", help="Docker container name (default: test-mssql-sample)"
    )
    parser.add_argument(
        "--timeout", type=int, default=15, help="Timeout for graceful container stop in seconds (default: 15)"
    )

    args = parser.parse_args()

    if fixtures.stop_mssql_container(args.container_name, args.timeout):
        print(f"Container {args.container_name} stopped successfully")
        return 0
    else:
        print(f"Failed to stop container {args.container_name}")
        return 1


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "start":
        sys.exit(start_mssql())
    elif len(sys.argv) > 1 and sys.argv[1] == "stop":
        sys.exit(stop_mssql())
    else:
        print("Usage: python -m test_utils.mssql.commands [start|stop] [options]")
        sys.exit(1)
