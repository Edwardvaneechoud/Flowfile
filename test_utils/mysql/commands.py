"""
Command-line interface for MySQL test database management.

This module provides command-line functions that can be called via Poetry scripts
to start and stop MySQL containers with sample data.
"""

import argparse
import logging
import sys

from . import fixtures

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("mysql_commands")


def start_mysql():
    """
    Start MySQL container with sample data.

    This function is the entry point for the 'start_mysql' Poetry script.
    """
    # Check if Docker is available first
    if not fixtures.is_docker_available():
        logger.warning("Docker is not available. Cannot start MySQL container.")
        print("\n" + "=" * 50)
        print("SKIPPING: Docker is not available on this system")
        print("Tests requiring Docker will need to be skipped")
        print("=" * 50 + "\n")
        return 0

    parser = argparse.ArgumentParser(description="Start MySQL container with sample data")
    parser.add_argument("--port", type=int, default=3307, help="Port to expose MySQL on (default: 3307)")
    parser.add_argument("--user", default="testuser", help="MySQL username (default: testuser)")
    parser.add_argument("--password", default="testpass", help="MySQL password (default: testpass)")
    parser.add_argument("--db", default="testdb", help="MySQL database name (default: testdb)")
    parser.add_argument(
        "--container-name", default="test-mysql-sample", help="Docker container name (default: test-mysql-sample)"
    )
    parser.add_argument("--image", default="mysql:8", help="Docker image (default: mysql:8)")

    args = parser.parse_args()

    _, success = fixtures.start_mysql_container(args.container_name, args.port, args.image)
    if success:
        fixtures.print_connection_info("localhost", args.port, args.db, args.user, args.password, args.container_name)
        return 0
    return 1


def stop_mysql():
    """
    Stop MySQL container.

    This function is the entry point for the 'stop_mysql' Poetry script.
    """
    # Check if Docker is available first
    if not fixtures.is_docker_available():
        logger.warning("Docker is not available. No MySQL container to stop.")
        return 0

    parser = argparse.ArgumentParser(description="Stop MySQL container")
    parser.add_argument(
        "--container-name", default="test-mysql-sample", help="Docker container name (default: test-mysql-sample)"
    )
    parser.add_argument(
        "--timeout", type=int, default=15, help="Timeout for graceful container stop in seconds (default: 15)"
    )

    args = parser.parse_args()

    if fixtures.stop_mysql_container(args.container_name, args.timeout):
        print(f"Container {args.container_name} stopped successfully")
        return 0
    else:
        print(f"Failed to stop container {args.container_name}")
        return 1


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "start":
        sys.exit(start_mysql())
    elif len(sys.argv) > 1 and sys.argv[1] == "stop":
        sys.exit(stop_mysql())
    else:
        print("Usage: python -m test_utils.mysql.commands [start|stop] [options]")
        sys.exit(1)
