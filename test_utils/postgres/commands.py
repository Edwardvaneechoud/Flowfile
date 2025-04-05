"""
Command-line interface for PostgreSQL test database management.

This module provides command-line functions that can be called via Poetry scripts
to start and stop PostgreSQL containers with sample data.
"""

import argparse
import sys
import logging
from . import fixtures

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("postgres_commands")


def start_postgres():
    """
    Start PostgreSQL container with sample data.

    This function is the entry point for the 'start_postgres' Poetry script.
    It parses command-line arguments and starts a PostgreSQL container
    with the requested configuration.

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    # Check if Docker is available first
    if not fixtures.is_docker_available():
        logger.warning("Docker is not available. Cannot start PostgreSQL container.")
        print("\n" + "=" * 50)
        print("SKIPPING: Docker is not available on this system")
        print("Tests requiring Docker will need to be skipped")
        print("=" * 50 + "\n")
        return 0  # Return success to allow pipeline to continue

    parser = argparse.ArgumentParser(description="Start PostgreSQL container with sample data")
    parser.add_argument("--schema", choices=["movies", "stocks"], default="movies",
                        help="Sample schema to use (movies or stocks)")
    parser.add_argument("--port", type=int, default=5433,
                        help="Port to expose PostgreSQL on (default: 5433)")
    parser.add_argument("--user", default="testuser",
                        help="PostgreSQL username (default: testuser)")
    parser.add_argument("--password", default="testpass",
                        help="PostgreSQL password (default: testpass)")
    parser.add_argument("--db", default="testdb",
                        help="PostgreSQL database name (default: testdb)")
    parser.add_argument("--container-name", default="test-postgres-sample",
                        help="Docker container name (default: test-postgres-sample)")
    parser.add_argument("--image-tag", default="test-sample-db",
                        help="Docker image tag (default: test-sample-db)")

    args = parser.parse_args()

    # Setup and start
    if fixtures.setup_postgres_samples(args.schema, args.port, args.user, args.password, args.db, args.image_tag):
        if fixtures.start_postgres_container(args.container_name, args.port, args.image_tag)[1]:
            fixtures.print_connection_info(
                "localhost", args.port, args.db, args.user, args.password, args.container_name
            )
            return 0
    return 1


def stop_postgres():
    """
    Stop PostgreSQL container.

    This function is the entry point for the 'stop_postgres' Poetry script.
    It parses command-line arguments and stops the specified PostgreSQL container.

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    # Check if Docker is available first
    if not fixtures.is_docker_available():
        logger.warning("Docker is not available. No PostgreSQL container to stop.")
        return 0  # Return success to allow pipeline to continue

    parser = argparse.ArgumentParser(description="Stop PostgreSQL container")
    parser.add_argument("--container-name", default="test-postgres-sample",
                        help="Docker container name (default: test-postgres-sample)")
    parser.add_argument("--timeout", type=int, default=15,
                        help="Timeout for graceful container stop in seconds (default: 15)")

    args = parser.parse_args()

    if fixtures.stop_postgres_container(args.container_name, args.timeout):
        print(f"Container {args.container_name} stopped successfully")
        return 0
    else:
        print(f"Failed to stop container {args.container_name}")
        return 1


if __name__ == "__main__":
    # Allow direct script execution for testing
    if len(sys.argv) > 1 and sys.argv[1] == "start":
        sys.exit(start_postgres())
    elif len(sys.argv) > 1 and sys.argv[1] == "stop":
        sys.exit(stop_postgres())
    else:
        print("Usage: python -m test_utils.postgres.commands [start|stop] [options]")
        sys.exit(1)