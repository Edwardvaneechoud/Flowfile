import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("postgres_commands")


def start_gcs():
    """Start GCS Server container for testing"""
    from . import fixtures
    if not fixtures.is_docker_available():
        logger.warning("Docker is not available. Cannot start PostgreSQL container.")
        print("\n" + "=" * 50)
        print("SKIPPING: Docker is not available on this system")
        print("Tests requiring Docker will need to be skipped")
        print("=" * 50 + "\n")
        return 0  # Return success to allow pipeline to continue


    if fixtures.start_gcs_container():
        print(f"MinIO started at http://localhost:{fixtures.GCS_PORT}")
        return 0
    return 1


def stop_gcs():
    """Stop MinIO container"""
    from . import fixtures

    if not fixtures.is_docker_available():
        logger.warning("Docker is not available. Cannot stop MinIO container.")
        print("\n" + "=" * 50)
        print("SKIPPING: Docker is not available on this system")
        print("Tests requiring Docker will need to be skipped")
        print("=" * 50 + "\n")
        return 0

    if fixtures.stop_gcs_container():
        print("MinIO stopped successfully")
        return 0
    return 1