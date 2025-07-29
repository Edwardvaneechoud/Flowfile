import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("postgres_commands")


def start_minio():
    """Start MinIO container for S3 testing"""
    from . import fixtures
    if not fixtures.is_docker_available():
        logger.warning("Docker is not available. Cannot start PostgreSQL container.")
        print("\n" + "=" * 50)
        print("SKIPPING: Docker is not available on this system")
        print("Tests requiring Docker will need to be skipped")
        print("=" * 50 + "\n")
        return 0  # Return success to allow pipeline to continue


    if fixtures.start_minio_container():
        print(f"MinIO started at http://localhost:{fixtures.MINIO_PORT}")
        print(f"Access Key: {fixtures.MINIO_ACCESS_KEY}")
        return 0
    return 1


def stop_minio():
    """Stop MinIO container"""
    from . import fixtures

    if not fixtures.is_docker_available():
        logger.warning("Docker is not available. Cannot stop MinIO container.")
        print("\n" + "=" * 50)
        print("SKIPPING: Docker is not available on this system")
        print("Tests requiring Docker will need to be skipped")
        print("=" * 50 + "\n")
        return 0

    if fixtures.stop_minio_container():
        print("MinIO stopped successfully")
        return 0
    return 1