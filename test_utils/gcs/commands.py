import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("gcs_commands")


def start_gcs():
    """CLI entry point to start the fake-gcs-server container."""
    from . import fixtures

    if not fixtures.is_docker_available():
        logger.warning("Docker is not available. Cannot start fake-gcs-server container.")
        print("\n" + "=" * 50)
        print("SKIPPING: Docker is not available on this system")
        print("Tests requiring Docker will need to be skipped")
        print("=" * 50 + "\n")
        return 0

    if fixtures.start_fake_gcs_container():
        print("fake-gcs-server started successfully.")
        return 0
    return 1


def stop_gcs():
    """CLI entry point to stop the fake-gcs-server container."""
    from . import fixtures

    if not fixtures.is_docker_available():
        logger.warning("Docker is not available. Cannot stop fake-gcs-server container.")
        print("\n" + "=" * 50)
        print("SKIPPING: Docker is not available on this system")
        print("=" * 50 + "\n")
        return 0

    if fixtures.stop_fake_gcs_container():
        print("fake-gcs-server stopped successfully.")
        return 0
    return 1
