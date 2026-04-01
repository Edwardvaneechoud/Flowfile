import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("azurite_commands")


def start_azurite():
    """CLI entry point to start the Azurite container."""
    from . import fixtures

    if not fixtures.is_docker_available():
        logger.warning("Docker is not available. Cannot start Azurite container.")
        print("\n" + "=" * 50)
        print("SKIPPING: Docker is not available on this system")
        print("Tests requiring Docker will need to be skipped")
        print("=" * 50 + "\n")
        return 0

    if fixtures.start_azurite_container():
        print("Azurite started successfully.")
        return 0
    return 1


def stop_azurite():
    """CLI entry point to stop the Azurite container."""
    from . import fixtures

    if not fixtures.is_docker_available():
        logger.warning("Docker is not available. Cannot stop Azurite container.")
        print("\n" + "=" * 50)
        print("SKIPPING: Docker is not available on this system")
        print("=" * 50 + "\n")
        return 0

    if fixtures.stop_azurite_container():
        print("Azurite stopped successfully.")
        return 0
    return 1
