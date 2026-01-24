import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("azurite_commands")


def start_azurite():
    """Start Azurite container for ADLS testing"""
    from . import fixtures

    if not fixtures.is_docker_available():
        logger.warning("Docker is not available. Cannot start Azurite container.")
        print("\n" + "=" * 50)
        print("SKIPPING: Docker is not available on this system")
        print("Tests requiring Docker will need to be skipped")
        print("=" * 50 + "\n")
        return 0  # Return success to allow pipeline to continue

    if fixtures.start_azurite_container():
        print(f"Azurite started at http://localhost:{fixtures.AZURITE_BLOB_PORT}")
        print(f"Account Name: {fixtures.AZURITE_ACCOUNT_NAME}")
        print(f"Account Key: {fixtures.AZURITE_ACCOUNT_KEY}")
        print("\nTest containers created:")
        print("  - test-container")
        print("  - flowfile-test")
        print("  - sample-data")
        print("  - worker-test-container")
        print("  - demo-container")
        return 0
    return 1


def stop_azurite():
    """Stop Azurite container"""
    from . import fixtures

    if not fixtures.is_docker_available():
        logger.warning("Docker is not available. Cannot stop Azurite container.")
        print("\n" + "=" * 50)
        print("SKIPPING: Docker is not available on this system")
        print("Tests requiring Docker will need to be skipped")
        print("=" * 50 + "\n")
        return 0

    if fixtures.stop_azurite_container():
        print("Azurite stopped successfully")
        return 0
    return 1
