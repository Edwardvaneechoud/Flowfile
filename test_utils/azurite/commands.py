from test_utils.azurite.fixtures import start_azurite_container, stop_azurite_container


def start_azurite():
    """CLI entry point to start the Azurite container."""
    if start_azurite_container():
        print("Azurite started successfully.")
    else:
        print("Failed to start Azurite.")
        raise SystemExit(1)


def stop_azurite():
    """CLI entry point to stop the Azurite container."""
    if stop_azurite_container():
        print("Azurite stopped successfully.")
    else:
        print("Failed to stop Azurite.")
        raise SystemExit(1)
