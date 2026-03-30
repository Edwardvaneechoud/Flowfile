from test_utils.gcs.fixtures import start_fake_gcs_container, stop_fake_gcs_container


def start_gcs():
    """CLI entry point to start the fake-gcs-server container."""
    if start_fake_gcs_container():
        print("fake-gcs-server started successfully.")
    else:
        print("Failed to start fake-gcs-server.")
        raise SystemExit(1)


def stop_gcs():
    """CLI entry point to stop the fake-gcs-server container."""
    if stop_fake_gcs_container():
        print("fake-gcs-server stopped successfully.")
    else:
        print("Failed to stop fake-gcs-server.")
        raise SystemExit(1)
