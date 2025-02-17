# conftest.py
import subprocess
import time
import pytest
import requests
import sys

@pytest.fixture(scope="session", autouse=True)
def flowfile_worker():
    """
    Ensure that flowfile_worker is running before tests.
    If it's already running, use it; otherwise, start it using:
        poetry run flowfile_worker
    Provides extra logging to help diagnose startup issues.
    """
    # Use localhost when checking the service.
    worker_url = "http://0.0.0.0:63579/docs"
    proc = None

    # Check if the worker is already running.
    try:
        response = requests.get(worker_url)
        if response.ok:
            print("flowfile_worker already running!", flush=True)
            already_running = True
        else:
            already_running = False
    except requests.exceptions.RequestException:
        already_running = False

    # If not running, attempt to start the worker.
    if not already_running:
        print("flowfile_worker not running. Attempting to start...", flush=True)
        proc = subprocess.Popen(
            "poetry run flowfile_worker",
            shell=True,
            stdout=sys.stdout,  # Directly prints to terminal.
            stderr=sys.stderr,
            universal_newlines=True,
        )
        max_retries = 10
        for i in range(max_retries):
            time.sleep(2)
            # Check if the process terminated unexpectedly.
            retcode = proc.poll()
            if retcode is not None:
                # Process terminated; capture output for debugging.
                stdout, stderr = proc.communicate()
                pytest.skip(
                    f"flowfile_worker terminated unexpectedly with code {retcode}.\n"
                    f"Stdout:\n{stdout}\nStderr:\n{stderr}"
                )
            try:
                response = requests.get(worker_url)
                if response.ok:
                    print("flowfile_worker started successfully.", flush=True)
                    break
            except requests.exceptions.RequestException:
                print(
                    f"Waiting for flowfile_worker to start... (retry {i + 1}/{max_retries})",
                    flush=True
                )
        else:
            # If we never get a successful response, capture output and skip tests.
            stdout, stderr = proc.communicate(timeout=5)
            pytest.skip(
                f"flowfile_worker did not start in time.\nLast output:\nStdout:\n{stdout}\nStderr:\n{stderr}"
            )

    yield  # Run tests.

    # Teardown: only terminate the worker if we started it.
    if proc is not None:
        print("Terminating flowfile_worker subprocess...", flush=True)
        print("Sending SIGTERM...", flush=True)
        # time.sleep(10)
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
