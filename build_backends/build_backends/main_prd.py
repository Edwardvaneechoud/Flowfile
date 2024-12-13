import os
import subprocess
import platform
from concurrent.futures import ProcessPoolExecutor, wait

import subprocess
import time
import requests
from statistics import mean, stdev
import sys
from datetime import datetime


def wait_for_endpoint(url, timeout=60):
    """Wait for the endpoint to become available."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = requests.get(url)
            if response.status_code == 200:
                return True
        except requests.exceptions.ConnectionError:
            time.sleep(0.1)
    return False


def shutdown_service():
    """Shutdown the service gracefully using the shutdown endpoint."""
    try:
        response = requests.post(
            'http://0.0.0.0:63578/shutdown',
            headers={'accept': 'application/json'},
            data=''
        )
        print("Shutdown request sent, waiting for service to stop...")
        time.sleep(1)  # Wait 10 seconds to ensure the service is fully stopped
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error shutting down service: {e}")
        return False


def measure_startup_time(executable_path):
    """Measure the startup time of the executable."""
    start_time = time.time()

    # Start the process
    process = subprocess.Popen([executable_path])

    # Wait for the endpoint to become available
    endpoint_url = "http://0.0.0.0:63578/docs"
    if not wait_for_endpoint(endpoint_url):
        print(f"Error: Endpoint did not become available for {executable_path}")
        process.kill()
        return None

    elapsed_time = time.time() - start_time

    # Gracefully shutdown the service
    if not shutdown_service():
        print("Failed to shutdown service gracefully, killing process...")
        process.kill()
        time.sleep(1)

    return elapsed_time


def run_comparison_test(old_exe, new_exe, num_runs=3):
    """Run multiple comparison tests and print statistics."""
    print(f"\nStarting comparison test at {datetime.now()}")
    print(f"Number of runs: {num_runs}")
    print("\nExecutables being tested:")
    print(f"Old: {old_exe}")
    print(f"New: {new_exe}")

    old_times = []
    new_times = []

    for i in range(num_runs):
        print(f"\nRun {i + 1}/{num_runs}")

        # Test old executable
        print("Testing old executable...")
        old_time = measure_startup_time(old_exe)
        if old_time is not None:
            old_times.append(old_time)
            print(f"Old startup time: {old_time:.3f} seconds")

        # Test new executable
        print("Testing new executable...")
        new_time = measure_startup_time(new_exe)
        if new_time is not None:
            new_times.append(new_time)
            print(f"New startup time: {new_time:.3f} seconds")

    # Print results
    print("\nResults:")
    print("-" * 50)
    if old_times:
        print(f"Old executable:")
        print(f"  Average: {mean(old_times):.3f} seconds")
        print(f"  Std Dev: {stdev(old_times):.3f} seconds" if len(old_times) > 1 else "  Std Dev: N/A")
        print(f"  Min: {min(old_times):.3f} seconds")
        print(f"  Max: {max(old_times):.3f} seconds")

    if new_times:
        print(f"\nNew executable:")
        print(f"  Average: {mean(new_times):.3f} seconds")
        print(f"  Std Dev: {stdev(new_times):.3f} seconds" if len(new_times) > 1 else "  Std Dev: N/A")
        print(f"  Min: {min(new_times):.3f} seconds")
        print(f"  Max: {max(new_times):.3f} seconds")

    if old_times and new_times:
        improvement = (mean(old_times) - mean(new_times)) / mean(old_times) * 100
        print(f"\nPerformance difference:")
        print(f"  {improvement:.1f}% {'faster' if improvement > 0 else 'slower'} than old version")


if __name__ == "__main__":
    old_exe = '/Users/edwardvanechoud/personal_dev/Flowfile/dist/flowfile_core/flowfile_core'
    new_exe = '/Users/edwardvanechoud/personal_dev/Flowfile/dist_flowfile_core/flowfile_core'

    run_comparison_test(old_exe, old_exe)

# def build_backend(directory, script_name, output_name, hidden_imports=None):
#     try:
#         script_path = os.path.join(directory, script_name)
#         command = [
#             "python", "-m", "nuitka",
#             "--onefile",
#             "--standalone",
#             "--assume-yes-for-downloads",
#             "--include-package=tempfile",
#             "--include-package=polars",
#             "--include-package=fastexcel",
#             "--include-package=snowflake.connector"
#         ]
#
#         if hidden_imports:
#             for imp in hidden_imports:
#                 if '.' not in imp:
#                     command.extend(["--include-package=" + imp])
#                 else:
#                     command.extend(["--include-module=" + imp])
#
#         dist_folder = f"dist_{output_name}"
#         os.makedirs(dist_folder, exist_ok=True)
#         ext = ".exe" if platform.system() == "Windows" else ""
#         command.extend([
#             f"--output-dir={dist_folder}",
#             f"--output-filename={output_name}{ext}",
#             script_path
#         ])
#
#         print(f"Starting build for {output_name}")
#         result = subprocess.run(command, check=True)
#         print(f"Build completed for {output_name} with exit code {result.returncode}")
#         return result.returncode
#
#     except subprocess.CalledProcessError as e:
#         print(f"Error while building {script_name}: {e}")
#         return 1
#
#
# def main():
#     common_imports = [
#         "fastexcel",
#         "polars",
#         "snowflake.connector",
#         "snowflake.connector.snow_logging",
#         "snowflake.connector.errors"
#     ]
#
#     builds = [
#         {
#             "directory": os.path.join("flowfile_worker", "flowfile_worker"),
#             "script_name": "main.py",
#             "output_name": "flowfile_worker",
#             "hidden_imports": ["multiprocessing", "multiprocessing.resource_tracker",
#                                "multiprocessing.sharedctypes", "uvicorn",
#                                "uvicorn.logging", "uvicorn.protocols.http",
#                                "uvicorn.protocols.websockets"] + common_imports
#         },
#         {
#             "directory": os.path.join("flowfile_core", "flowfile_core"),
#             "script_name": "main.py",
#             "output_name": "flowfile_core",
#             "hidden_imports": ["passlib.handlers.bcrypt"] + common_imports
#         }
#     ]
#
#     with ProcessPoolExecutor(max_workers=2) as executor:
#         futures = [executor.submit(build_backend, **build) for build in builds]
#         wait(futures)
#
#         for future in futures:
#             if future.result() != 0:
#                 raise Exception("One or more builds failed")
#
#
# if __name__ == "__main__":
#     main()