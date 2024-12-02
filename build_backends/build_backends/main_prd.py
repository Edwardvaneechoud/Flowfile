import os
import subprocess
import platform
from concurrent.futures import ProcessPoolExecutor, wait


def build_backend(directory, script_name, output_name, hidden_imports=None):
    try:
        script_path = os.path.join(directory, script_name)
        command = [
            "python", "-m", "nuitka",
            "--onefile",
            "--standalone",
            "--assume-yes-for-downloads",
            "--include-package=tempfile",
            "--include-package=polars",
            "--include-package=fastexcel",
            "--include-package=snowflake.connector"
        ]

        if hidden_imports:
            for imp in hidden_imports:
                if '.' not in imp:
                    command.extend(["--include-package=" + imp])
                else:
                    command.extend(["--include-module=" + imp])

        dist_folder = f"dist_{output_name}"
        os.makedirs(dist_folder, exist_ok=True)
        ext = ".exe" if platform.system() == "Windows" else ""
        command.extend([
            f"--output-dir={dist_folder}",
            f"--output-filename={output_name}{ext}",
            script_path
        ])

        print(f"Starting build for {output_name}")
        result = subprocess.run(command, check=True)
        print(f"Build completed for {output_name} with exit code {result.returncode}")
        return result.returncode

    except subprocess.CalledProcessError as e:
        print(f"Error while building {script_name}: {e}")
        return 1


def main():
    common_imports = [
        "fastexcel",
        "polars",
        "snowflake.connector",
        "snowflake.connector.snow_logging",
        "snowflake.connector.errors"
    ]

    builds = [
        {
            "directory": os.path.join("flowfile_worker", "flowfile_worker"),
            "script_name": "main.py",
            "output_name": "flowfile_worker",
            "hidden_imports": ["multiprocessing", "multiprocessing.resource_tracker",
                               "multiprocessing.sharedctypes", "uvicorn",
                               "uvicorn.logging", "uvicorn.protocols.http",
                               "uvicorn.protocols.websockets"] + common_imports
        },
        {
            "directory": os.path.join("flowfile_core", "flowfile_core"),
            "script_name": "main.py",
            "output_name": "flowfile_core",
            "hidden_imports": ["passlib.handlers.bcrypt"] + common_imports
        }
    ]

    with ProcessPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(build_backend, **build) for build in builds]
        wait(futures)

        for future in futures:
            if future.result() != 0:
                raise Exception("One or more builds failed")


if __name__ == "__main__":
    main()