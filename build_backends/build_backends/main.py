import os
import subprocess
import platform


def build_backend(directory, script_name, output_name, hidden_imports=None):
    try:
        script_path = os.path.join(directory, script_name)

        # Base command
        command = [
            "pyinstaller",
            "--onefile",  # Compiles to a single executable
            "--clean",  # Cleans up previous builds
            "--log-level=INFO",  # Log level for debugging
            "--collect-all=tempfile",  # Collects all tempfile resources
            "--collect-all=polars",  # Collect all polars resources
            "--collect-all=fastexcel",  # Collect all fastexcel resources
            "--collect-all=snowflake.connector",  # Add Snowflake connector collection
        ]

        # Add hidden imports if provided
        if hidden_imports:
            for hidden_import in hidden_imports:
                command.extend(["--hidden-import", hidden_import])

        # Add the script path
        command.append(script_path)

        print(f"Running command: {command}")
        subprocess.run(command, check=True)

        # Define paths for renaming
        dist_folder = "dist"
        ext = ".exe" if platform.system() == "Windows" else ""
        generated_executable = os.path.join(dist_folder, script_name.replace(".py", ext))
        renamed_executable = os.path.join(dist_folder, output_name+ext)

        # Rename the executable
        if os.path.exists(generated_executable):
            os.rename(generated_executable, renamed_executable)
            print(f"Successfully renamed {generated_executable} to {renamed_executable}")
        else:
            print(f"Error: {generated_executable} not found")

        print(f"Successfully built and renamed {script_name} to {output_name}")

    except subprocess.CalledProcessError as e:
        print(f"Error while building {script_name}: {e}")


def main():
    # Build for flowfile_worker
    common_imports = [
        "fastexcel",
        "fastexcel.xlsx2csv",
        "fastexcel.Sheet",
        "polars",
        # Add minimal Snowflake imports
        "snowflake.connector",
        "snowflake.connector.snow_logging",
        "snowflake.connector.errors"
    ]

    build_backend(
        directory=os.path.join("flowfile_worker", "flowfile_worker"),
        script_name="main.py",
        output_name="flowfile_worker",
        hidden_imports=["multiprocessing", "multiprocessing.resource_tracker", "multiprocessing.sharedctypes",
                        "uvicorn", "uvicorn.logging", "uvicorn.protocols.http",
                        "uvicorn.protocols.websockets"] + common_imports
    )

    # Build for flowfile_core
    build_backend(
        directory=os.path.join("flowfile_core", "flowfile_core"),
        script_name="main.py",
        output_name="flowfile_core",
        hidden_imports=["passlib.handlers.bcrypt"] + common_imports
    )


if __name__ == "__main__":
    main()
