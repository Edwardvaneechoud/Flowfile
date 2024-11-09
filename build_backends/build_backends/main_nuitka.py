import os
import subprocess


def build_backend(directory, script_name):
    try:
        script_path = os.path.join(directory, script_name)
        command = [
            "nuitka",
            "--onefile",  # Compiles to a single executable
            "--standalone",  # Includes all dependencies
            f"--output-dir=build/",  # Output to the build folder
            script_path,
        ]
        subprocess.run(command, check=True)
        executable_name = script_name.replace(".py", "")  # Assuming .py extension is removed
        os.rename(f"build/{executable_name}", f"../your-electron-app/backends/{executable_name}")
        print(f"Successfully built and moved {script_name}")
    except subprocess.CalledProcessError as e:
        print(f"Error while building {script_name}: {e}")


def main():
    # build_backend("flowfile_core/flowfile_core", "main.py")
    build_backend("flowfile_worker/flowfile_worker", "main.py")


if __name__ == "__main__":
    main()
