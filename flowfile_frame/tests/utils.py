import code
import platform
import subprocess
from pathlib import Path
from typing import Any

from flowfile_frame._console_source import install_hook


def is_docker_available():
    """Check if Docker is running."""
    if platform.system() == "Windows":
        return False
    try:
        subprocess.run(["docker", "info"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def find_parent_directory(target_dir_name, start_path=None):
    """Navigate up directories until finding the target directory"""
    current_path = Path(start_path) if start_path else Path.cwd()

    while current_path != current_path.parent:
        if current_path.name == target_dir_name:
            return current_path
        if current_path.name == target_dir_name:
            return current_path
        current_path = current_path.parent

    raise FileNotFoundError(f"Directory '{target_dir_name}' not found")


def console_namespace(*fragments: str, filename: str = "<console>") -> dict[str, Any]:
    """Run each fragment through ``code.InteractiveConsole`` under ``filename`` (``<input>`` is PyCharm's console's)."""
    install_hook()  # pytest is not a console, so importing flowfile_frame left the hook out
    namespace: dict[str, Any] = {"__name__": "__main__"}
    interpreter = code.InteractiveConsole(namespace)
    for fragment in fragments:
        assert interpreter.runsource(fragment, filename, "exec") is False
    return namespace
