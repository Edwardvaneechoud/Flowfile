import platform
import subprocess


def is_docker_available():
    """Check if Docker is running."""
    if platform.system() == "Windows":
        return False
    try:
        subprocess.run(["docker", "info"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False
