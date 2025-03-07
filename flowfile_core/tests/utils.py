
import subprocess


def is_docker_available():
    """Check if Docker is running."""
    try:
        subprocess.run(["docker", "info"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


from pathlib import Path

Path("/var/folders/qf/skdr0r8s49g1_qf1lb7n3sfw0000gn/T/flowfile_logs/flow_808236605.log").read_text()