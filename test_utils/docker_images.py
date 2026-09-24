"""Local Docker image checks shared by the container fixtures: never pull a tag that is already present."""

import logging
import subprocess


def is_image_present(image: str) -> bool:
    """True when the image is already local, so no registry round-trip is needed."""
    return subprocess.run(["docker", "image", "inspect", image], capture_output=True, check=False).returncode == 0


def pull_image_if_missing(image: str, logger: logging.Logger, timeout: int = 300) -> bool:
    """Pull *image* only when it is not local; a pull of a present tag still round-trips to the registry."""
    if is_image_present(image):
        return True
    logger.info(f"Pulling Docker image {image}...")
    try:
        subprocess.run(["docker", "pull", image], capture_output=True, timeout=timeout, check=True)
    except subprocess.TimeoutExpired:
        logger.error(f"Timed out pulling Docker image {image}")
        return False
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to pull Docker image {image}: {e}")
        return False
    return True
