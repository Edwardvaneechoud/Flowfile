"""Local Docker image checks shared by the container fixtures: never pull a tag that is already present."""

import logging
import subprocess


def is_image_present(image: str) -> bool:
    """True when the image is already local, so no registry round-trip is needed."""
    return subprocess.run(["docker", "image", "inspect", image], capture_output=True, check=False).returncode == 0


def docker_daemon_responds(logger: logging.Logger, attempts: int = 3, timeout: int = 30) -> bool:
    """Whether ``docker info`` succeeds, asked again when the daemon gives no answer in time.

    A loaded CI runner's daemon can take longer than a few seconds to answer its first call;
    a daemon that is down refuses at once, so only a timeout is worth another attempt.
    """
    for attempt in range(1, attempts + 1):
        try:
            result = subprocess.run(["docker", "info"], capture_output=True, timeout=timeout, check=False)
        except subprocess.TimeoutExpired:
            logger.warning(f"docker info gave no answer within {timeout}s (attempt {attempt}/{attempts})")
            continue
        except (subprocess.SubprocessError, OSError) as exc:
            logger.warning(f"Error running Docker command: {exc}")
            return False
        if result.returncode != 0:
            error = result.stderr.decode(errors="replace").strip() or "(no stderr)"
            logger.warning(f"Docker is not operational: {error}")
            return False
        return True
    return False


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
