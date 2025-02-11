# flowfile_worker.configs

import logging
import os
import platform

logging.basicConfig(format='%(asctime)s: %(message)s')
logger = logging.getLogger('FlowfileWorker')
logger.setLevel(logging.INFO)

def get_default_core_url():
    # Check for Docker environment first
    worker_host = os.getenv('WORKER_HOST', None)
    if worker_host:
        return f"http://{worker_host}:63579"

    # Fall back to default behavior
    if platform.system() == "Windows":
        return "http://127.0.0.1:63578"
    else:
        return "http://0.0.0.0:63578"


FLOWFILE_CORE_URI = get_default_core_url()

if __name__ == "__main__":
    get_default_core_url()