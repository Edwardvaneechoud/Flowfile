# flowfile_core/flowfile_core/configs/__init__.py
import logging
import sys
from pathlib import Path
import os

os.environ["FLOWFILE_MODE"] = "electron"

# Create and configure the logger
logger = logging.getLogger('PipelineHandler')
logger.setLevel(logging.INFO)
logger.propagate = False

# Create console handler with a specific format
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)

# Create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

if logger.hasHandlers():
    logger.handlers.clear()
logger.addHandler(console_handler)

# Create logs directory in temp at startup
try:
    from tempfile import gettempdir
    log_dir = Path(gettempdir()) / "flowfile_logs"
    log_dir.mkdir(exist_ok=True)
except Exception as e:
    logger.warning(f"Failed to create logs directory: {e}")

# Initialize vault
logger.info("Logging system initialized")