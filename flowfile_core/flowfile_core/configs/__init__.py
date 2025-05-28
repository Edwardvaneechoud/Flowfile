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

# Clear any existing handlers
if logger.hasHandlers():
    logger.handlers.clear()

# Try to determine the best output stream
output_stream = None
if hasattr(sys.stdout, 'isatty') and sys.stdout.isatty():
    output_stream = sys.stdout
elif hasattr(sys.stderr, 'isatty') and sys.stderr.isatty():
    output_stream = sys.stderr
else:
    # Use __stdout__ for debugger environments (PyDev, PyCharm, etc.)
    output_stream = sys.__stdout__

console_handler = logging.StreamHandler(output_stream)
console_handler.setLevel(logging.INFO)

# Create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

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