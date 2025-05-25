import logging

logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(message)s'
)

# Create and export the logger
logger = logging.getLogger('flow_frame')