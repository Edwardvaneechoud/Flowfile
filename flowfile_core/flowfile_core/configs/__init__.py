import logging
from flowfile_core.configs.key_vault import SimpleKeyVault

# Create your custom logger
logger = logging.getLogger('PipelineHandler')
logger.setLevel(logging.INFO)
logger.propagate = False  # Add this line to prevent propagation

# Create a handler that outputs to the console (or a file if needed)
console_handler = logging.StreamHandler()

# Create a formatter and set it for the handler
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

# Remove existing handlers if they were added by an external package
if logger.hasHandlers():
    logger.handlers.clear()

# Add the custom handler to your logger
logger.addHandler(console_handler)

# Example usage
vault = SimpleKeyVault()
logger.info("Vault initialized")
