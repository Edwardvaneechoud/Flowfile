
# Runtime hook to handle connectorx metadata issues
import sys
import importlib.metadata

# Store original version function
original_version = importlib.metadata.version

# Create patched version function
def patched_version(distribution_name):
    try:
        return original_version(distribution_name)
    except (importlib.metadata.PackageNotFoundError, StopIteration):
        # Handle specific packages
        if distribution_name == 'connectorx':
            return '0.4.3'  # Hardcode the version
        # Let other package errors propagate normally
        raise

# Apply the patch
importlib.metadata.version = patched_version
print("Applied connectorx metadata patch")
