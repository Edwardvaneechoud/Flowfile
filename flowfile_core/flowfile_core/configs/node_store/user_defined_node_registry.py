import importlib.util
import inspect
import logging
import sys
from pathlib import Path

from flowfile_core.flowfile.node_designer.custom_node import CustomNodeBase
from shared import storage

logger = logging.getLogger(__name__)


def get_all_custom_nodes() -> dict[str, type[CustomNodeBase]]:
    """
    Scan the user-defined nodes directory and import all CustomNodeBase subclasses.

    Returns:
        Dictionary mapping node names to node classes
    """
    custom_nodes = {}

    # Get the directory path where user-defined nodes are stored
    nodes_directory = storage.user_defined_nodes_icons

    # Convert to Path object for easier handling
    nodes_path = Path(nodes_directory)

    if not nodes_path.exists() or not nodes_path.is_dir():
        print(f"Warning: Nodes directory {nodes_path} does not exist or is not a directory")
        return custom_nodes

    # Scan all Python files in the directory
    for file_path in nodes_path.glob("*.py"):
        # Skip __init__.py and other special files
        if file_path.name.startswith("__"):
            continue

        try:
            # Load the module dynamically
            module_name = file_path.stem  # filename without extension
            spec = importlib.util.spec_from_file_location(module_name, file_path)

            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)

                # Add to sys.modules to handle imports within the module
                sys.modules[module_name] = module

                # Execute the module
                spec.loader.exec_module(module)

                # Inspect the module for CustomNodeBase subclasses
                for name, obj in inspect.getmembers(module):
                    # Check if it's a class and a subclass of CustomNodeBase
                    # but not CustomNodeBase itself
                    if inspect.isclass(obj) and issubclass(obj, CustomNodeBase) and obj is not CustomNodeBase:
                        # Use the node_name attribute if it exists, otherwise use class name
                        node_name = getattr(obj, "node_name", name)
                        custom_nodes[node_name] = obj
                        print(f"Loaded custom node: {node_name} from {file_path.name}")

        except Exception as e:
            print(f"Error loading module from {file_path}: {e}")
            # Continue with other files even if one fails
            continue

    return custom_nodes


def get_all_custom_nodes_with_validation() -> dict[str, type[CustomNodeBase]]:
    """
    Enhanced version that validates the nodes before adding them.
    """

    custom_nodes = {}
    nodes_path = storage.user_defined_nodes_directory

    if not nodes_path.exists():
        return custom_nodes

    for file_path in nodes_path.glob("*.py"):
        if file_path.name.startswith("__"):
            continue

        try:
            module_name = file_path.stem
            spec = importlib.util.spec_from_file_location(module_name, file_path)

            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                sys.modules[module_name] = module
                spec.loader.exec_module(module)

                for name, obj in inspect.getmembers(module):
                    if inspect.isclass(obj) and issubclass(obj, CustomNodeBase) and obj is not CustomNodeBase:
                        try:
                            _obj = obj()
                            # Validate that the node has required attributes
                            if not hasattr(_obj, "node_name"):
                                logger.error(f"Warning: {name} missing node_name attribute")
                                raise ValueError(f"Node {name} must implement a node_name attribute")

                            if not hasattr(_obj, "settings_schema"):
                                logger.error(f"Warning: {name} missing settings_schema attribute")
                                raise ValueError(f"Node {name} must implement a settings_schema attribute")

                            if not hasattr(_obj, "process"):
                                logger.error(f"Warning: {name} missing process method")
                                raise ValueError(f"Node {name} must implement a process method")
                            if not (storage.user_defined_nodes_icons / _obj.node_icon).exists():
                                logger.warning(
                                    f"Warning: Icon file does not exist for node {_obj.node_name} at {_obj.node_icon} "
                                    "Falling back to default icon."
                                )

                            node_name = _obj.to_node_template().item
                            custom_nodes[node_name] = obj
                            print(f"✓ Loaded: {node_name} from {file_path.name}")
                        except Exception as e:
                            print(f"Error validating node {name} in {file_path}: {e}")
                            continue
        except SyntaxError as e:
            print(f"Syntax error in {file_path}: {e}")
        except ImportError as e:
            print(f"Import error in {file_path}: {e}")
        except Exception as e:
            print(f"Unexpected error loading {file_path}: {e}")

    return custom_nodes


def get_custom_nodes_lazy() -> list[type[CustomNodeBase]]:
    """
    Returns a list of custom node classes without instantiating them.
    Useful for registration or catalog purposes.
    """
    nodes = []
    nodes_path = Path(storage.user_defined_nodes_directory)

    if not nodes_path.exists():
        return nodes

    for file_path in nodes_path.glob("*.py"):
        if file_path.name.startswith("__"):
            continue

        try:
            # Create a unique module name to avoid conflicts
            module_name = f"custom_node_{file_path.stem}_{id(file_path)}"
            spec = importlib.util.spec_from_file_location(module_name, file_path)

            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)

                for name, obj in inspect.getmembers(module):
                    if (
                        inspect.isclass(obj)
                        and issubclass(obj, CustomNodeBase)
                        and obj is not CustomNodeBase
                        and obj.__module__ == module.__name__
                    ):  # Only get classes defined in this module
                        nodes.append(obj)

        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            continue

    return nodes


# Example usage function that matches your original pattern
def add_custom_node(node_class: type[CustomNodeBase], registry: dict[str, type[CustomNodeBase]]):
    """Add a single custom node to the registry."""
    if hasattr(node_class, "node_name"):
        registry[node_class.node_name] = node_class
    else:
        registry[node_class.__name__] = node_class


def get_all_nodes_from_standard_location() -> dict[str, type[CustomNodeBase]]:
    """
    Main function to get all custom nodes from the standard location.
    This matches your original function signature.
    """

    return get_all_custom_nodes_with_validation()


def load_single_node_from_file(file_path: Path) -> type[CustomNodeBase] | None:
    """
    Load a single custom node from a specific file.

    Args:
        file_path: Path to the Python file containing the custom node

    Returns:
        The custom node class if found and valid, None otherwise
    """
    if not file_path.exists():
        logger.error(f"File not found: {file_path}")
        return None

    try:
        # Create a unique module name to avoid conflicts with cached modules
        module_name = f"custom_node_{file_path.stem}_{id(file_path)}"

        # Remove old module from sys.modules if it exists (for reloading)
        old_module_name = file_path.stem
        if old_module_name in sys.modules:
            del sys.modules[old_module_name]

        spec = importlib.util.spec_from_file_location(module_name, file_path)

        if spec and spec.loader:
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)

            for name, obj in inspect.getmembers(module):
                if inspect.isclass(obj) and issubclass(obj, CustomNodeBase) and obj is not CustomNodeBase:
                    try:
                        _obj = obj()
                        # Validate required attributes
                        if not hasattr(_obj, "node_name"):
                            raise ValueError(f"Node {name} must have a node_name attribute")
                        if not hasattr(_obj, "settings_schema"):
                            raise ValueError(f"Node {name} must have a settings_schema attribute")
                        if not hasattr(_obj, "process"):
                            raise ValueError(f"Node {name} must have a process method")

                        logger.info(f"✓ Loaded: {_obj.node_name} from {file_path.name}")
                        return obj
                    except Exception as e:
                        logger.error(f"Error validating node {name}: {e}")
                        raise

    except SyntaxError as e:
        logger.error(f"Syntax error in {file_path}: {e}")
        raise
    except ImportError as e:
        logger.error(f"Import error in {file_path}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error loading {file_path}: {e}")
        raise

    return None


def unload_node_by_name(node_name: str) -> bool:
    """
    Remove a node from sys.modules cache by its name.
    This helps ensure the node can be cleanly reloaded later.

    Args:
        node_name: The name of the node to unload

    Returns:
        True if any modules were removed, False otherwise
    """
    # Convert node name to potential module names
    module_stem = node_name.lower().replace(" ", "_")

    # Find and remove any matching modules from sys.modules
    modules_to_remove = [
        key for key in sys.modules.keys() if key == module_stem or key.startswith(f"custom_node_{module_stem}")
    ]

    for mod_name in modules_to_remove:
        del sys.modules[mod_name]
        logger.info(f"Removed module from cache: {mod_name}")

    return len(modules_to_remove) > 0
