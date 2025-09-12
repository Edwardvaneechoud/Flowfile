import sys
import importlib.util
import inspect
from pathlib import Path
from typing import Dict, Type, List
import logging

from flowfile_core.flowfile.node_designer.custom_node import CustomNodeBase, NodeSettings
from shared import storage


logger = logging.getLogger(__name__)


def get_all_custom_nodes() -> Dict[str, Type[CustomNodeBase]]:
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
                    if (inspect.isclass(obj) and
                            issubclass(obj, CustomNodeBase) and
                            obj is not CustomNodeBase):

                        # Use the node_name attribute if it exists, otherwise use class name
                        node_name = getattr(obj, 'node_name', name)
                        custom_nodes[node_name] = obj
                        print(f"Loaded custom node: {node_name} from {file_path.name}")

        except Exception as e:
            print(f"Error loading module from {file_path}: {e}")
            # Continue with other files even if one fails
            continue

    return custom_nodes


def get_all_custom_nodes_with_validation() -> Dict[str, Type[CustomNodeBase]]:
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
                    if (inspect.isclass(obj) and
                            issubclass(obj, CustomNodeBase) and
                            obj is not CustomNodeBase):

                        try:
                            _obj = obj()
                            # Validate that the node has required attributes
                            if not hasattr(_obj, 'node_name'):
                                logger.error(f"Warning: {name} missing node_name attribute")
                                raise ValueError(f"Node {name} must implement a node_name attribute")

                            if not hasattr(_obj, 'settings_schema'):
                                logger.error(f"Warning: {name} missing settings_schema attribute")
                                raise ValueError(f"Node {name} must implement a settings_schema attribute")

                            if not hasattr(_obj, 'process'):
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


def get_custom_nodes_lazy() -> List[Type[CustomNodeBase]]:
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
                    if (inspect.isclass(obj) and
                            issubclass(obj, CustomNodeBase) and
                            obj is not CustomNodeBase and
                            obj.__module__ == module.__name__):  # Only get classes defined in this module
                        nodes.append(obj)

        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            continue

    return nodes


# Example usage function that matches your original pattern
def add_custom_node(node_class: Type[CustomNodeBase], registry: Dict[str, Type[CustomNodeBase]]):
    """Add a single custom node to the registry."""
    if hasattr(node_class, 'node_name'):
        registry[node_class.node_name] = node_class
    else:
        registry[node_class.__name__] = node_class


def get_all_nodes_from_standard_location() -> Dict[str, Type[CustomNodeBase]]:
    """
    Main function to get all custom nodes from the standard location.
    This matches your original function signature.
    """

    return get_all_custom_nodes_with_validation()
