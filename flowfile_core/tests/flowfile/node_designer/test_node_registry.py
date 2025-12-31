"""
Tests for the node registry functions that handle loading, registering,
and removing custom nodes dynamically.
"""
import pytest
import tempfile
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

from flowfile_core.configs.node_store.user_defined_node_registry import (
    load_single_node_from_file,
    unload_node_by_name,
)


@pytest.fixture
def temp_node_file():
    """Creates a temporary valid custom node file for testing."""
    node_code = '''
import polars as pl
from flowfile_core.flowfile.node_designer import (
    CustomNodeBase,
    NodeSettings,
    Section,
    TextInput,
)


main_section = Section(
    title="Configuration",
    my_input=TextInput(label="Input Value", default="test"),
)


class TestNodeSettings(NodeSettings):
    main_section: Section = main_section


class TestCustomNode(CustomNodeBase):
    node_name: str = "Test Custom Node"
    node_category: str = "Testing"
    title: str = "Test Custom Node"
    intro: str = "A test custom node"
    number_of_inputs: int = 1
    number_of_outputs: int = 1
    settings_schema: TestNodeSettings = TestNodeSettings()

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0]
'''
    with tempfile.NamedTemporaryFile(
        mode='w',
        suffix='.py',
        delete=False,
        prefix='test_node_'
    ) as f:
        f.write(node_code)
        f.flush()
        yield Path(f.name)

    # Cleanup
    Path(f.name).unlink(missing_ok=True)


@pytest.fixture
def temp_invalid_node_file():
    """Creates a temporary invalid node file (missing required attributes)."""
    node_code = '''
import polars as pl
from flowfile_core.flowfile.node_designer import CustomNodeBase


class InvalidNode(CustomNodeBase):
    # Missing node_name, settings_schema, and process method
    pass
'''
    with tempfile.NamedTemporaryFile(
        mode='w',
        suffix='.py',
        delete=False,
        prefix='invalid_node_'
    ) as f:
        f.write(node_code)
        f.flush()
        yield Path(f.name)

    # Cleanup
    Path(f.name).unlink(missing_ok=True)


@pytest.fixture
def temp_syntax_error_file():
    """Creates a temporary file with Python syntax errors."""
    bad_code = '''
def broken_function(
    # Missing closing parenthesis and body
'''
    with tempfile.NamedTemporaryFile(
        mode='w',
        suffix='.py',
        delete=False,
        prefix='syntax_error_'
    ) as f:
        f.write(bad_code)
        f.flush()
        yield Path(f.name)

    # Cleanup
    Path(f.name).unlink(missing_ok=True)


class TestLoadSingleNodeFromFile:
    """Tests for the load_single_node_from_file function."""

    def test_load_valid_node(self, temp_node_file):
        """Test loading a valid custom node file."""
        node_class = load_single_node_from_file(temp_node_file)

        assert node_class is not None
        instance = node_class()
        assert instance.node_name == "Test Custom Node"
        assert instance.node_category == "Testing"
        assert hasattr(instance, 'settings_schema')
        assert hasattr(instance, 'process')

    def test_load_nonexistent_file(self):
        """Test loading a file that doesn't exist returns None."""
        fake_path = Path("/nonexistent/path/to/node.py")
        result = load_single_node_from_file(fake_path)
        assert result is None

    def test_load_invalid_node_raises_error(self, temp_invalid_node_file):
        """Test loading an invalid node raises ValueError."""
        with pytest.raises(ValueError):
            load_single_node_from_file(temp_invalid_node_file)

    def test_load_syntax_error_raises_error(self, temp_syntax_error_file):
        """Test loading a file with syntax errors raises SyntaxError."""
        with pytest.raises(SyntaxError):
            load_single_node_from_file(temp_syntax_error_file)


class TestUnloadNodeByName:
    """Tests for the unload_node_by_name function."""

    def test_unload_removes_module_from_cache(self):
        """Test that unloading removes related modules from sys.modules."""
        # Add some fake modules to sys.modules
        sys.modules['test_node_module'] = MagicMock()
        sys.modules['custom_node_test_node_module_123'] = MagicMock()

        # Verify they exist
        assert 'test_node_module' in sys.modules

        # Unload
        result = unload_node_by_name('Test Node Module')

        # Verify removed
        assert 'test_node_module' not in sys.modules
        assert 'custom_node_test_node_module_123' not in sys.modules
        assert result is True

    def test_unload_nonexistent_module_returns_false(self):
        """Test that unloading a non-existent module returns False."""
        result = unload_node_by_name('nonexistent_module_xyz')
        assert result is False


class TestNodeRegistryIntegration:
    """Integration tests for the node registry workflow."""

    def test_load_and_use_custom_node(self, temp_node_file):
        """Test the full workflow of loading and using a custom node."""
        import polars as pl

        # Load the node
        node_class = load_single_node_from_file(temp_node_file)
        assert node_class is not None

        # Create an instance
        node = node_class()

        # Verify it can process data
        input_df = pl.DataFrame({"a": [1, 2, 3]}).lazy()
        result = node.process(input_df)

        assert result is not None

    def test_reload_modified_node(self, temp_node_file):
        """Test that a node can be reloaded after modification."""
        # First load
        node_class_1 = load_single_node_from_file(temp_node_file)
        assert node_class_1 is not None
        assert node_class_1().node_name == "Test Custom Node"

        # Modify the file
        new_code = temp_node_file.read_text().replace(
            'node_name: str = "Test Custom Node"',
            'node_name: str = "Modified Node Name"'
        )
        temp_node_file.write_text(new_code)

        # Reload - should get the modified version
        node_class_2 = load_single_node_from_file(temp_node_file)
        assert node_class_2 is not None
        assert node_class_2().node_name == "Modified Node Name"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
