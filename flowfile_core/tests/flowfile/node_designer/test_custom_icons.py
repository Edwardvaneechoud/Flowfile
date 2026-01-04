"""
Tests for custom icon support in user-defined nodes.

This module tests:
- Icon file handling (upload, list, serve, delete)
- Custom node icon attribute parsing
- Icon validation (file types, size limits)
"""
import pytest
import tempfile
import io
from pathlib import Path
from unittest.mock import patch, MagicMock

from fastapi.testclient import TestClient
from fastapi import FastAPI

from flowfile_core.routes.user_defined_components import (
    router,
    IconInfo,
    ALLOWED_ICON_EXTENSIONS,
    MAX_ICON_SIZE,
    _extract_node_info_from_file,
)


@pytest.fixture
def app():
    """Create a test FastAPI application with the router."""
    test_app = FastAPI()
    test_app.include_router(router, prefix="/user_defined_components")
    return test_app


@pytest.fixture
def client(app):
    """Create a test client."""
    return TestClient(app)


@pytest.fixture
def temp_icons_dir():
    """Create a temporary icons directory for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        icons_dir = Path(tmpdir) / "icons"
        icons_dir.mkdir(parents=True, exist_ok=True)
        yield icons_dir


@pytest.fixture
def mock_storage(temp_icons_dir):
    """Mock the storage module to use temporary directories."""
    mock = MagicMock()
    mock.user_defined_nodes_icons = temp_icons_dir
    mock.user_defined_nodes_directory = temp_icons_dir.parent
    return mock


class TestIconInfo:
    """Tests for the IconInfo model."""

    def test_icon_info_creation(self):
        """Test creating an IconInfo instance."""
        info = IconInfo(file_name="test.png", is_custom=True)
        assert info.file_name == "test.png"
        assert info.is_custom is True

    def test_icon_info_defaults(self):
        """Test IconInfo default values."""
        info = IconInfo(file_name="test.png")
        assert info.is_custom is True  # Default value


class TestAllowedIconExtensions:
    """Tests for icon extension validation."""

    def test_png_allowed(self):
        """PNG files should be allowed."""
        assert '.png' in ALLOWED_ICON_EXTENSIONS

    def test_jpg_allowed(self):
        """JPG files should be allowed."""
        assert '.jpg' in ALLOWED_ICON_EXTENSIONS
        assert '.jpeg' in ALLOWED_ICON_EXTENSIONS

    def test_svg_allowed(self):
        """SVG files should be allowed."""
        assert '.svg' in ALLOWED_ICON_EXTENSIONS

    def test_gif_allowed(self):
        """GIF files should be allowed."""
        assert '.gif' in ALLOWED_ICON_EXTENSIONS

    def test_webp_allowed(self):
        """WebP files should be allowed."""
        assert '.webp' in ALLOWED_ICON_EXTENSIONS

    def test_exe_not_allowed(self):
        """Executable files should not be allowed."""
        assert '.exe' not in ALLOWED_ICON_EXTENSIONS

    def test_py_not_allowed(self):
        """Python files should not be allowed."""
        assert '.py' not in ALLOWED_ICON_EXTENSIONS


class TestMaxIconSize:
    """Tests for icon size limits."""

    def test_max_size_is_5mb(self):
        """Maximum icon size should be 5MB."""
        assert MAX_ICON_SIZE == 5 * 1024 * 1024


class TestListIcons:
    """Tests for the list-icons endpoint."""

    def test_list_icons_empty_directory(self, client, mock_storage):
        """Test listing icons when directory is empty."""
        with patch('flowfile_core.routes.user_defined_components.storage', mock_storage):
            response = client.get("/user_defined_components/list-icons")
            assert response.status_code == 200
            assert response.json() == []

    def test_list_icons_with_files(self, client, mock_storage, temp_icons_dir):
        """Test listing icons when files exist."""
        # Create some test icon files
        (temp_icons_dir / "icon1.png").write_bytes(b"fake png")
        (temp_icons_dir / "icon2.svg").write_bytes(b"fake svg")
        (temp_icons_dir / "not_an_icon.txt").write_text("not an icon")

        with patch('flowfile_core.routes.user_defined_components.storage', mock_storage):
            response = client.get("/user_defined_components/list-icons")
            assert response.status_code == 200
            icons = response.json()

            # Should only include icon files, not .txt
            file_names = [icon["file_name"] for icon in icons]
            assert "icon1.png" in file_names
            assert "icon2.svg" in file_names
            assert "not_an_icon.txt" not in file_names

    def test_list_icons_sorted(self, client, mock_storage, temp_icons_dir):
        """Test that icons are sorted alphabetically."""
        # Create files in non-alphabetical order
        (temp_icons_dir / "zebra.png").write_bytes(b"fake")
        (temp_icons_dir / "alpha.png").write_bytes(b"fake")
        (temp_icons_dir / "beta.png").write_bytes(b"fake")

        with patch('flowfile_core.routes.user_defined_components.storage', mock_storage):
            response = client.get("/user_defined_components/list-icons")
            icons = response.json()
            file_names = [icon["file_name"] for icon in icons]

            assert file_names == ["alpha.png", "beta.png", "zebra.png"]


class TestUploadIcon:
    """Tests for the upload-icon endpoint."""

    def test_upload_valid_png(self, client, mock_storage, temp_icons_dir):
        """Test uploading a valid PNG file."""
        file_content = b"\x89PNG\r\n\x1a\n" + b"fake png content"

        with patch('flowfile_core.routes.user_defined_components.storage', mock_storage):
            response = client.post(
                "/user_defined_components/upload-icon",
                files={"file": ("test.png", io.BytesIO(file_content), "image/png")}
            )

            assert response.status_code == 200
            data = response.json()
            assert data["success"] is True
            assert data["file_name"] == "test.png"

            # Verify file was created
            assert (temp_icons_dir / "test.png").exists()

    def test_upload_valid_svg(self, client, mock_storage, temp_icons_dir):
        """Test uploading a valid SVG file."""
        svg_content = b'<svg xmlns="http://www.w3.org/2000/svg"></svg>'

        with patch('flowfile_core.routes.user_defined_components.storage', mock_storage):
            response = client.post(
                "/user_defined_components/upload-icon",
                files={"file": ("icon.svg", io.BytesIO(svg_content), "image/svg+xml")}
            )

            assert response.status_code == 200
            assert response.json()["file_name"] == "icon.svg"

    def test_upload_invalid_extension(self, client, mock_storage):
        """Test uploading a file with invalid extension."""
        with patch('flowfile_core.routes.user_defined_components.storage', mock_storage):
            response = client.post(
                "/user_defined_components/upload-icon",
                files={"file": ("malicious.exe", io.BytesIO(b"bad"), "application/octet-stream")}
            )

            assert response.status_code == 400
            assert "Invalid file type" in response.json()["detail"]

    def test_upload_file_too_large(self, client, mock_storage):
        """Test uploading a file that exceeds size limit."""
        large_content = b"x" * (MAX_ICON_SIZE + 1)

        with patch('flowfile_core.routes.user_defined_components.storage', mock_storage):
            response = client.post(
                "/user_defined_components/upload-icon",
                files={"file": ("large.png", io.BytesIO(large_content), "image/png")}
            )

            assert response.status_code == 400
            assert "too large" in response.json()["detail"]

    def test_upload_sanitizes_filename(self, client, mock_storage, temp_icons_dir):
        """Test that filenames are sanitized."""
        with patch('flowfile_core.routes.user_defined_components.storage', mock_storage):
            response = client.post(
                "/user_defined_components/upload-icon",
                files={"file": ("my icon (1).png", io.BytesIO(b"fake"), "image/png")}
            )

            assert response.status_code == 200
            # Spaces and parentheses should be replaced
            safe_name = response.json()["file_name"]
            assert " " not in safe_name
            assert "(" not in safe_name
            assert ")" not in safe_name

    def test_upload_preserves_hyphens(self, client, mock_storage, temp_icons_dir):
        """Test that hyphens in filenames are preserved."""
        with patch('flowfile_core.routes.user_defined_components.storage', mock_storage):
            response = client.post(
                "/user_defined_components/upload-icon",
                files={"file": ("ruler-plus.svg", io.BytesIO(b"svg"), "image/svg+xml")}
            )

            assert response.status_code == 200
            assert response.json()["file_name"] == "ruler-plus.svg"


class TestGetIcon:
    """Tests for the get icon endpoint."""

    def test_get_existing_icon(self, client, mock_storage, temp_icons_dir):
        """Test retrieving an existing icon."""
        icon_content = b"fake png content"
        (temp_icons_dir / "test.png").write_bytes(icon_content)

        with patch('flowfile_core.routes.user_defined_components.storage', mock_storage):
            response = client.get("/user_defined_components/icon/test.png")

            assert response.status_code == 200
            assert response.content == icon_content

    def test_get_nonexistent_icon(self, client, mock_storage):
        """Test retrieving a non-existent icon returns 404."""
        with patch('flowfile_core.routes.user_defined_components.storage', mock_storage):
            response = client.get("/user_defined_components/icon/nonexistent.png")

            assert response.status_code == 404

    def test_get_icon_with_hyphen(self, client, mock_storage, temp_icons_dir):
        """Test retrieving an icon with hyphens in the name."""
        icon_content = b"svg content"
        (temp_icons_dir / "ruler-plus.svg").write_bytes(icon_content)

        with patch('flowfile_core.routes.user_defined_components.storage', mock_storage):
            response = client.get("/user_defined_components/icon/ruler-plus.svg")

            assert response.status_code == 200
            assert response.content == icon_content

    def test_get_icon_content_type_png(self, client, mock_storage, temp_icons_dir):
        """Test that PNG files are served with correct content type."""
        (temp_icons_dir / "test.png").write_bytes(b"png")

        with patch('flowfile_core.routes.user_defined_components.storage', mock_storage):
            response = client.get("/user_defined_components/icon/test.png")

            assert response.headers["content-type"] == "image/png"

    def test_get_icon_content_type_svg(self, client, mock_storage, temp_icons_dir):
        """Test that SVG files are served with correct content type."""
        (temp_icons_dir / "test.svg").write_bytes(b"svg")

        with patch('flowfile_core.routes.user_defined_components.storage', mock_storage):
            response = client.get("/user_defined_components/icon/test.svg")

            assert response.headers["content-type"] == "image/svg+xml"


class TestDeleteIcon:
    """Tests for the delete-icon endpoint."""

    def test_delete_existing_icon(self, client, mock_storage, temp_icons_dir):
        """Test deleting an existing icon."""
        (temp_icons_dir / "to_delete.png").write_bytes(b"delete me")

        with patch('flowfile_core.routes.user_defined_components.storage', mock_storage):
            response = client.delete("/user_defined_components/delete-icon/to_delete.png")

            assert response.status_code == 200
            assert response.json()["success"] is True
            assert not (temp_icons_dir / "to_delete.png").exists()

    def test_delete_nonexistent_icon(self, client, mock_storage):
        """Test deleting a non-existent icon returns 404."""
        with patch('flowfile_core.routes.user_defined_components.storage', mock_storage):
            response = client.delete("/user_defined_components/delete-icon/nonexistent.png")

            assert response.status_code == 404


class TestExtractNodeIconFromFile:
    """Tests for extracting node_icon from Python files."""

    @pytest.fixture
    def temp_node_file_with_icon(self):
        """Create a temporary node file with a custom icon."""
        node_code = '''
import polars as pl
from flowfile_core.flowfile.node_designer import CustomNodeBase, NodeSettings

class MyCustomNode(CustomNodeBase):
    node_name: str = "My Custom Node"
    node_category: str = "Custom"
    node_icon: str = "my-custom-icon.png"
    title: str = "My Custom Node"
    intro: str = "A custom node with a custom icon"

    def process(self, *inputs):
        return inputs[0]
'''
        with tempfile.NamedTemporaryFile(
            mode='w',
            suffix='.py',
            delete=False,
            prefix='node_with_icon_'
        ) as f:
            f.write(node_code)
            f.flush()
            yield Path(f.name)

        Path(f.name).unlink(missing_ok=True)

    @pytest.fixture
    def temp_node_file_without_icon(self):
        """Create a temporary node file without a custom icon."""
        node_code = '''
import polars as pl
from flowfile_core.flowfile.node_designer import CustomNodeBase, NodeSettings

class MyCustomNode(CustomNodeBase):
    node_name: str = "My Custom Node"
    node_category: str = "Custom"
    title: str = "My Custom Node"
    intro: str = "A custom node without a custom icon"

    def process(self, *inputs):
        return inputs[0]
'''
        with tempfile.NamedTemporaryFile(
            mode='w',
            suffix='.py',
            delete=False,
            prefix='node_without_icon_'
        ) as f:
            f.write(node_code)
            f.flush()
            yield Path(f.name)

        Path(f.name).unlink(missing_ok=True)

    def test_extract_node_icon_present(self, temp_node_file_with_icon):
        """Test extracting node_icon when it's present in the file."""
        info = _extract_node_info_from_file(temp_node_file_with_icon)

        assert info.node_name == "My Custom Node"
        assert info.node_icon == "my-custom-icon.png"

    def test_extract_node_icon_default(self, temp_node_file_without_icon):
        """Test that default icon is used when node_icon is not specified."""
        info = _extract_node_info_from_file(temp_node_file_without_icon)

        assert info.node_name == "My Custom Node"
        assert info.node_icon == "user-defined-icon.png"  # Default value


class TestCustomNodeBaseIcon:
    """Tests for icon handling in CustomNodeBase."""

    def test_custom_node_default_icon(self):
        """Test that CustomNodeBase has default icon."""
        from flowfile_core.flowfile.node_designer import CustomNodeBase, NodeSettings

        class TestNode(CustomNodeBase):
            node_name: str = "Test Node"

            def process(self, *inputs):
                return inputs[0]

        node = TestNode()
        assert node.node_icon == "user-defined-icon.png"

    def test_custom_node_custom_icon(self):
        """Test that CustomNodeBase can have a custom icon."""
        from flowfile_core.flowfile.node_designer import CustomNodeBase, NodeSettings

        class TestNode(CustomNodeBase):
            node_name: str = "Test Node"
            node_icon: str = "my-custom-icon.svg"

            def process(self, *inputs):
                return inputs[0]

        node = TestNode()
        assert node.node_icon == "my-custom-icon.svg"

    def test_node_template_includes_icon(self):
        """Test that to_node_template includes the icon."""
        from flowfile_core.flowfile.node_designer import CustomNodeBase

        class TestNode(CustomNodeBase):
            node_name: str = "Test Node"
            node_icon: str = "test-icon.png"

            def process(self, *inputs):
                return inputs[0]

        node = TestNode()
        template = node.to_node_template()

        assert template.image == "test-icon.png"

    def test_frontend_schema_includes_icon(self):
        """Test that get_frontend_schema includes the icon."""
        from flowfile_core.flowfile.node_designer import CustomNodeBase

        class TestNode(CustomNodeBase):
            node_name: str = "Test Node"
            node_icon: str = "test-icon.png"

            def process(self, *inputs):
                return inputs[0]

        node = TestNode()
        schema = node.get_frontend_schema()

        assert schema["node_icon"] == "test-icon.png"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
