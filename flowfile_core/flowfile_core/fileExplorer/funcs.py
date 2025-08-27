from pathlib import Path
from typing import List, Optional, Set, Union
from datetime import datetime
from pydantic import BaseModel
from typing_extensions import Literal
import os


class FileInfo(BaseModel):
    """Comprehensive information about a file or directory."""
    name: str
    path: str
    is_directory: bool
    size: int
    file_type: str
    last_modified: datetime
    created_date: datetime
    is_hidden: bool
    exists: bool = True

    @classmethod
    def from_path(cls, path: Path, sandbox_root: Optional[Path] = None,
                  use_relative_paths: bool = False) -> 'FileInfo':
        """Create FileInfo instance from a path.

        Args:
            path: The path to create FileInfo from
            sandbox_root: The root directory for sandboxing (for relative path calculation)
            use_relative_paths: If True, store relative paths; if False, store absolute paths
        """
        try:
            stats = path.stat()

            # Decide whether to use relative or absolute path
            if use_relative_paths and sandbox_root:
                try:
                    display_path = str(path.relative_to(sandbox_root))
                except ValueError:
                    display_path = str(path.absolute())
            else:
                display_path = str(path.absolute())

            return cls(
                name=path.name,
                path=display_path,
                is_directory=path.is_dir(),
                size=stats.st_size,
                file_type=path.suffix[1:] if path.suffix else "",
                last_modified=datetime.fromtimestamp(stats.st_mtime),
                created_date=datetime.fromtimestamp(stats.st_ctime),
                is_hidden=path.name.startswith('.') or (
                        os.name == 'nt' and bool(stats.st_file_attributes & 0x2)
                ),
                exists=True
            )
        except (PermissionError, OSError):
            # Handle error case
            if use_relative_paths and sandbox_root:
                try:
                    display_path = str(path.relative_to(sandbox_root))
                except ValueError:
                    display_path = str(path.absolute())
            else:
                display_path = str(path.absolute())

            return cls(
                name=path.name,
                path=display_path,
                is_directory=False,
                size=0,
                file_type="",
                last_modified=datetime.fromtimestamp(0),
                created_date=datetime.fromtimestamp(0),
                is_hidden=False,
                exists=False
            )


class SecureFileExplorer:
    """File explorer with sandbox enforcement to prevent directory traversal."""

    def __init__(self, start_path: Union[str, Path],
                 sandbox_root: Optional[Union[str, Path]] = None,
                 use_relative_paths: bool = False):
        """Initialize SecureFileExplorer with sandboxing.

        Args:
            start_path: Initial directory to start in
            sandbox_root: Root directory that user cannot escape from.
                         If None, no sandbox enforcement.
            use_relative_paths: If True, FileInfo will contain relative paths;
                               if False (default), contains absolute paths
        """
        self.use_relative_paths = use_relative_paths

        # Set up the sandbox root
        if sandbox_root is not None:
            self.sandbox_root = Path(sandbox_root).expanduser().resolve()
        else:
            self.sandbox_root = None

        # Set initial current path
        initial_path = Path(start_path).expanduser().resolve()

        # If sandbox is set and initial path is outside it, use sandbox root
        if self.sandbox_root and not self._is_path_safe(initial_path):
            self.current_path = self.sandbox_root
        else:
            self.current_path = initial_path

    def _is_path_safe(self, path: Path) -> bool:
        """Check if a path is within the sandbox root.

        Uses resolve() to handle symlinks and relative paths securely.
        Returns True if no sandbox is set (no restrictions).
        """
        if self.sandbox_root is None:
            return True  # No sandbox = no restrictions

        try:
            resolved_path = path.resolve()
            resolved_sandbox = self.sandbox_root.resolve()
            # Check if the resolved path is within sandbox
            resolved_path.relative_to(resolved_sandbox)
            return True
        except (ValueError, RuntimeError):
            return False

    def _sanitize_path(self, path: Union[str, Path]) -> Optional[Path]:
        """Sanitize and validate a path, ensuring it stays within sandbox.

        Returns None if path would escape sandbox.
        """
        try:
            # Handle relative paths from current directory
            if isinstance(path, str):
                # Remove any suspicious patterns
                if '..' in Path(path).parts or path.startswith('/'):
                    # For absolute paths or parent references, resolve from sandbox root
                    test_path = Path(path).expanduser()
                else:
                    # For simple relative paths, resolve from current directory
                    test_path = self.current_path / path
            else:
                test_path = path

            # Resolve to absolute path
            resolved = test_path.resolve()

            # Check if within sandbox
            if self._is_path_safe(resolved):
                return resolved
            else:
                return None
        except (ValueError, RuntimeError, OSError):
            return None

    @property
    def current_directory(self) -> str:
        """Get the current directory path relative to sandbox root."""
        if self.sandbox_root is None:
            return str(self.current_path)

        try:
            relative = self.current_path.relative_to(self.sandbox_root)
            return str(relative) if str(relative) != "." else "/"
        except ValueError:
            return "/"

    @property
    def parent_directory(self) -> Optional[str]:
        """Get the parent directory path if it exists and is within sandbox."""
        parent = self.current_path.parent
        if self._is_path_safe(parent) and parent != self.current_path:
            try:
                relative = parent.relative_to(self.sandbox_root)
                return str(relative) if str(relative) != "." else "/"
            except ValueError:
                return None
        return None

    def list_contents(
            self,
            *,
            show_hidden: bool = False,
            file_types: Optional[List[str]] = None,
            recursive: bool = False,
            min_size: Optional[int] = None,
            max_size: Optional[int] = None,
            sort_by: Literal['name', 'date', 'size', 'type'] = 'name',
            reverse: bool = False,
            exclude_patterns: Optional[List[str]] = None,
            max_depth: int = 5  # Add depth limit for recursive operations
    ) -> List[FileInfo]:
        """List contents with security-conscious filtering."""
        contents: List[FileInfo] = []
        excluded_paths: Set[str] = set()

        if exclude_patterns:
            for pattern in exclude_patterns:
                # Ensure patterns don't escape sandbox
                safe_pattern = pattern.replace('../', '').replace('..\\', '')
                excluded_paths.update(str(p) for p in self.current_path.glob(safe_pattern))

        def should_include(info: FileInfo) -> bool:
            """Determine if a file should be included based on filters."""
            full_path = self.current_path / info.path
            if str(full_path) in excluded_paths:
                return False
            if not show_hidden and info.is_hidden:
                return False
            if min_size is not None and info.size < min_size:
                return False
            if max_size is not None and info.size > max_size:
                return False
            if file_types and not info.is_directory:
                return info.file_type.lower() in (t.lower() for t in file_types)
            return True

        try:
            if recursive:
                # Use iterative approach with depth limit for safety
                dirs_to_process = [(self.current_path, 0)]
                processed = set()

                while dirs_to_process:
                    current_dir, depth = dirs_to_process.pop(0)

                    # Skip if already processed or exceeds depth
                    if current_dir in processed or depth > max_depth:
                        continue

                    processed.add(current_dir)

                    try:
                        for item in current_dir.iterdir():
                            # Security check for each item
                            if not self._is_path_safe(item):
                                continue

                            try:
                                file_info = FileInfo.from_path(item, self.sandbox_root,
                                                               self.use_relative_paths)
                                if should_include(file_info):
                                    contents.append(file_info)

                                if item.is_dir() and depth < max_depth:
                                    dirs_to_process.append((item, depth + 1))
                            except (PermissionError, OSError):
                                continue
                    except (PermissionError, OSError):
                        continue
            else:
                # Non-recursive listing
                for item in self.current_path.iterdir():
                    # Security check
                    if not self._is_path_safe(item):
                        continue

                    try:
                        file_info = FileInfo.from_path(item, self.sandbox_root,
                                                       self.use_relative_paths)
                        if should_include(file_info):
                            contents.append(file_info)
                    except (PermissionError, OSError):
                        continue

        except PermissionError:
            raise PermissionError(f"Permission denied to access directory: {self.current_directory}")

        # Sort results
        sort_key = {
            'name': lambda x: (not x.is_directory, x.name.lower()),
            'date': lambda x: (not x.is_directory, x.last_modified),
            'size': lambda x: (not x.is_directory, x.size),
            'type': lambda x: (not x.is_directory, x.file_type.lower(), x.name.lower())
        }[sort_by]

        return sorted(contents, key=sort_key, reverse=reverse)

    def navigate_to(self, path: str) -> bool:
        """Navigate to a new directory path within sandbox."""
        sanitized = self._sanitize_path(path)

        if sanitized is None:
            return False

        if not sanitized.exists() or not sanitized.is_dir():
            return False

        try:
            # Test if we can actually read the directory
            next(sanitized.iterdir(), None)
            self.current_path = sanitized
            return True
        except (PermissionError, OSError):
            # Still navigate if we have permission issues (user will see empty dir)
            self.current_path = sanitized
            return True

    def navigate_up(self) -> bool:
        """Navigate up to the parent directory, respecting sandbox."""
        parent = self.current_path.parent

        # Check if parent is within sandbox
        if not self._is_path_safe(parent):
            return False

        # Don't navigate if we're already at sandbox root
        if parent == self.current_path:
            return False

        self.current_path = parent
        return True

    def navigate_into(self, directory_name: str) -> bool:
        """Navigate into a subdirectory, with path sanitization."""
        # Sanitize directory name
        if '/' in directory_name or '\\' in directory_name or '..' in directory_name:
            return False

        new_path = self.current_path / directory_name
        return self.navigate_to(str(new_path))

    def get_absolute_path(self, relative_path: str) -> Optional[Path]:
        """Get absolute path for a file within sandbox.

        Returns None if the path would escape sandbox.
        """
        sanitized = self._sanitize_path(relative_path)
        return sanitized if sanitized else None


def get_files_from_directory(
        dir_name: Union[str, Path],
        types: Optional[List[str]] = None,
        *,
        include_hidden: bool = False,
        recursive: bool = False,
        sandbox_root: Optional[Union[str, Path]] = None
) -> Optional[List[FileInfo]]:
    """
    Get list of files from a directory with sandbox enforcement.

    Args:
        dir_name: Directory path to scan
        types: List of file extensions to include
        include_hidden: Whether to include hidden files
        recursive: Whether to scan subdirectories
        sandbox_root: Root directory to enforce as sandbox boundary

    Returns:
        List of FileInfo objects or None if directory doesn't exist or is outside sandbox
    """
    try:
        # Create a secure explorer with sandbox
        if sandbox_root:
            explorer = SecureFileExplorer(start_path=dir_name, sandbox_root=sandbox_root)
        else:
            explorer = SecureFileExplorer(start_path=dir_name)

        # Use the explorer's list_contents method
        return explorer.list_contents(
            show_hidden=include_hidden,
            file_types=types,
            recursive=recursive
        )

    except (ValueError, PermissionError) as e:
        # Return None for invalid/inaccessible directories
        return None
    except Exception as e:
        raise type(e)(f"Error scanning directory {dir_name}: {str(e)}") from e


# Alias for backward compatibility
FileExplorer = SecureFileExplorer