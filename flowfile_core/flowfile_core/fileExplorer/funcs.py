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
    def from_path(cls, path: Path) -> 'FileInfo':
        """Create FileInfo instance from a path."""
        try:
            stats = path.stat()
            return cls(
                name=path.name,
                path=str(path.absolute()),
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
            return cls(
                name=path.name,
                path=str(path.absolute()),
                is_directory=False,
                size=0,
                file_type="",
                last_modified=datetime.fromtimestamp(0),
                created_date=datetime.fromtimestamp(0),
                is_hidden=False,
                exists=False
            )


class FileExplorer:
    def __init__(self, start_path: Optional[str|Path] = None):
        """Initialize FileExplorer with user's home directory or specified path."""
        if start_path is None:
            self.current_path = Path.home()
        else:
            self.current_path = Path(start_path).expanduser().resolve()

        if not self.current_path.exists():
            raise ValueError(f"Path does not exist: {self.current_path}")

        if not self.current_path.is_dir():
            raise ValueError(f"Path is not a directory: {self.current_path}")

    @property
    def current_directory(self) -> str:
        """Get the current directory path."""
        return str(self.current_path.absolute())

    @property
    def parent_directory(self) -> Optional[str]:
        """Get the parent directory path if it exists."""
        parent = self.current_path.parent
        return str(parent.absolute()) if parent != self.current_path else None

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
            exclude_patterns: Optional[List[str]] = None
    ) -> List[FileInfo]:
        """
        List contents of the current directory with advanced filtering and sorting.

        Args:
            show_hidden: Whether to show hidden files and directories
            file_types: List of file extensions to include (without dots)
            recursive: Whether to scan subdirectories
            min_size: Minimum file size in bytes
            max_size: Maximum file size in bytes
            sort_by: Field to sort results by
            reverse: Whether to reverse sort order
            exclude_patterns: Glob patterns to exclude

        Returns:
            List of FileInfo objects sorted according to parameters
        """
        contents: List[FileInfo] = []
        excluded_paths: Set[str] = set()

        if exclude_patterns:
            for pattern in exclude_patterns:
                excluded_paths.update(str(p) for p in self.current_path.glob(pattern))

        def should_include(info: FileInfo) -> bool:
            """Determine if a file should be included based on filters."""
            if str(info.path) in excluded_paths:
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
            # Define the scan pattern based on recursion
            pattern = '**/*' if recursive else '*'

            # Scan directory
            for item in self.current_path.glob(pattern):
                try:
                    file_info = FileInfo.from_path(item)
                    if should_include(file_info):
                        contents.append(file_info)
                except (PermissionError, OSError):
                    continue

        except PermissionError:
            raise PermissionError(f"Permission denied to access directory: {self.current_path}")

        # Sort results
        sort_key = {
            'name': lambda x: (not x.is_directory, x.name.lower()),
            'date': lambda x: (not x.is_directory, x.last_modified),
            'size': lambda x: (not x.is_directory, x.size),
            'type': lambda x: (not x.is_directory, x.file_type.lower(), x.name.lower())
        }[sort_by]

        return sorted(contents, key=sort_key, reverse=reverse)

    def navigate_to(self, path: str) -> bool:
        """
        Navigate to a new directory path.
        Returns True if navigation was successful, False otherwise.
        """
        new_path = None
        try:
            new_path = Path(path).expanduser().resolve()

            if not new_path.exists() or not new_path.is_dir():
                return False

            # Test if we can actually read the directory
            next(new_path.iterdir(), None)
            self.current_path = new_path
            return True

        except PermissionError:
            if new_path:
                self.current_path = new_path
            return True
        except OSError:
            return False

    def navigate_up(self) -> bool:
        """
        Navigate up to the parent directory.
        Returns True if navigation was successful, False otherwise.
        """
        parent = self.parent_directory
        if parent is None:
            return False
        return self.navigate_to(parent)

    def navigate_into(self, directory_name: str) -> bool:
        """
        Navigate into a subdirectory of the current directory.
        Returns True if navigation was successful, False otherwise.
        """
        new_path = self.current_path / directory_name
        return self.navigate_to(str(new_path))


def get_files_from_directory(
        dir_name: Union[str, Path],
        types: Optional[List[str]] = None,
        *,
        include_hidden: bool = False,
        recursive: bool = False
) -> Optional[List[FileInfo]]:
    """
    Get list of files from a directory with optional type filtering.

    Args:
        dir_name: Directory path to scan
        types: List of file extensions to include (without dots). None means all types
        include_hidden: Whether to include hidden files
        recursive: Whether to scan subdirectories

    Returns:
        List of FileInfo objects or None if directory doesn't exist

    Example:
        >>> files = get_files_from_directory("/path/to/dir", types=["pdf", "txt"])
        >>> for file in files:
        ...     print(f"{file.name} - {file.size} bytes")
    """
    try:
        dir_path = Path(dir_name).resolve()
        if not dir_path.exists():
            return None
        if not dir_path.is_dir():
            raise ValueError(f"Path is not a directory: {dir_path}")

        # Normalize file types
        if types:
            types = [t.lower().lstrip('.') for t in types]

        files = []
        pattern = '**/*' if recursive else '*'

        for item in dir_path.glob(pattern):
            try:
                # Skip hidden files unless specifically requested
                if not include_hidden and item.name.startswith('.'):
                    continue

                # Skip directories unless recursive is True
                if item.is_dir() and not recursive:
                    continue

                # Check file type if types are specified
                if types and not item.is_dir():
                    if item.suffix[1:].lower() not in types:
                        continue

                file_info = FileInfo.from_path(item)
                files.append(file_info)

            except (PermissionError, OSError):
                continue

        return sorted(files, key=lambda x: (not x.is_directory, x.name.lower()))

    except Exception as e:
        raise type(e)(f"Error scanning directory {dir_name}: {str(e)}") from e

