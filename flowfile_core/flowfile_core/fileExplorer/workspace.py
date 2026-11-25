"""A person's own folder on a multi-user server, and the folders their file browser and their flows may use.

On a server (``is_server_mode``) each person has ``<user data>/workspaces/<user id>/``: the root of their file
browser, where they save flows and keep files. Beside it is the shared folder: the user data folder, without
``workspaces/``, without the folders the app keeps for itself (``_app_folders``), and without the server's own
storage, working and secure-storage folders where they lie inside it. A server whose user data folder is the home of
the account it runs as has no shared folder (``has_shared_folder``): a person there has their workspace and the
flows, uploads and outputs folders. A desktop and a package install have no workspace: their routes keep the checks
they always had, and the functions that resolve a person's places are asked on a server only.

What a person may use depends on what for (``Places``). The file browser shows their workspace, the shared folder,
the flows, uploads and outputs folders and their own project folder. A flow is saved in and opened from the same
folders and the server's flow folders. A file node reads and writes the workspace, the shared folder without the
flows folder, and the uploads and outputs folders; it also reads the data of the flow templates.

The user id names the folder because a person can neither choose nor change it, and it is a safe path segment. When
an account is deleted its folder moves to ``workspaces/.deleted/<user id>-<UTC time>/`` (``retire_workspace``),
which no account's id names and no browser root holds.

Folders are compared by their resolved text and, for the folders left out, by file identity as well, so a spelling
in other letter case on a disk that ignores case names the same folder.
"""

import glob
import os
import shutil
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from fastapi import HTTPException

from flowfile_core.fileExplorer.funcs import LeftOut, SecureFileExplorer, validate_path_under_cwd
from shared.cloud_storage.uri import is_cloud_uri
from shared.path_utils import contained_path, is_glob_pattern, is_url
from shared.storage_config import _is_docker_mode, flow_storage_directories, is_server_mode, storage

WORKSPACES_FOLDER = "workspaces"
DELETED_FOLDER = ".deleted"
WORKSPACE = "Workspace"
SHARED = "Shared"

Use = Literal["browse", "flow", "read", "write"]
"""What a path is wanted for: the file browser, a flow's own file, or a file a node reads or writes."""


@dataclass(frozen=True)
class Root:
    """A top folder of the file browser: its name as the browser shows it, and where it is."""

    name: str
    path: Path


class NotYours(PermissionError):
    """A path a flow or a person may not use here; ``shown`` is the sentence they read."""

    def __init__(self, shown: str):
        super().__init__(shown)
        self.shown = shown


def workspaces_root() -> Path:
    """The folder that holds every person's workspace."""
    return storage.user_data_directory / WORKSPACES_FOLDER


def workspace_of(user_id: int) -> Path:
    """``user_id``'s own folder, made the first time it is asked for."""
    folder = workspaces_root() / str(int(user_id))
    folder.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(folder, 0o700)
    except OSError:
        pass
    return folder


def retire_workspace(user_id: int) -> Path | None:
    """Move a deleted account's folder to ``workspaces/.deleted/<user id>-<UTC time>/``; where it went, if anywhere.

    A later account given the same id then starts with an empty folder, and the files wait there for whoever hands
    them over. Off a server there are no workspaces and nothing is moved.
    """
    if not is_server_mode():
        return None
    folder = workspaces_root() / str(int(user_id))
    if not folder.is_dir():
        return None
    aside = workspaces_root() / DELETED_FOLDER
    aside.mkdir(parents=True, exist_ok=True)
    target = aside / f"{int(user_id)}-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    shutil.move(str(folder), str(target))
    return target


@contextmanager
def retiring(user_id: int) -> Iterator[None]:
    """Set ``user_id``'s folder aside (``retire_workspace``) for the block that removes their account.

    The folder is back in its place when the block fails, so an account and its folder stay or go together; when
    the folder cannot be moved the block does not run.
    """
    aside = retire_workspace(user_id)
    try:
        yield
    except BaseException:
        if aside is not None:
            folder = workspaces_root() / str(int(user_id))
            if folder.is_dir() and not any(folder.iterdir()):
                folder.rmdir()
            shutil.move(str(aside), str(folder))
        raise


def shared_root() -> Path:
    """The shared folder: the user data folder."""
    return storage.user_data_directory


def has_shared_folder() -> bool:
    """Whether this server has a shared folder: its user data folder is a folder of its own.

    The docker image keeps user data in one. A server run by hand keeps it in the home of the account it runs as,
    which is that account's and not a folder to share; it has a shared folder only where user data is a folder
    that does not hold that home.
    """
    if _is_docker_mode():
        return True
    try:
        home = Path.home()
    except RuntimeError:
        return False
    return contained_path(shared_root(), home) is None


def _secure_storage_folder() -> Path:
    return Path(os.environ.get("SECURE_STORAGE_PATH", "/tmp/.flowfile"))


def _app_folders() -> list[Path]:
    """The folders the app keeps per person, under catalog grants or for every flow inside the user data folder."""
    return [
        storage.ai_sessions_directory,
        storage.catalog_tables_directory,
        storage.catalog_virtual_results_directory,
        storage.notebooks_directory,
        storage.user_defined_nodes_directory,
        storage.user_data_directory / "projects",
    ]


def shared_excluded() -> list[Path]:
    """The folders inside the shared folder that nobody reaches through it.

    Everyone's workspaces, the app's own folders (``_app_folders``), and each of the server's storage, working and
    secure-storage folders that lies strictly inside the shared folder.
    """
    root = os.path.realpath(shared_root())
    excluded = [Path(os.path.realpath(workspaces_root()))]
    for folder in (*_app_folders(), storage.base_directory, Path(os.getcwd()), _secure_storage_folder()):
        real = os.path.realpath(folder)
        if real != root and contained_path(root, real) is not None:
            excluded.append(Path(real))
    return excluded


def _template_data_folders() -> list[Path]:
    """Where the data of the flow templates is kept: a flow made from a template reads it."""
    from flowfile_core.templates.data_downloader import template_data_folders

    return template_data_folders()


def _own_projects_folder(user_id: int) -> Path:
    return storage.user_data_directory / "projects" / str(int(user_id))


class Places:
    """The folders one person may use for one purpose on a server, worked out once and asked many times.

    ``roots`` are the folders, resolved. In the shared folder, when it is one of them, the folders of
    ``shared_excluded`` are left out, and for a file node the flows folder too. Paths are compared resolved, so
    ``..``, a symlink and a relative path lead nowhere else.
    """

    def __init__(self, user_id: int | None, use: Use):
        own = [workspace_of(user_id)] if user_id is not None else []
        shared = [shared_root()] if has_shared_folder() else []
        folders = [*own, *shared, storage.uploads_directory, storage.outputs_directory]
        left_out = shared_excluded() if shared else []
        if use == "browse":
            folders.append(storage.flows_directory)
            if user_id is not None:
                folders.append(_own_projects_folder(user_id))
        elif use == "flow":
            folders += flow_storage_directories()
        else:
            left_out.append(storage.flows_directory)
            if use == "read":
                folders += _template_data_folders()
        self.shared = os.path.realpath(shared[0]) if shared else None
        self.roots = tuple(dict.fromkeys(os.path.realpath(folder) for folder in folders))
        self._prefixes = {root: root if root.endswith(os.sep) else root + os.sep for root in self.roots}
        self.left_out = LeftOut(self.shared, left_out) if self.shared is not None else None
        self._real_folders: dict[str, str] = {}

    def root_of(self, real: str) -> str | None:
        """The first of the folders that ``real``, a resolved path, lies in."""
        for root in self.roots:
            if real == root:
                inside = root
            elif real.startswith(self._prefixes[root]):
                inside = real
            else:
                continue
            if root != self.shared or not self.left_out.holds(inside):
                return root
        return None

    def holds(self, real: str) -> bool:
        """Whether ``real``, a resolved path, lies in one of the folders."""
        return self.root_of(real) is not None

    def place(self, path: str | os.PathLike) -> str | None:
        """``path`` resolved, when it lies in one of the folders.

        What is answered is the value ``contained_path`` checked, which CodeQL's ``py/path-injection`` takes as a
        barrier, so a route opens this and never the path it was asked for.
        """
        asked = os.path.expanduser(path)
        for root in self.roots:
            inside = contained_path(root, asked)
            if inside is not None and (root != self.shared or not self.left_out.holds(inside)):
                return inside
        return None

    def entry(self, path: str | os.PathLike) -> str | None:
        """Where ``path`` itself is, when that lies in one of the folders: its folder resolved, a link at its end
        not followed. A link counts by the folder it is in. The same CodeQL barrier as :meth:`place`."""
        folder, name = os.path.split(os.path.abspath(os.path.expanduser(path)))
        target = os.path.normpath(os.path.join(os.path.realpath(folder), name))
        for root in self.roots:
            if target == root:
                return root
            if target.startswith(self._prefixes[root]):
                kept_by = os.path.dirname(target) if os.path.islink(target) else target
                if root != self.shared or not self.left_out.holds(kept_by):
                    return target
        return None

    def holds_match(self, match: str) -> bool:
        """Whether a file a pattern matched lies in one of the folders; its folder is resolved once per folder."""
        folder, name = os.path.split(match)
        real_folder = self._real_folders.get(folder)
        if real_folder is None:
            real_folder = self._real_folders[folder] = os.path.realpath(folder)
        if os.path.islink(match):
            return self.holds(os.path.realpath(match))
        return self.holds(os.path.join(real_folder, name))

    def holds_all_of(self, folder: str) -> bool:
        """Whether everything in ``folder``, a resolved folder in one of the folders, lies in them too."""
        inside = folder if folder.endswith(os.sep) else folder + os.sep
        if self.left_out is not None and any(
            left.startswith(inside) and os.path.lexists(left) for left in self.left_out.folders
        ):
            return False
        return all(self.holds_match(match) for match in _matches(os.path.join(folder, "**", "*")))


def _flow_folders_outside_shared() -> list[Root]:
    """The flows, uploads and outputs folders, as roots of their own where they are not in the shared folder."""
    roots = [
        Root("Flows", storage.flows_directory),
        Root("Uploads", storage.uploads_directory),
        Root("Outputs", storage.outputs_directory),
    ]
    if not has_shared_folder():
        return roots
    left_out = LeftOut(shared_root(), shared_excluded())
    return [
        root for root in roots if (real := contained_path(shared_root(), root.path)) is None or left_out.holds(real)
    ]


def browse_roots(user_id: int | None) -> list[Root]:
    """The top folders of ``user_id``'s file browser on a server: their workspace, the shared folder where the
    server has one, then the flows, uploads and outputs folders where the shared folder does not hold them.

    Empty off a server, where the browser reaches what it always did.
    """
    if not is_server_mode():
        return []
    roots = [Root(WORKSPACE, workspace_of(user_id))] if user_id is not None else []
    shared = [Root(SHARED, shared_root())] if has_shared_folder() else []
    return [*roots, *shared, *_flow_folders_outside_shared()]


def default_folder(user_id: int | None) -> Path:
    """Where ``user_id``'s file browser opens: their workspace on a server, the user data folder elsewhere."""
    if is_server_mode() and user_id is not None:
        return workspace_of(user_id)
    return storage.user_data_directory


def data_place(path: str, user_id: int | None, write: bool = False) -> str | None:
    """``path`` resolved, when a flow run for ``user_id`` on a server may read it, or with ``write`` write it; else
    ``None``.

    That is the person's workspace, the shared folder without the flows folder, and the uploads and outputs folders
    every flow uses; to read, also the data of the flow templates. ``..``, a symlink and a relative path are
    resolved first, so none of them leads out.
    """
    return Places(user_id, "write" if write else "read").place(path)


def flow_file_place(path: str, user_id: int | None) -> str | None:
    """``path`` resolved, when ``user_id`` may save a flow there or open one from there on a server.

    Their workspace, the shared folder, the uploads and outputs folders, and the server's flow folders
    (``flow_storage_directories``), where the catalog keeps flows and the editor its unnamed ones.
    """
    return Places(user_id, "flow").place(path)


def file_folder(path: str, user_id: int | None) -> str | None:
    """``path`` resolved, when ``user_id`` may browse it on a server: one of their ``browse_roots`` or their own
    project folder."""
    return Places(user_id, "browse").place(path)


def upload_folder(path: str, user_id: int | None) -> str:
    """``path`` resolved, when ``user_id`` may put a file in that folder on a server: their workspace, the shared
    folder without the flows folder, and the uploads and outputs folders, as a file node writes them.

    Raises:
        NotYours: with a sentence naming a folder Flowfile keeps, else :func:`browse_refusal`.
    """
    place = data_place(path, user_id, write=True)
    if place is None:
        if file_folder(path, user_id) is not None:
            raise NotYours(f'"{path}" is kept by Flowfile, so nothing is uploaded there.')
        raise NotYours(browse_refusal(path))
    return place


def _places_named() -> str:
    return "your workspace or the shared folder" if has_shared_folder() else "your workspace"


def refusal(path: str) -> str:
    """The sentence a person reads when a flow names a path outside their places."""
    return f'"{path}" is not in {_places_named()}, so this flow cannot use it.'


def pattern_refusal(path: str) -> str:
    """The sentence a person reads when a folder or pattern in their places also matches others' files."""
    return (
        f'"{path}" also matches files in other people\'s workspaces, so this flow cannot read it. '
        f"Name a folder or pattern inside {_places_named()}."
    )


def browse_refusal(path: str) -> str:
    """The sentence a person reads when they open, save, upload or download a file outside their places."""
    return f'"{path}" is not in {_places_named()}.'


def _top_folders(user_id: int | None, places: Places) -> tuple[set[str], set[tuple[int, int]]]:
    """The folders no delete removes, by text and by file identity: the browser's roots, the places' own folders,
    the flows, uploads and outputs folders and the folder of workspaces."""
    folders = [root.path for root in browse_roots(user_id)]
    folders += [
        *places.roots,
        storage.flows_directory,
        storage.uploads_directory,
        storage.outputs_directory,
        workspaces_root(),
    ]
    named: set[str] = set()
    identities: set[tuple[int, int]] = set()
    for folder in folders:
        named.update({os.path.realpath(folder), os.path.abspath(folder)})
        try:
            found = os.stat(folder)
        except OSError:
            continue
        identities.add((found.st_dev, found.st_ino))
    return named, identities


def deletable(path: str, user_id: int | None) -> tuple[str, bool]:
    """Where a delete of ``path`` for ``user_id`` acts, and whether that is a link to remove rather than follow.

    A file or a folder in the person's workspace, the shared folder or the uploads and outputs folders, as a file
    node writes them; never one of those folders itself, nor a folder that holds anything outside them. A link is
    removed where it is, so it never leads the delete to what it points at.

    Raises:
        NotYours: with :func:`browse_refusal`, or a sentence naming a top folder.
        FileNotFoundError: when nothing is at ``path``.
    """
    places = Places(user_id, "write")
    target = places.entry(path)
    if target is None:
        if Places(user_id, "browse").entry(path) is not None:
            raise NotYours(f'"{path}" is kept by Flowfile and cannot be deleted here.')
        raise NotYours(browse_refusal(path))
    named, identities = _top_folders(user_id, places)
    if not os.path.lexists(target):
        raise FileNotFoundError(path)
    link = os.path.islink(target)
    if not link:
        found = os.stat(target)
        if target in named or (found.st_dev, found.st_ino) in identities:
            raise NotYours(f'"{path}" is a top folder and cannot be deleted.')
        if os.path.isdir(target) and not places.holds_all_of(target):
            raise NotYours(f'"{path}" holds files outside {_places_named()}, so it cannot be deleted.')
    return target, link


def items_in(folder: str) -> int:
    """How many files and folders lie in ``folder``, at any depth; links are counted, not followed."""
    count = 0
    for _, folders, files in os.walk(folder):
        count += len(folders) + len(files)
    return count


def delete_place(real: str, link: bool, dry_run: bool = False) -> int:
    """Delete what :func:`deletable` answered, a folder with what is in it; how many items went with it. With
    ``dry_run`` nothing is deleted."""
    is_folder = not link and os.path.isdir(real)
    items = items_in(real) if is_folder else 0
    if not dry_run:
        if is_folder:
            shutil.rmtree(real)
        else:
            os.unlink(real)
    return items


def explorer_for(
    directory: str | Path, user_id: int | None, desktop_root: str | Path | None, use: Use = "browse"
) -> SecureFileExplorer:
    """A file explorer at ``directory`` for ``user_id``; ``NotYours`` when it is none of their places.

    On a server the explorer is held to the folder of ``Places`` that holds ``directory``, the shared folder without
    the folders left out of it. Elsewhere it is held to ``desktop_root``, as the browse routes always were.
    """
    if not is_server_mode():
        return SecureFileExplorer(directory, desktop_root)
    places = Places(user_id, use)
    root = places.root_of(os.path.realpath(os.path.expanduser(str(directory))))
    if root is None:
        raise NotYours(browse_refusal(str(directory)))
    excluded = list(places.left_out.folders) if root == places.shared else []
    try:
        return SecureFileExplorer(directory, root, excluded=excluded)
    except PermissionError:
        raise NotYours(browse_refusal(str(directory))) from None


def checked_flow_path(path: str, user_id: int | None) -> str:
    """The resolved path a by-path save, create or open of a flow uses; a 403 with :func:`browse_refusal` on a
    server when it is none of ``user_id``'s places. Elsewhere ``validate_path_under_cwd``, as always."""
    if not is_server_mode():
        return validate_path_under_cwd(path)
    place = flow_file_place(path, user_id)
    if place is None:
        raise HTTPException(403, browse_refusal(path))
    return place


def checked_data_path(path: str, user_id: int | None) -> str:
    """The resolved path a route reads a data file at; a 403 with :func:`browse_refusal` on a server when it is none
    of ``user_id``'s places. Elsewhere ``validate_path_under_cwd``, as always."""
    if not is_server_mode():
        return validate_path_under_cwd(path)
    place = data_place(path, user_id)
    if place is None:
        raise HTTPException(403, browse_refusal(path))
    return place


def running_person(settings_user_id: int | None) -> int | None:
    """Whose workspace a node uses: the person a call is made for (an editor run, fetch or prediction), else the
    user its settings carry: the person whose editor session holds the flow, or the user a headless or scheduled
    run stamps its nodes with."""
    from flowfile_core.flowfile.execution import registry

    acting = registry.acting()
    return acting if acting is not None else settings_user_id


def _matches(pattern: str) -> list[str]:
    """What a reader may expand ``pattern`` to: hidden names and ``**`` included, as Polars expands them."""
    return glob.glob(os.path.expanduser(pattern), recursive=True, include_hidden=True)


def require_data_paths(
    user_id: int | None, *paths: str | None, expand_glob: bool = False, shown: str | None = None, write: bool = False
) -> None:
    """Refuse, on a server, a local path a node would read, or with ``write`` write, outside ``user_id``'s places.

    A URL and an object-storage URI are left to their own nodes' rules. With ``expand_glob`` a pattern's matches
    are checked too, and everything in a folder, since a reader expands both. ``shown`` is the path the person
    typed, named in the sentence in place of the pattern built from it.

    Raises:
        NotYours: with :func:`refusal`, or :func:`pattern_refusal` when only some matches are outside.
    """
    if not is_server_mode():
        return
    places = Places(user_id, "write" if write else "read")
    for path in paths:
        if not path or is_url(path) or is_cloud_uri(path):
            continue
        named = shown or path
        real = places.place(path)
        if real is None:
            raise NotYours(refusal(named))
        if not expand_glob:
            continue
        if is_glob_pattern(path):
            inside = all(places.holds_match(match) for match in _matches(path))
        else:
            inside = not os.path.isdir(real) or places.holds_all_of(real)
        if not inside:
            raise NotYours(pattern_refusal(named))
