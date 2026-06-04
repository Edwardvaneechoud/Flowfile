import asyncio
import json
import logging
import os
import re
import socket
import tempfile
import threading
import time
import uuid
from pathlib import Path

import docker
import docker.errors
import docker.types
import httpx

from flowfile_core.configs.flow_logger import FlowLogger
from flowfile_core.kernel.models import (
    ArtifactPersistenceInfo,
    CleanupRequest,
    CleanupResult,
    ClearNodeArtifactsResult,
    ExecuteRequest,
    ExecuteResult,
    ImageFlavour,
    KernelConfig,
    KernelInfo,
    KernelMemoryInfo,
    KernelState,
    RecoveryStatus,
    ResolvedPackage,
)
from shared.storage_config import storage

logger = logging.getLogger(__name__)

_KERNEL_IMAGE_BASE_DEFAULT = "edwardvaneechoud/flowfile-kernel-base:0.3.0"
_KERNEL_IMAGE_ML_DEFAULT = "edwardvaneechoud/flowfile-kernel-ml:0.3.0"
_KERNEL_IMAGE_LITE_DEFAULT = "edwardvaneechoud/flowfile-kernel-lite:0.3.0"


def _envvar_or_default(name: str, default: str) -> str:
    """Read an env var, treating unset OR empty/whitespace as 'use default'.

    Compose's ``${VAR:-}`` writes an empty string into the container when the
    host hasn't set the var; treat that the same as 'unset' so we fall back to
    the registry default instead of trying to ``docker run ""``.
    """
    return (os.environ.get(name) or "").strip() or default


# FLOWFILE_KERNEL_IMAGE is the legacy override for the base image (kept for
# backwards compatibility). FLOWFILE_KERNEL_IMAGE_BASE / _ML let an operator
# pin each flavour to a specific tag (or their own registry). Reads happen at
# lookup time, not module-import time, so the env var can be set after Python
# starts (e.g. by a container entrypoint, or a pytest step env block) without
# poisoning the rest of the process with the default value.
def _kernel_image_base() -> str:
    return _envvar_or_default(
        "FLOWFILE_KERNEL_IMAGE_BASE",
        _envvar_or_default("FLOWFILE_KERNEL_IMAGE", _KERNEL_IMAGE_BASE_DEFAULT),
    )


def _kernel_image_ml() -> str:
    return _envvar_or_default("FLOWFILE_KERNEL_IMAGE_ML", _KERNEL_IMAGE_ML_DEFAULT)


def _kernel_image_lite() -> str:
    return _envvar_or_default("FLOWFILE_KERNEL_IMAGE_LITE", _KERNEL_IMAGE_LITE_DEFAULT)


def _flavour_images() -> dict[ImageFlavour, str]:
    return {
        ImageFlavour.BASE: _kernel_image_base(),
        ImageFlavour.ML: _kernel_image_ml(),
        ImageFlavour.LITE: _kernel_image_lite(),
    }


def _resolve_image(
    flavour: ImageFlavour,
    custom_image: str | None,
    docker_client=None,
) -> str:
    """Resolve the image tag to use for ``flavour``.

    When ``docker_client`` is provided, falls back to a locally-built variant
    (``flowfile-kernel-<flavour>:local`` or any other tag matching the repo
    name) if the registry default isn't present. Without a client (e.g. unit
    tests not exercising the docker layer) returns the registry default
    unchanged.
    """
    if flavour == ImageFlavour.CUSTOM:
        if not custom_image:
            raise ValueError(
                "custom_image must be provided when image_flavour='custom'"
            )
        _validate_custom_image(custom_image)
        return custom_image
    registry_default = _flavour_images()[flavour]
    if docker_client is None:
        return registry_default
    return _resolve_local_image(flavour, docker_client, registry_default) or registry_default


def _resolve_local_image(
    flavour: ImageFlavour,
    docker_client,
    registry_default: str,
) -> str | None:
    """Return a locally-available tag for ``flavour``, or ``None`` if nothing matches.

    Priority:
    1. The registry default itself if present locally
    2. ``flowfile-kernel-<flavour>:local`` (docker-compose dev build convention)
    3. Any other tag of the ``flowfile-kernel-<flavour>`` repo, newest first
    """
    try:
        docker_client.images.get(registry_default)
        return registry_default
    except docker.errors.ImageNotFound:
        pass
    except docker.errors.APIError:
        return None

    repo_name = f"flowfile-kernel-{flavour.value}"
    try:
        images = docker_client.images.list(name=repo_name)
    except docker.errors.APIError:
        return None
    if not images:
        return None

    preferred_tag = f"{repo_name}:local"
    for img in images:
        if preferred_tag in (img.tags or ()):
            return preferred_tag

    # Fall back to the most recently-created tag for this repo
    images_sorted = sorted(
        images,
        key=lambda i: i.attrs.get("Created", "") if isinstance(i.attrs, dict) else "",
        reverse=True,
    )
    for img in images_sorted:
        for tag in img.tags or ():
            if tag.startswith(f"{repo_name}:"):
                return tag
    return None


# Pip package specifier (PEP 508-ish, conservative): rejects anything with
# shell metacharacters so we can pass the list straight into a docker build.
# \A / \Z anchor strictly — `$` would let a trailing newline through.
_VALID_PACKAGE_SPEC = re.compile(r"\A[A-Za-z0-9_.\-+\[\]<>=!~,]+\Z")
_KERNEL_ID_TAG_RE = re.compile(r"[^a-z0-9._-]")
_VALID_TAG_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.\-]{0,127}$")
_DIGEST_RE = re.compile(r"@sha256:[A-Fa-f0-9]{12,}$")

# Docker labels stamped on every derived image so orphan GC can filter by
# (a) which Core instance built it, and (b) which kernel id it belongs to —
# without parsing the tag. Multiple Core instances against the same daemon
# can coexist because GC only touches images carrying this Core's instance id.
_IMAGE_LABEL_CORE_INSTANCE = "flowfile_core_instance"
_IMAGE_LABEL_KERNEL_ID = "flowfile_kernel_id"


def _load_or_create_core_instance_id() -> str:
    """Return a stable UUID identifying this Core install.

    Persisted under ``<storage.base_directory>/core_instance_id.txt`` so a
    Core restart matches its previously-built derived images. Falls back to
    an in-memory UUID if the file can't be written (e.g. read-only FS).
    """
    try:
        path = storage.base_directory / "core_instance_id.txt"
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            existing = path.read_text().strip()
            if existing:
                return existing
        new_id = str(uuid.uuid4())
        path.write_text(new_id)
        return new_id
    except OSError:
        logger.warning(
            "Could not persist core instance id; using volatile id for this run only"
        )
        return str(uuid.uuid4())


def _derived_image_tag(kernel_id: str) -> str:
    """Stable tag for the per-kernel derived image."""
    safe = _KERNEL_ID_TAG_RE.sub("-", kernel_id.lower())
    return f"flowfile-kernel-derived-{safe}:latest"


def _validate_packages(packages: list[str]) -> None:
    for pkg in packages:
        if not _VALID_PACKAGE_SPEC.match(pkg):
            raise ValueError(
                f"Invalid package specifier {pkg!r}: only PyPI-safe characters "
                "(alphanumerics, '.-_+[]<>=!~,') are allowed."
            )


# Strip a pip spec like ``pandas[extra]>=2.3,<3`` down to its package name.
_PACKAGE_NAME_RE = re.compile(r"^[A-Za-z0-9_.\-]+")


def _spec_to_name(spec: str) -> str:
    """Extract the canonical package name from a pip specifier."""
    match = _PACKAGE_NAME_RE.match(spec.strip())
    if not match:
        return spec.strip().lower()
    # PEP 503 normalisation: lowercase + collapse runs of -_. to '-'
    return re.sub(r"[-_.]+", "-", match.group(0).lower())


def _validate_custom_image(uri: str) -> None:
    """Custom image URIs must pin an explicit version (tag or @sha256 digest).

    Untagged refs default to ``:latest`` on Docker and break reproducibility.
    """
    ref = (uri or "").strip()
    if not ref:
        raise ValueError("Custom image URI is empty.")
    if _DIGEST_RE.search(ref):
        return
    last_slash = ref.rfind("/")
    last_segment = ref[last_slash + 1 :] if last_slash >= 0 else ref
    if ":" not in last_segment:
        raise ValueError(
            f"Custom image {ref!r} has no explicit tag. "
            "Append a version, e.g. 'myorg/kernel:1.2.3' or use '@sha256:...'."
        )
    tag = last_segment.split(":", 1)[1]
    if not tag or not _VALID_TAG_RE.match(tag):
        raise ValueError(
            f"Custom image {ref!r} has an invalid tag {tag!r}. "
            "Tags must start with an alphanumeric and may include '.-_'."
        )


_BASE_PORT = 19000
_PORT_RANGE = 1000  # 19000-19999
_HEALTH_TIMEOUT = 120
_HEALTH_POLL_INTERVAL = 2


def _is_docker_mode() -> bool:
    """Check if running in Docker mode based on FLOWFILE_MODE."""
    return os.environ.get("FLOWFILE_MODE") == "docker"


def _is_port_available(port: int) -> bool:
    """Check whether a TCP port is free on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("127.0.0.1", port))
            return True
        except OSError:
            return False


class KernelManager:
    def __init__(self, shared_volume_path: str | None = None):
        self._docker = docker.from_env()
        # Stable id for this Core install; stamped onto every derived image
        # so orphan GC only touches images this Core built.
        self._core_instance_id = _load_or_create_core_instance_id()
        self._kernels: dict[str, KernelInfo] = {}
        self._kernel_owners: dict[str, int] = {}  # kernel_id -> user_id
        # Cached scratch FlowRegistration ids — one per kernel. Populated on
        # create_kernel and during _restore_kernels_from_db; consulted in the
        # execute path so ``flowfile_ctx.publish_global`` / ``schema.publish_artifact``
        # have a valid producer to point at when no real source_registration_id
        # is in scope. Cleared on delete_kernel.
        self._scratch_flow_ids: dict[str, int] = {}
        # Serialises _provision_scratch_flow so two concurrent create / publish
        # paths for the same kernel can't both insert a FlowRegistration row.
        self._scratch_flow_lock = threading.Lock()
        # Background image-pull state: image-tag -> "pulling" or "error:<msg>".
        # Absent key means idle. Lock is held only for read/write, never across
        # a network call (the actual pull runs on a thread without the lock).
        self._pull_state: dict[str, str] = {}
        self._pull_state_lock = threading.Lock()
        self._shared_volume = shared_volume_path or str(storage.cache_directory)
        # Catalog tables (Delta-format) live outside the shared volume. The
        # kernel needs a separate mount so ``flowfile_ctx.read_catalog_table`` /
        # ``write_catalog_table`` can read/write Delta directories directly.
        self._catalog_tables_dir = str(storage.catalog_tables_directory)

        # Docker-in-Docker settings: when core itself runs in a container,
        # kernel containers must use a named volume (not a bind mount) and
        # connect to the same Docker network for service discovery.
        self._docker_network: str | None = os.environ.get("FLOWFILE_DOCKER_NETWORK") or self._detect_docker_network()

        # In Docker mode, discover the volume that covers _shared_volume
        # (e.g. flowfile-internal-storage mounted at /app/internal_storage).
        # Kernel containers will mount the same volume at the same path so
        # all file paths are identical across core, worker, and kernel.
        self._kernel_volume: str | None = None
        self._kernel_volume_type: str | None = None
        self._kernel_mount_target: str | None = None  # mount point inside containers
        # Catalog volume (separate from _kernel_volume when catalog_tables is
        # under user_data_directory rather than the internal storage volume).
        self._catalog_volume: str | None = None
        self._catalog_volume_type: str | None = None
        self._catalog_mount_target: str | None = None
        if _is_docker_mode():
            vol_name, vol_type, mount_dest = self._discover_volume_for_path(self._shared_volume)
            if vol_name:
                self._kernel_volume = vol_name
                self._kernel_volume_type = vol_type
                self._kernel_mount_target = mount_dest
                logger.info(
                    "Docker-in-Docker mode: volume=%s (type=%s) at %s covers shared_path=%s, network=%s",
                    vol_name,
                    vol_type,
                    mount_dest,
                    self._shared_volume,
                    self._docker_network,
                )
            else:
                logger.warning(
                    "Could not discover volume for shared_path=%s; "
                    "kernel containers will use bind mounts (local mode only)",
                    self._shared_volume,
                )

            cvol_name, cvol_type, cmount_dest = self._discover_volume_for_path(self._catalog_tables_dir)
            if cvol_name and cvol_name != self._kernel_volume:
                self._catalog_volume = cvol_name
                self._catalog_volume_type = cvol_type
                self._catalog_mount_target = cmount_dest
                logger.info(
                    "Catalog volume=%s (type=%s) at %s covers catalog_tables_path=%s",
                    cvol_name,
                    cvol_type,
                    cmount_dest,
                    self._catalog_tables_dir,
                )

        self._restore_kernels_from_db()
        self._reclaim_running_containers()
        self._remove_orphan_derived_images()

    @property
    def shared_volume_path(self) -> str:
        return self._shared_volume

    # Docker-in-Docker helpers

    def _detect_docker_network(self) -> str | None:
        """Auto-detect the Docker network this container is connected to.

        When core runs inside Docker, we inspect the current container's
        network settings and return the first user-defined network.  This
        allows kernel containers to be attached to the same network without
        requiring an explicit FLOWFILE_DOCKER_NETWORK env var.
        """
        if not _is_docker_mode():
            return None
        try:
            hostname = socket.gethostname()
            container = self._docker.containers.get(hostname)
            networks = container.attrs["NetworkSettings"]["Networks"]
            for name in networks:
                if name not in ("bridge", "host", "none"):
                    return name
        except Exception as exc:
            logger.debug("Could not auto-detect Docker network: %s", exc)
        return None

    def _discover_volume_for_path(self, path: str) -> tuple[str | None, str | None, str | None]:
        """Find which Docker volume/bind covers *path* in this container.

        Inspects the current container's mounts and returns the one whose
        ``Destination`` is a prefix of *path* (longest match wins).

        Returns ``(source_or_name, mount_type, destination)`` or
        ``(None, None, None)`` if no mount covers the path.
        """
        try:
            hostname = socket.gethostname()
            container = self._docker.containers.get(hostname)
            mounts = container.attrs.get("Mounts", [])
            logger.debug("Container %s mounts: %s", hostname, mounts)

            best: dict | None = None
            for mount in mounts:
                dest = mount.get("Destination", "")
                if path.startswith(dest) and (best is None or len(dest) > len(best.get("Destination", ""))):
                    best = mount

            if best:
                mount_type = best.get("Type", "volume")
                dest = best["Destination"]
                name = best.get("Name") if mount_type == "volume" else best.get("Source")
                return name, mount_type, dest

            logger.warning("No mount covers path %s in container %s", path, hostname)
        except Exception as exc:
            logger.warning("Could not inspect container mounts: %s", exc)
        return None, None, None

    def _kernel_url(self, kernel: KernelInfo) -> str:
        """Return the base URL for communicating with a kernel container.

        In Docker-in-Docker mode, use the container name on the shared
        Docker network.  Otherwise, use localhost with the mapped host port.
        """
        if self._docker_network:
            return f"http://flowfile-kernel-{kernel.id}:9999"
        return f"http://localhost:{kernel.port}"

    def to_kernel_path(self, local_path: str) -> str:
        """Translate a local filesystem path to the path visible inside a kernel container.

        In Docker-in-Docker mode the volume is mounted at the same path in all
        containers, so paths are identical.  In local mode the host shared dir is
        bind-mounted at ``/shared`` and the host catalog_tables dir at
        ``/catalog_tables``; we swap whichever prefix matches.
        """
        if self._kernel_volume:
            # Same volume, same mount point — no translation needed
            return local_path
        # Local mode: try each known prefix
        normalized = os.path.normpath(local_path)
        catalog_norm = os.path.normpath(self._catalog_tables_dir)
        if normalized == catalog_norm or normalized.startswith(catalog_norm + os.sep):
            return local_path.replace(self._catalog_tables_dir, "/catalog_tables", 1)
        return local_path.replace(self._shared_volume, "/shared", 1)

    def resolve_node_paths(self, request: "ExecuteRequest") -> None:
        """Populate ``input_paths`` and ``output_dir`` from ``flow_id``/``node_id``.

        When the frontend sends only ``flow_id`` and ``node_id`` (without
        pre-built filesystem paths), this method resolves the actual paths
        on the shared volume and translates them for the kernel container.
        If ``input_paths`` is already populated (e.g. from ``flow_graph.py``),
        this is a no-op.
        """
        if request.input_paths or not request.flow_id or not request.node_id:
            return

        input_dir = os.path.join(
            self._shared_volume,
            str(request.flow_id),
            str(request.node_id),
            "inputs",
        )
        output_dir = os.path.join(
            self._shared_volume,
            str(request.flow_id),
            str(request.node_id),
            "outputs",
        )

        # Discover parquet files in the input directory and group by input name.
        # Files are named {name}_{index}.parquet (e.g. orders_0.parquet, clients_1.parquet).
        if os.path.isdir(input_dir):
            parquet_files = sorted(f for f in os.listdir(input_dir) if f.endswith(".parquet"))
            if parquet_files:
                input_paths: dict[str, list[str]] = {}
                all_paths: list[str] = []
                for f in parquet_files:
                    kernel_path = self.to_kernel_path(os.path.join(input_dir, f))
                    all_paths.append(kernel_path)
                    # Extract name from filename pattern: name_index.parquet
                    stem = f[: -len(".parquet")]
                    parts = stem.rsplit("_", 1)
                    name = parts[0] if len(parts) == 2 and parts[1].isdigit() else "main"
                    input_paths.setdefault(name, []).append(kernel_path)
                # Always include "main" as backward-compatible alias for all inputs
                if "main" not in input_paths:
                    input_paths["main"] = all_paths
                request.input_paths = input_paths

        request.output_dir = self.to_kernel_path(output_dir)

    def _build_run_kwargs(self, kernel_id: str, kernel: KernelInfo, env: dict) -> dict:
        """Build Docker ``containers.run()`` keyword arguments.

        Adapts volume mounts and networking for local vs Docker-in-Docker.
        Always mounts the catalog_tables directory so kernel cells can read
        and write Delta-format catalog tables directly.
        """
        run_kwargs: dict = {
            "detach": True,
            "name": f"flowfile-kernel-{kernel_id}",
            "environment": env,
            "mem_limit": f"{kernel.memory_gb}g",
            "nano_cpus": int(kernel.cpu_cores * 1e9),
        }

        if self._kernel_volume:
            # Docker-in-Docker: mount the same volume at the same path so
            # all file paths are identical in core, worker, and kernel.
            mount_type = self._kernel_volume_type or "volume"
            mount_target = self._kernel_mount_target or "/app/internal_storage"
            mounts = [
                docker.types.Mount(
                    target=mount_target,
                    source=self._kernel_volume,
                    type=mount_type,
                    read_only=False,
                )
            ]
            # Add the catalog volume if it's separate (catalog_tables lives
            # under user_data_directory, which is typically a different volume
            # than internal storage).
            if self._catalog_volume:
                mounts.append(
                    docker.types.Mount(
                        target=self._catalog_mount_target or "/app/user_data",
                        source=self._catalog_volume,
                        type=self._catalog_volume_type or "volume",
                        read_only=False,
                    )
                )
            run_kwargs["mounts"] = mounts
            if self._docker_network:
                run_kwargs["network"] = self._docker_network
        else:
            # Local: bind-mount the shared dir + the catalog_tables dir, and map ports.
            os.makedirs(self._catalog_tables_dir, exist_ok=True)
            run_kwargs["volumes"] = {
                self._shared_volume: {"bind": "/shared", "mode": "rw"},
                self._catalog_tables_dir: {"bind": "/catalog_tables", "mode": "rw"},
            }
            run_kwargs["ports"] = {"9999/tcp": kernel.port}
            run_kwargs["extra_hosts"] = {"host.docker.internal": "host-gateway"}

        return run_kwargs

    # Database persistence helpers

    def _restore_kernels_from_db(self) -> None:
        """Load persisted kernel configs from the database on startup."""
        try:
            from flowfile_core.database.connection import get_db_context
            from flowfile_core.kernel.persistence import get_all_kernel_scratch_ids, get_all_kernels

            with get_db_context() as db:
                for config, resolved_packages, user_id in get_all_kernels(db):
                    if config.id in self._kernels:
                        continue
                    kernel = KernelInfo(
                        id=config.id,
                        name=config.name,
                        state=KernelState.STOPPED,
                        packages=config.packages,
                        resolved_packages=resolved_packages,
                        memory_gb=config.memory_gb,
                        cpu_cores=config.cpu_cores,
                        gpu=config.gpu,
                        image_flavour=config.image_flavour,
                        custom_image=config.custom_image,
                    )
                    self._kernels[config.id] = kernel
                    self._kernel_owners[config.id] = user_id
                    logger.info("Restored kernel '%s' for user %d from database", config.id, user_id)
                # Hydrate the in-memory scratch-flow id cache so the execute
                # path doesn't have to hit the DB on each call. Missing values
                # (e.g. kernels created before this migration ran) are filled
                # in lazily by get_scratch_flow_id.
                for kernel_id, scratch_id in get_all_kernel_scratch_ids(db).items():
                    if scratch_id is not None:
                        self._scratch_flow_ids[kernel_id] = scratch_id
        except Exception:
            # Log with full traceback so a silently-swallowed schema-drift bug
            # can't lurk again (a missing column here used to disappear into a
            # one-line warning).
            logger.exception("Could not restore kernels from database")

    def _persist_kernel(self, kernel: KernelInfo, user_id: int) -> None:
        """Save a kernel record to the database."""
        try:
            from flowfile_core.database.connection import get_db_context
            from flowfile_core.kernel.persistence import save_kernel

            with get_db_context() as db:
                save_kernel(db, kernel, user_id)
        except Exception as exc:
            logger.warning("Could not persist kernel '%s': %s", kernel.id, exc)

    def _remove_kernel_from_db(self, kernel_id: str) -> None:
        """Remove a kernel record from the database."""
        try:
            from flowfile_core.database.connection import get_db_context
            from flowfile_core.kernel.persistence import delete_kernel

            with get_db_context() as db:
                delete_kernel(db, kernel_id)
        except Exception as exc:
            logger.warning("Could not remove kernel '%s' from database: %s", kernel_id, exc)

    # Scratch FlowRegistration management
    #
    # Each kernel owns an auto-created FlowRegistration row that gets used as
    # ``source_registration_id`` whenever a publish call has no real flow in
    # scope. This preserves Core's invariant that every artifact has a
    # producing flow without requiring the user to register one manually.

    def _provision_scratch_flow(self, kernel_id: str, user_id: int) -> None:
        """Create a scratch FlowRegistration row for *kernel_id* if missing.

        The scratch flow is registered inside the seeded ``General/default``
        schema so that artifacts published from cells without an explicit
        ``namespace_id`` inherit that namespace and become visible in the
        catalog UI's tree view (which only walks ``namespace_id`` non-null
        rows). Safe to call multiple times — does nothing if the kernel
        already has one cached. Failures are logged and swallowed; publish
        calls then fall back to the legacy "no source registration" path
        (warning + return -1).
        """
        # Serialise check-then-insert so concurrent callers don't both create
        # a FlowRegistration row for the same kernel. The path is rare (once
        # per kernel) so a single dict-wide lock is fine.
        with self._scratch_flow_lock:
            if kernel_id in self._scratch_flow_ids:
                return
            try:
                from flowfile_core.catalog import SQLAlchemyCatalogRepository
                from flowfile_core.catalog.service import CatalogService
                from flowfile_core.database.connection import get_db_context
                from flowfile_core.database.models import FlowRegistration
                from flowfile_core.kernel.persistence import set_kernel_scratch_flow_id

                with get_db_context() as db:
                    # Place the scratch flow under the seeded default schema so
                    # artifacts published from cells appear in the catalog tree.
                    # If the default schema hasn't been initialised yet, fall back
                    # to namespace_id=None (artifacts will exist but won't show
                    # in the tree until a default schema exists).
                    try:
                        default_namespace_id = CatalogService(
                            SQLAlchemyCatalogRepository(db)
                        ).get_default_namespace_id()
                    except Exception:
                        default_namespace_id = None
                    flow = FlowRegistration(
                        name=f"_kernel_scratch_{kernel_id}",
                        description=(
                            "Auto-created producer for artifacts published from "
                            f"kernel '{kernel_id}' interactive cells."
                        ),
                        flow_path=f"<kernel:{kernel_id}>",
                        namespace_id=default_namespace_id,
                        owner_id=user_id,
                    )
                    db.add(flow)
                    db.commit()
                    db.refresh(flow)
                    set_kernel_scratch_flow_id(db, kernel_id, flow.id)
                    self._scratch_flow_ids[kernel_id] = flow.id
                    logger.debug(
                        "Provisioned scratch FlowRegistration id=%s (namespace=%s) for kernel '%s'",
                        flow.id,
                        default_namespace_id,
                        kernel_id,
                    )
            except Exception:
                logger.exception(
                    "Could not provision scratch FlowRegistration for kernel '%s'",
                    kernel_id,
                )

    def _discard_scratch_flow(self, kernel_id: str) -> None:
        """Delete the scratch FlowRegistration row (if any) on kernel deletion."""
        scratch_id = self._scratch_flow_ids.pop(kernel_id, None)
        if scratch_id is None:
            return
        try:
            from flowfile_core.database.connection import get_db_context
            from flowfile_core.database.models import FlowRegistration

            with get_db_context() as db:
                row = db.get(FlowRegistration, scratch_id)
                if row is not None:
                    db.delete(row)
                    db.commit()
        except Exception as exc:
            logger.warning(
                "Could not delete scratch FlowRegistration id=%s for kernel '%s': %s",
                scratch_id,
                kernel_id,
                exc,
            )

    def get_scratch_flow_id(self, kernel_id: str) -> int | None:
        """Return the kernel's scratch FlowRegistration id, provisioning lazily if absent.

        Called by the execute path when no explicit ``source_registration_id``
        is in scope. The lazy-provision branch covers kernels created before
        the auto-scratch feature shipped (their row has ``NULL`` in the new
        column) and edges where the scratch was removed out-of-band.
        """
        cached = self._scratch_flow_ids.get(kernel_id)
        if cached is not None:
            return cached
        user_id = self._kernel_owners.get(kernel_id)
        if user_id is None:
            return None
        self._provision_scratch_flow(kernel_id, user_id)
        return self._scratch_flow_ids.get(kernel_id)

    # Port allocation

    def _reclaim_running_containers(self) -> None:
        """Discover running flowfile-kernel containers and reclaim their ports."""
        try:
            containers = self._docker.containers.list(filters={"name": "flowfile-kernel-", "status": "running"})
        except (docker.errors.APIError, docker.errors.DockerException) as exc:
            logger.warning("Could not list running containers: %s", exc)
            return

        for container in containers:
            name = container.name
            if not name.startswith("flowfile-kernel-"):
                continue
            kernel_id = name[len("flowfile-kernel-") :]

            if kernel_id in self._kernels:
                # Determine which host port is mapped (not available in DinD mode)
                port = None
                if not self._kernel_volume:
                    try:
                        bindings = container.attrs["NetworkSettings"]["Ports"].get("9999/tcp")
                        if bindings:
                            port = int(bindings[0]["HostPort"])
                    except (KeyError, IndexError, TypeError, ValueError):
                        pass

                # Kernel was restored from DB — update with runtime info
                self._kernels[kernel_id].container_id = container.id
                if port is not None:
                    self._kernels[kernel_id].port = port
                self._kernels[kernel_id].state = KernelState.IDLE
                logger.info(
                    "Reclaimed running kernel '%s' (container %s)",
                    kernel_id,
                    container.short_id,
                )
            else:
                # Orphan container with no DB record — stop it
                logger.warning(
                    "Found orphan kernel container '%s' with no database record, stopping it",
                    kernel_id,
                )
                try:
                    container.stop(timeout=10)
                    container.remove(force=True)
                except Exception as exc:
                    logger.warning("Error stopping orphan container '%s': %s", kernel_id, exc)

    def _allocate_port(self) -> int:
        """Find the next available port in the kernel port range."""
        used_ports = {k.port for k in self._kernels.values() if k.port is not None}
        for port in range(_BASE_PORT, _BASE_PORT + _PORT_RANGE):
            if port not in used_ports and _is_port_available(port):
                return port
        raise RuntimeError(f"No available ports in range {_BASE_PORT}-{_BASE_PORT + _PORT_RANGE - 1}")

    # Image discovery and pulling

    def resolve_local_image(self, flavour: ImageFlavour) -> str | None:
        """Public accessor for the local-image fallback resolver.

        Returns the tag actually in use for ``flavour`` (registry default if
        present locally, else a ``flowfile-kernel-<flavour>:*`` variant). Used
        by the docker-status route so the UI can show "Found locally: X".
        """
        if flavour == ImageFlavour.CUSTOM:
            return None
        registry_default = _flavour_images()[flavour]
        return _resolve_local_image(flavour, self._docker, registry_default)

    def get_pull_state(self, image_tag: str) -> str | None:
        """Return current pull state for ``image_tag``, or ``None`` if idle."""
        with self._pull_state_lock:
            return self._pull_state.get(image_tag)

    def start_image_pull(self, flavour: ImageFlavour) -> str:
        """Kick off a background pull for the flavour's registry-default image.

        Idempotent: if a pull is already running for that tag, returns the
        existing state without spawning a duplicate worker.
        """
        if flavour == ImageFlavour.CUSTOM:
            raise ValueError(
                "Cannot pull the 'custom' flavour: no canonical image to pull. "
                "Provide a custom image URI when creating the kernel instead."
            )
        image_tag = _flavour_images()[flavour]
        with self._pull_state_lock:
            existing = self._pull_state.get(image_tag)
            if existing == "pulling":
                return existing
            self._pull_state[image_tag] = "pulling"

        threading.Thread(
            target=self._do_pull,
            args=(image_tag,),
            name=f"image-pull-{image_tag}",
            daemon=True,
        ).start()
        return "pulling"

    def _do_pull(self, image_tag: str) -> None:
        """Worker run on a background thread to fetch ``image_tag``.

        On success clears the pull state (the image is now locally available,
        and a follow-up ``docker-status`` poll will reflect that). On failure
        stores ``error:<message>`` so the UI can show what went wrong.
        """
        try:
            repo, _, tag = image_tag.partition(":")
            self._docker.images.pull(repo, tag=tag or "latest")
            with self._pull_state_lock:
                self._pull_state.pop(image_tag, None)
            logger.info("Pulled image '%s'", image_tag)
        except Exception as exc:
            msg = str(exc).splitlines()[0][:200] if str(exc) else exc.__class__.__name__
            with self._pull_state_lock:
                self._pull_state[image_tag] = f"error:{msg}"
            logger.exception("Failed to pull image '%s'", image_tag)

    # Derived image build (per-kernel, packages baked in)

    def _build_derived_image(self, kernel: KernelInfo) -> str:
        """Build a derived image with the kernel's extra packages baked in.

        Reuses the existing image if it is already present locally. Returns the
        derived image tag.
        """
        _validate_packages(kernel.packages)
        base_image = _resolve_image(kernel.image_flavour, kernel.custom_image, self._docker)
        derived_tag = _derived_image_tag(kernel.id)

        try:
            self._docker.images.get(derived_tag)
            logger.info("Reusing existing derived image '%s'", derived_tag)
            return derived_tag
        except docker.errors.ImageNotFound:
            pass

        # Make sure the FROM image is on the host before we start the build.
        # docker build will otherwise reach for the registry and produce a
        # confusing "manifest unknown" error when the image isn't pushed yet.
        try:
            self._docker.images.get(base_image)
        except docker.errors.ImageNotFound:
            raise RuntimeError(
                f"Base image '{base_image}' is not available locally. "
                f"Pull it first: docker pull {base_image} "
                f"(or set FLOWFILE_KERNEL_IMAGE_{kernel.image_flavour.value.upper()} "
                "to a tag you already have)."
            ) from None

        # JSON exec form keeps each package as a discrete argv item — no shell
        # interpretation, no quoting bugs.
        pip_args = (
            ["pip", "install", "--no-cache-dir", "--constraint", "/opt/constraints.txt"]
            + list(kernel.packages)
        )
        # Stamp Core-instance + kernel-id labels so orphan GC can find images
        # owned by this Core without relying on tag-name parsing.
        safe_kernel_id = _KERNEL_ID_TAG_RE.sub("-", kernel.id.lower())
        dockerfile = (
            f"FROM {base_image}\n"
            f"LABEL {_IMAGE_LABEL_CORE_INSTANCE}={self._core_instance_id}\n"
            f"LABEL {_IMAGE_LABEL_KERNEL_ID}={safe_kernel_id}\n"
            f"RUN {json.dumps(pip_args)}\n"
        )

        logger.info(
            "Building derived image '%s' on top of '%s' (%d extra packages)",
            derived_tag,
            base_image,
            len(kernel.packages),
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            (Path(tmpdir) / "Dockerfile").write_text(dockerfile)
            try:
                self._docker.images.build(
                    path=tmpdir,
                    tag=derived_tag,
                    rm=True,
                    forcerm=True,
                    pull=False,
                )
            except docker.errors.BuildError as exc:
                # Surface the failing pip output so the user can see why
                tail = "\n".join(
                    line.get("stream", "").rstrip()
                    for line in (exc.build_log or [])
                    if isinstance(line, dict) and line.get("stream")
                )[-20000:]
                raise RuntimeError(
                    f"Failed to bake packages into kernel image: {exc}\n{tail}"
                ) from exc
        return derived_tag

    def _resolve_installed_versions(
        self, image_tag: str, package_specs: list[str]
    ) -> list[ResolvedPackage]:
        """Run ``pip list`` inside the derived image and return the resolved
        version for each user-requested package.

        Best-effort: returns an empty list if the inspection fails so a single
        flaky read doesn't break kernel creation.
        """
        if not package_specs:
            return []
        wanted = {_spec_to_name(s): s for s in package_specs}
        try:
            output = self._docker.containers.run(
                image_tag,
                entrypoint=["pip"],
                command=["list", "--format=json", "--disable-pip-version-check"],
                remove=True,
                stdout=True,
                stderr=False,
            )
        except (docker.errors.ContainerError, docker.errors.APIError, docker.errors.DockerException) as exc:
            logger.warning("Could not inspect '%s' for resolved versions: %s", image_tag, exc)
            return []

        if isinstance(output, bytes):
            output = output.decode("utf-8", errors="replace")
        try:
            installed = json.loads(output)
        except (TypeError, json.JSONDecodeError) as exc:
            logger.warning("Could not parse pip list output from '%s': %s", image_tag, exc)
            return []

        installed_by_name = {
            re.sub(r"[-_.]+", "-", str(p.get("name", "")).lower()): str(p.get("version", ""))
            for p in installed
            if isinstance(p, dict)
        }

        # Preserve the order the user specified.
        resolved: list[ResolvedPackage] = []
        for spec in package_specs:
            name = _spec_to_name(spec)
            version = installed_by_name.get(name)
            if version:
                # Use the package's canonical name as reported by pip if we have it,
                # otherwise fall back to whatever the user typed before any specifier.
                display_name = next(
                    (
                        str(p["name"])
                        for p in installed
                        if isinstance(p, dict)
                        and re.sub(r"[-_.]+", "-", str(p.get("name", "")).lower()) == name
                    ),
                    wanted[name].split("[", 1)[0],
                )
                resolved.append(ResolvedPackage(name=display_name, version=version))
        return resolved

    def _remove_derived_image(self, kernel_id: str) -> None:
        """Best-effort removal of the per-kernel derived image."""
        tag = _derived_image_tag(kernel_id)
        try:
            self._docker.images.remove(tag, force=True)
            logger.info("Removed derived image '%s'", tag)
        except docker.errors.ImageNotFound:
            pass
        except docker.errors.APIError as exc:
            logger.warning("Could not remove derived image '%s': %s", tag, exc)

    def _remove_orphan_derived_images(self) -> None:
        """Remove derived kernel images whose owning kernel no longer exists.

        Called once at startup, after ``_restore_kernels_from_db``, so any
        derived image left over from a kernel that was deleted between core
        runs (e.g. core crashed between dropping the DB row and removing the
        image) gets reclaimed. Each orphan is hundreds of MB.

        Filters by the ``flowfile_core_instance`` label so two Core instances
        sharing a Docker daemon don't wipe each other's images. Uses the
        ``flowfile_kernel_id`` label as the source of truth — falls back to
        tag-name parsing only for legacy un-labelled images.
        """
        try:
            images = self._docker.images.list(
                filters={"label": f"{_IMAGE_LABEL_CORE_INSTANCE}={self._core_instance_id}"}
            )
        except (docker.errors.APIError, docker.errors.DockerException) as exc:
            logger.warning("Could not list images for orphan GC: %s", exc)
            return

        # Sanitisation is not reversible, so compare on the sanitised id.
        expected = {_KERNEL_ID_TAG_RE.sub("-", kid.lower()) for kid in self._kernels}
        prefix = "flowfile-kernel-derived-"

        for image in images:
            labels = image.labels or {}
            safe_id = labels.get(_IMAGE_LABEL_KERNEL_ID)
            if not safe_id:
                # Legacy image built before labelling existed — fall back to
                # the tag prefix. Skip if even the tag is missing.
                for tag in image.tags or []:
                    if tag.startswith(prefix):
                        safe_id = tag.split(":", 1)[0][len(prefix) :]
                        break
            if not safe_id or safe_id in expected:
                continue
            # Prefer the canonical tag; fall back to the image id when the
            # image has no remaining tags.
            target = next(
                (t for t in (image.tags or []) if t.startswith(prefix)),
                image.id,
            )
            try:
                self._docker.images.remove(target, force=True)
                logger.info("Removed orphan derived image '%s'", target)
            except docker.errors.ImageNotFound:
                pass
            except docker.errors.APIError as exc:
                logger.warning(
                    "Could not remove orphan derived image '%s': %s", target, exc
                )

    def _build_kernel_env(self, kernel_id: str, kernel: KernelInfo) -> dict[str, str]:
        """Build the environment dictionary for a kernel container.

        This centralizes all environment variables passed to kernel containers,
        including Core API connection, authentication, and persistence settings.
        """
        # Packages are pre-baked into the derived image when present, so the
        # entrypoint's KERNEL_PACKAGES install loop must be a no-op.
        env = {"KERNEL_PACKAGES": ""}
        # FLOWFILE_CORE_URL: how kernel reaches Core API from inside Docker.
        # In Docker-in-Docker mode the kernel is on the same Docker network
        # as core, so it can reach core by service name.
        if self._docker_network:
            default_core_url = "http://flowfile-core:63578"
        else:
            default_core_url = "http://host.docker.internal:63578"
        core_url = os.environ.get("FLOWFILE_CORE_URL", default_core_url)
        env["FLOWFILE_CORE_URL"] = core_url
        # FLOWFILE_INTERNAL_TOKEN: service-to-service auth for kernel → Core
        # Use get_internal_token() instead of reading env directly so that in
        # Electron mode the token is auto-generated before the kernel starts.
        try:
            from flowfile_core.auth.jwt import get_internal_token

            env["FLOWFILE_INTERNAL_TOKEN"] = get_internal_token()
        except (ValueError, ImportError):
            # Token not configured (e.g. local dev without env var) – skip
            internal_token = os.environ.get("FLOWFILE_INTERNAL_TOKEN")
            if internal_token:
                env["FLOWFILE_INTERNAL_TOKEN"] = internal_token
        # FLOWFILE_KERNEL_ID: pass kernel ID for lineage tracking
        env["FLOWFILE_KERNEL_ID"] = kernel_id
        # FLOWFILE_HOST_SHARED_DIR tells the kernel how to translate Core
        # API paths to container paths.  Only needed in local mode where the
        # shared dir is bind-mounted at /shared.  In Docker-in-Docker mode
        # the volume is mounted at the *same* path in core, worker and
        # kernel, so no translation is required and the variable is omitted.
        if not self._kernel_volume:
            env["FLOWFILE_HOST_SHARED_DIR"] = self._shared_volume
        # FLOWFILE_KERNEL_SHARED_DIR tells the kernel the absolute path of
        # the shared directory *as seen from inside the kernel container*.
        # Used by flowfile_ctx.get_shared_location() to resolve user file paths.
        env["FLOWFILE_KERNEL_SHARED_DIR"] = self.to_kernel_path(self._shared_volume)
        # Catalog tables: kernel needs to translate host catalog paths returned
        # by Core (e.g. ~/.flowfile/catalog_tables/orders_abc123/) to the
        # in-container path under the new mount. Mirror the shared-dir pattern:
        # ``FLOWFILE_HOST_CATALOG_TABLES_DIR`` is the host path (only set in
        # local mode), ``FLOWFILE_KERNEL_CATALOG_TABLES_DIR`` is what the kernel
        # sees inside its container.
        if not self._kernel_volume:
            env["FLOWFILE_HOST_CATALOG_TABLES_DIR"] = self._catalog_tables_dir
        env["FLOWFILE_KERNEL_CATALOG_TABLES_DIR"] = self.to_kernel_path(self._catalog_tables_dir)
        env["KERNEL_ID"] = kernel_id
        env["PERSISTENCE_ENABLED"] = "true" if kernel.persistence_enabled else "false"
        env["PERSISTENCE_PATH"] = self.to_kernel_path(os.path.join(self._shared_volume, "artifacts"))
        env["RECOVERY_MODE"] = kernel.recovery_mode.value
        return env

    async def create_kernel(self, config: KernelConfig, user_id: int) -> KernelInfo:
        if config.id in self._kernels:
            raise ValueError(f"Kernel '{config.id}' already exists")

        # In Docker-in-Docker mode we don't map host ports — kernels are
        # reached via container name on the shared Docker network.
        port = None if self._kernel_volume else self._allocate_port()
        # Validate image flavour and packages up-front so we fail before
        # persisting state or kicking off a long-running build.
        try:
            _resolve_image(config.image_flavour, config.custom_image, self._docker)
            _validate_packages(config.packages)
        except ValueError as exc:
            raise ValueError(str(exc)) from exc

        kernel = KernelInfo(
            id=config.id,
            name=config.name,
            state=KernelState.STOPPED,
            port=port,
            packages=config.packages,
            memory_gb=config.memory_gb,
            cpu_cores=config.cpu_cores,
            gpu=config.gpu,
            health_timeout=config.health_timeout,
            image_flavour=config.image_flavour,
            custom_image=config.custom_image,
            persistence_enabled=config.persistence_enabled,
            recovery_mode=config.recovery_mode,
        )

        # Pre-bake packages into a derived image so subsequent kernel starts
        # don't re-run pip in the entrypoint. Run in a thread to keep the
        # event loop responsive — image builds can take 30–60 s.
        if config.packages:
            try:
                derived_tag = await asyncio.to_thread(self._build_derived_image, kernel)
            except (RuntimeError, ValueError) as exc:
                raise ValueError(f"Failed to prepare kernel image: {exc}") from exc

            kernel.resolved_packages = await asyncio.to_thread(
                self._resolve_installed_versions, derived_tag, config.packages
            )

        self._kernels[config.id] = kernel
        self._kernel_owners[config.id] = user_id
        self._persist_kernel(kernel, user_id)
        # Auto-register a scratch FlowRegistration so artifacts published from
        # interactive cells have a valid producer. See ``_create_scratch_flow``.
        self._provision_scratch_flow(config.id, user_id)
        logger.info("Created kernel '%s' on port %s for user %d", config.id, port, user_id)
        return kernel

    async def start_kernel(self, kernel_id: str) -> KernelInfo:
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state == KernelState.IDLE:
            return kernel

        base_image = _resolve_image(kernel.image_flavour, kernel.custom_image, self._docker)

        # Verify the (base) kernel image exists before doing anything else.
        try:
            self._docker.images.get(base_image)
        except docker.errors.ImageNotFound:
            kernel.state = KernelState.ERROR
            kernel.error_message = (
                f"Docker image '{base_image}' not found. "
                f"Pull it with: docker pull {base_image} "
                "(or pick a different image flavour)."
            )
            raise RuntimeError(kernel.error_message) from None

        # If the kernel was created with extra packages, use the derived image
        # (built once at create_kernel time). Rebuild on the fly if a previous
        # core run was interrupted before the image landed.
        if kernel.packages:
            try:
                image = await asyncio.to_thread(self._build_derived_image, kernel)
            except (RuntimeError, ValueError) as exc:
                kernel.state = KernelState.ERROR
                kernel.error_message = str(exc)
                raise RuntimeError(kernel.error_message) from exc
        else:
            image = base_image

        kernel.image = image

        # Allocate a port if needed (local mode only, not needed for DinD)
        if kernel.port is None and not self._kernel_volume:
            kernel.port = self._allocate_port()

        kernel.state = KernelState.STARTING
        kernel.error_message = None

        try:
            env = self._build_kernel_env(kernel_id, kernel)
            run_kwargs = self._build_run_kwargs(kernel_id, kernel, env)
            container = self._docker.containers.run(image, **run_kwargs)
            kernel.container_id = container.id
            await self._wait_for_healthy(kernel_id, timeout=kernel.health_timeout)
            kernel.state = KernelState.IDLE
            logger.info("Kernel '%s' is idle (container %s, image %s)", kernel_id, container.short_id, image)
        except (docker.errors.DockerException, httpx.HTTPError, TimeoutError, OSError) as exc:
            kernel.state = KernelState.ERROR
            kernel.error_message = str(exc)
            logger.error("Failed to start kernel '%s': %s", kernel_id, exc)
            self._cleanup_container(kernel_id)
            raise

        return kernel

    def start_kernel_sync(self, kernel_id: str, flow_logger: FlowLogger | None = None) -> KernelInfo:
        """Synchronous version of start_kernel() for use from non-async code."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state == KernelState.IDLE:
            return kernel

        base_image = _resolve_image(kernel.image_flavour, kernel.custom_image, self._docker)

        try:
            self._docker.images.get(base_image)
        except docker.errors.ImageNotFound:
            kernel.state = KernelState.ERROR
            kernel.error_message = (
                f"Docker image '{base_image}' not found. "
                f"Pull it with: docker pull {base_image} "
                "(or pick a different image flavour)."
            )
            if flow_logger:
                flow_logger.error(kernel.error_message)
            raise RuntimeError(kernel.error_message) from None

        if kernel.packages:
            try:
                image = self._build_derived_image(kernel)
            except (RuntimeError, ValueError) as exc:
                kernel.state = KernelState.ERROR
                kernel.error_message = str(exc)
                if flow_logger:
                    flow_logger.error(kernel.error_message)
                raise RuntimeError(kernel.error_message) from exc
        else:
            image = base_image

        kernel.image = image

        if kernel.port is None and not self._kernel_volume:
            kernel.port = self._allocate_port()

        kernel.state = KernelState.STARTING
        kernel.error_message = None

        try:
            env = self._build_kernel_env(kernel_id, kernel)
            run_kwargs = self._build_run_kwargs(kernel_id, kernel, env)
            container = self._docker.containers.run(image, **run_kwargs)
            kernel.container_id = container.id
            self._wait_for_healthy_sync(kernel_id, timeout=kernel.health_timeout)
            kernel.state = KernelState.IDLE
            if flow_logger:
                flow_logger.info(
                    f"Kernel {kernel_id} is idle (container {container.short_id}, image {image})"
                )
        except (docker.errors.DockerException, httpx.HTTPError, TimeoutError, OSError) as exc:
            kernel.state = KernelState.ERROR
            kernel.error_message = str(exc)
            flow_logger.error(f"Failed to start kernel {kernel_id}: {exc}") if flow_logger else None
            self._cleanup_container(kernel_id)
            raise
        flow_logger.info(f"Kernel {kernel_id} started (container {container.short_id})") if flow_logger else None
        return kernel

    async def stop_kernel(self, kernel_id: str) -> None:
        kernel = self._get_kernel_or_raise(kernel_id)
        self._cleanup_container(kernel_id)
        kernel.state = KernelState.STOPPED
        kernel.container_id = None
        logger.info("Stopped kernel '%s'", kernel_id)

    async def update_kernel(self, kernel_id: str, packages: list[str]) -> KernelInfo:
        """Update a kernel's package list (the only field we currently allow editing).

        The kernel must be stopped — package edits trigger a rebuild of the
        derived image and we don't want to surprise users with a hot restart.
        """
        kernel = self._get_kernel_or_raise(kernel_id)

        if kernel.state in (
            KernelState.IDLE,
            KernelState.EXECUTING,
            KernelState.STARTING,
        ):
            raise RuntimeError(
                f"Cannot edit kernel '{kernel_id}' while it is {kernel.state.value}. "
                "Stop the kernel first."
            )

        _validate_packages(packages)

        old_packages = list(kernel.packages)
        old_resolved = list(kernel.resolved_packages)
        if packages == old_packages:
            return kernel

        kernel.packages = packages
        kernel.resolved_packages = []

        # Old derived image (if any) is now stale.
        if old_packages:
            self._remove_derived_image(kernel_id)

        # Build the new derived image; on failure, roll back the package list
        # so the persisted state matches what's actually on disk.
        if packages:
            try:
                derived_tag = await asyncio.to_thread(self._build_derived_image, kernel)
            except (RuntimeError, ValueError) as exc:
                kernel.packages = old_packages
                kernel.resolved_packages = old_resolved
                # Rebuild the previous derived image so the kernel is startable.
                if old_packages:
                    try:
                        await asyncio.to_thread(self._build_derived_image, kernel)
                    except Exception as restore_exc:  # noqa: BLE001
                        logger.warning(
                            "Could not restore previous derived image for '%s': %s",
                            kernel_id,
                            restore_exc,
                        )
                raise ValueError(f"Failed to update kernel image: {exc}") from exc

            kernel.resolved_packages = await asyncio.to_thread(
                self._resolve_installed_versions, derived_tag, packages
            )

        user_id = self._kernel_owners.get(kernel_id)
        if user_id is not None:
            self._persist_kernel(kernel, user_id)
        logger.info("Updated kernel '%s' packages → %s", kernel_id, packages)
        return kernel

    async def delete_kernel(self, kernel_id: str) -> None:
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state in (KernelState.IDLE, KernelState.EXECUTING):
            await self.stop_kernel(kernel_id)
        had_packages = bool(kernel.packages)
        # Delete the scratch FlowRegistration first so the FK ON DELETE SET NULL
        # doesn't fire as a side-effect; explicit deletion keeps the catalog
        # tidy. Tolerant of a missing row (manual removal, downgrade-then-upgrade).
        self._discard_scratch_flow(kernel_id)
        del self._kernels[kernel_id]
        self._kernel_owners.pop(kernel_id, None)
        self._remove_kernel_from_db(kernel_id)
        if had_packages:
            self._remove_derived_image(kernel_id)
        logger.info("Deleted kernel '%s'", kernel_id)

    def shutdown_all(self) -> None:
        """Stop and remove all running kernel containers. Called on core shutdown."""
        kernel_ids = list(self._kernels.keys())
        for kernel_id in kernel_ids:
            kernel = self._kernels.get(kernel_id)
            if kernel and kernel.state in (KernelState.IDLE, KernelState.EXECUTING, KernelState.STARTING):
                logger.info("Shutting down kernel '%s'", kernel_id)
                self._cleanup_container(kernel_id)
                kernel.state = KernelState.STOPPED
                kernel.container_id = None
        logger.info("All kernels have been shut down")

    # Execution

    def _check_oom_killed(self, kernel_id: str) -> bool:
        """Check if the kernel container was killed due to an out-of-memory condition."""
        kernel = self._kernels.get(kernel_id)
        if kernel is None or kernel.container_id is None:
            return False
        try:
            container = self._docker.containers.get(kernel.container_id)
            state = container.attrs.get("State", {})
            return state.get("OOMKilled", False)
        except (docker.errors.NotFound, docker.errors.APIError):
            return False

    async def execute(self, kernel_id: str, request: ExecuteRequest) -> ExecuteResult:
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            await self._ensure_running(kernel_id)

        # Fall back to the per-kernel scratch FlowRegistration so artifacts
        # published from interactive cells have a valid producer to point at.
        # Registered-flow execution paths set ``source_registration_id`` ahead
        # of time, so this short-circuits when it's already populated.
        if request.source_registration_id is None:
            request.source_registration_id = self.get_scratch_flow_id(kernel_id)

        kernel.state = KernelState.EXECUTING
        try:
            url = f"{self._kernel_url(kernel)}/execute"
            async with httpx.AsyncClient(timeout=httpx.Timeout(300.0)) as client:
                response = await client.post(url, json=request.model_dump())
                response.raise_for_status()
                return ExecuteResult(**response.json())
        except (httpx.HTTPError, OSError):
            if self._check_oom_killed(kernel_id):
                kernel.state = KernelState.ERROR
                kernel.error_message = "Kernel ran out of memory"
                oom_msg = (
                    f"Kernel ran out of memory. The container exceeded its {kernel.memory_gb} GB "
                    "memory limit and was terminated. Consider increasing the kernel's memory "
                    "allocation or reducing your data size."
                )
                return ExecuteResult(success=False, error=oom_msg)
            raise
        finally:
            # Only return to IDLE if we haven't been stopped/errored in the meantime
            if kernel.state == KernelState.EXECUTING:
                kernel.state = KernelState.IDLE

    def execute_sync(
        self,
        kernel_id: str,
        request: ExecuteRequest,
        flow_logger: FlowLogger | None = None,
        cancel_event: threading.Event | None = None,
    ) -> ExecuteResult:
        """Synchronous wrapper around execute() for use from non-async code.

        When *cancel_event* is provided the HTTP call runs in a daemon thread
        so the caller can be unblocked promptly when the event is set.
        """
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            self._ensure_running_sync(kernel_id, flow_logger=flow_logger)

        # See ``execute`` above — same scratch-flow fallback applies here.
        if request.source_registration_id is None:
            request.source_registration_id = self.get_scratch_flow_id(kernel_id)

        kernel.state = KernelState.EXECUTING
        try:
            url = f"{self._kernel_url(kernel)}/execute"

            if cancel_event is None:
                # Simple blocking call (no cancellation support)
                with httpx.Client(timeout=httpx.Timeout(300.0)) as client:
                    response = client.post(url, json=request.model_dump())
                    response.raise_for_status()
                    return ExecuteResult(**response.json())

            # --- cancellation-aware path ---
            result_holder: list[ExecuteResult | None] = [None]
            error_holder: list[BaseException | None] = [None]

            def _post() -> None:
                try:
                    with httpx.Client(timeout=httpx.Timeout(300.0)) as client:
                        resp = client.post(url, json=request.model_dump())
                        resp.raise_for_status()
                        result_holder[0] = ExecuteResult(**resp.json())
                except BaseException as exc:
                    error_holder[0] = exc

            t = threading.Thread(target=_post, daemon=True)
            t.start()

            while t.is_alive():
                t.join(timeout=0.5)
                if cancel_event.is_set():
                    # Best-effort interrupt, then return immediately
                    self.interrupt_execution_sync(kernel_id)
                    return ExecuteResult(success=False, error="Execution cancelled by user")

            if error_holder[0] is not None:
                raise error_holder[0]
            if result_holder[0] is not None:
                return result_holder[0]
            raise RuntimeError("Kernel execution returned no result")

        except (httpx.HTTPError, OSError):
            if self._check_oom_killed(kernel_id):
                kernel.state = KernelState.ERROR
                kernel.error_message = "Kernel ran out of memory"
                oom_msg = (
                    f"Kernel ran out of memory. The container exceeded its {kernel.memory_gb} GB "
                    "memory limit and was terminated. Consider increasing the kernel's memory "
                    "allocation or reducing your data size."
                )
                if flow_logger:
                    flow_logger.error(oom_msg)
                return ExecuteResult(success=False, error=oom_msg)
            raise
        finally:
            if kernel.state == KernelState.EXECUTING:
                kernel.state = KernelState.IDLE

    def interrupt_execution_sync(self, kernel_id: str) -> bool:
        """Interrupt running user code on a kernel.

        Tries the HTTP ``/interrupt`` endpoint first (works when the kernel
        runs user code in a background thread and keeps the event loop free).
        Falls back to sending ``SIGUSR1`` via Docker if the HTTP call fails
        (e.g. older kernel image, or the event loop is blocked).
        """
        kernel = self._kernels.get(kernel_id)
        if kernel is None or kernel.container_id is None:
            logger.warning("Cannot interrupt kernel '%s': not found or no container", kernel_id)
            return False
        if kernel.state != KernelState.EXECUTING:
            return False

        # --- Try HTTP /interrupt (preferred) ---
        should_try_http = self._docker_network is not None or kernel.port is not None
        if should_try_http:
            try:
                url = f"{self._kernel_url(kernel)}/interrupt"
                with httpx.Client(timeout=httpx.Timeout(5.0)) as client:
                    resp = client.post(url)
                    if resp.status_code == 200:
                        logger.info("Interrupted kernel '%s' via HTTP", kernel_id)
                        return True
            except (httpx.HTTPError, OSError):
                logger.debug("HTTP /interrupt failed for kernel '%s', falling back to SIGUSR1", kernel_id)

        # --- Fallback: Docker SIGUSR1 ---
        try:
            container = self._docker.containers.get(kernel.container_id)
            container.kill(signal="SIGUSR1")
            logger.info("Sent SIGUSR1 to kernel '%s' (container %s)", kernel_id, kernel.container_id[:12])
            return True
        except docker.errors.NotFound:
            logger.warning("Container for kernel '%s' not found", kernel_id)
            return False
        except (docker.errors.APIError, docker.errors.DockerException) as exc:
            logger.error("Failed to send SIGUSR1 to kernel '%s': %s", kernel_id, exc)
            return False

    async def interrupt_execution(self, kernel_id: str) -> bool:
        """Async wrapper around :meth:`interrupt_execution_sync`."""
        return self.interrupt_execution_sync(kernel_id)

    async def clear_artifacts(self, kernel_id: str) -> None:
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            await self._ensure_running(kernel_id)

        url = f"{self._kernel_url(kernel)}/clear"
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            response = await client.post(url)
            response.raise_for_status()

    def clear_artifacts_sync(self, kernel_id: str) -> None:
        """Synchronous wrapper around clear_artifacts() for use from non-async code."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            self._ensure_running_sync(kernel_id)

        url = f"{self._kernel_url(kernel)}/clear"
        with httpx.Client(timeout=httpx.Timeout(30.0)) as client:
            response = client.post(url)
            response.raise_for_status()

    async def clear_node_artifacts(
        self,
        kernel_id: str,
        node_ids: list[int],
        flow_id: int | None = None,
    ) -> ClearNodeArtifactsResult:
        """Clear only artifacts published by the given node IDs."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            await self._ensure_running(kernel_id)

        url = f"{self._kernel_url(kernel)}/clear_node_artifacts"
        payload: dict = {"node_ids": node_ids}
        if flow_id is not None:
            payload["flow_id"] = flow_id
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            response = await client.post(url, json=payload)
            response.raise_for_status()
            return ClearNodeArtifactsResult(**response.json())

    def clear_node_artifacts_sync(
        self,
        kernel_id: str,
        node_ids: list[int],
        flow_id: int | None = None,
        flow_logger: FlowLogger | None = None,
    ) -> ClearNodeArtifactsResult:
        """Synchronous wrapper for clearing artifacts by node IDs."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            self._ensure_running_sync(kernel_id, flow_logger=flow_logger)

        url = f"{self._kernel_url(kernel)}/clear_node_artifacts"
        payload: dict = {"node_ids": node_ids}
        if flow_id is not None:
            payload["flow_id"] = flow_id
        with httpx.Client(timeout=httpx.Timeout(30.0)) as client:
            response = client.post(url, json=payload)
            response.raise_for_status()
            return ClearNodeArtifactsResult(**response.json())

    async def clear_namespace(self, kernel_id: str, flow_id: int) -> None:
        """Clear the execution namespace for a flow (variables, imports, etc.)."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            await self._ensure_running(kernel_id)

        url = f"{self._kernel_url(kernel)}/clear_namespace"
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            response = await client.post(url, params={"flow_id": flow_id})
            response.raise_for_status()

    async def get_node_artifacts(self, kernel_id: str, node_id: int) -> dict:
        """Get artifacts published by a specific node."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            await self._ensure_running(kernel_id)

        url = f"{self._kernel_url(kernel)}/artifacts/node/{node_id}"
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.json()

    async def get_display_outputs(self, kernel_id: str, flow_id: int, node_id: int) -> list[dict]:
        """Retrieve stored display outputs from the last execution of a node."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            await self._ensure_running(kernel_id)

        url = f"{self._kernel_url(kernel)}/display_outputs"
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            response = await client.get(url, params={"flow_id": flow_id, "node_id": node_id})
            response.raise_for_status()
            return response.json()

    # Artifact Persistence & Recovery

    async def recover_artifacts(self, kernel_id: str) -> RecoveryStatus:
        """Trigger manual artifact recovery on a running kernel."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            raise RuntimeError(f"Kernel '{kernel_id}' is not running (state: {kernel.state})")

        url = f"{self._kernel_url(kernel)}/recover"
        async with httpx.AsyncClient(timeout=httpx.Timeout(120.0)) as client:
            response = await client.post(url)
            response.raise_for_status()
            return RecoveryStatus(**response.json())

    async def get_recovery_status(self, kernel_id: str) -> RecoveryStatus:
        """Get the current recovery status of a kernel."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            raise RuntimeError(f"Kernel '{kernel_id}' is not running (state: {kernel.state})")

        url = f"{self._kernel_url(kernel)}/recovery-status"
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            response = await client.get(url)
            response.raise_for_status()
            return RecoveryStatus(**response.json())

    async def cleanup_artifacts(self, kernel_id: str, request: CleanupRequest) -> CleanupResult:
        """Clean up old persisted artifacts on a kernel."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            raise RuntimeError(f"Kernel '{kernel_id}' is not running (state: {kernel.state})")

        url = f"{self._kernel_url(kernel)}/cleanup"
        async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
            response = await client.post(url, json=request.model_dump())
            response.raise_for_status()
            return CleanupResult(**response.json())

    async def get_persistence_info(self, kernel_id: str) -> ArtifactPersistenceInfo:
        """Get persistence configuration and stats for a kernel."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            raise RuntimeError(f"Kernel '{kernel_id}' is not running (state: {kernel.state})")

        url = f"{self._kernel_url(kernel)}/persistence"
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            response = await client.get(url)
            response.raise_for_status()
            return ArtifactPersistenceInfo(**response.json())

    async def get_memory_stats(self, kernel_id: str) -> KernelMemoryInfo:
        """Get current memory usage from a running kernel container."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            raise RuntimeError(f"Kernel '{kernel_id}' is not running (state: {kernel.state})")

        url = f"{self._kernel_url(kernel)}/memory"
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(5.0)) as client:
                response = await client.get(url)
                response.raise_for_status()
                return KernelMemoryInfo(**response.json())
        except (httpx.HTTPError, OSError) as exc:
            raise RuntimeError(f"Could not retrieve memory stats from kernel '{kernel_id}': {exc}") from exc

    async def list_kernel_artifacts(self, kernel_id: str) -> list:
        """List all artifacts in a running kernel."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state not in (KernelState.IDLE, KernelState.EXECUTING):
            raise RuntimeError(f"Kernel '{kernel_id}' is not running (state: {kernel.state})")

        url = f"{self._kernel_url(kernel)}/artifacts"
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.json()

    # Queries

    async def list_kernels(self, user_id: int | None = None) -> list[KernelInfo]:
        if user_id is not None:
            return [k for kid, k in self._kernels.items() if self._kernel_owners.get(kid) == user_id]
        return list(self._kernels.values())

    async def get_kernel(self, kernel_id: str) -> KernelInfo | None:
        return self._kernels.get(kernel_id)

    def get_kernel_owner(self, kernel_id: str) -> int | None:
        return self._kernel_owners.get(kernel_id)

    # Internal helpers

    def _get_kernel_or_raise(self, kernel_id: str) -> KernelInfo:
        kernel = self._kernels.get(kernel_id)
        if kernel is None:
            raise KeyError(f"Kernel '{kernel_id}' not found")
        return kernel

    async def _ensure_running(self, kernel_id: str) -> None:
        """Restart the kernel if it is STOPPED or ERROR, then wait until IDLE."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state in (KernelState.IDLE, KernelState.EXECUTING):
            return
        if kernel.state in (KernelState.STOPPED, KernelState.ERROR):
            logger.info(
                "Kernel '%s' is %s, attempting automatic restart...",
                kernel_id,
                kernel.state.value,
            )
            self._cleanup_container(kernel_id)
            kernel.container_id = None
            await self.start_kernel(kernel_id)
            return
        # STARTING — wait for it to finish
        if kernel.state == KernelState.STARTING:
            logger.info("Kernel '%s' is starting, waiting for it to become ready...", kernel_id)
            await self._wait_for_healthy(kernel_id)
            kernel.state = KernelState.IDLE

    def _ensure_running_sync(self, kernel_id: str, flow_logger: FlowLogger | None = None) -> None:
        """Synchronous version of _ensure_running."""
        kernel = self._get_kernel_or_raise(kernel_id)
        if kernel.state in (KernelState.IDLE, KernelState.EXECUTING):
            return
        if kernel.state in (KernelState.STOPPED, KernelState.ERROR):
            msg = f"Kernel '{kernel_id}' is {kernel.state.value}, attempting automatic restart..."
            logger.info(msg)
            if flow_logger:
                flow_logger.info(msg)
            self._cleanup_container(kernel_id)
            kernel.container_id = None
            self.start_kernel_sync(kernel_id, flow_logger=flow_logger)
            return
        # STARTING — wait for it to finish
        if kernel.state == KernelState.STARTING:
            logger.info("Kernel '%s' is starting, waiting for it to become ready...", kernel_id)
            self._wait_for_healthy_sync(kernel_id)
            kernel.state = KernelState.IDLE

    def _cleanup_container(self, kernel_id: str) -> None:
        kernel = self._kernels.get(kernel_id)
        if kernel is None or kernel.container_id is None:
            return
        try:
            container = self._docker.containers.get(kernel.container_id)
            container.stop(timeout=10)
            container.remove(force=True)
        except docker.errors.NotFound:
            pass
        except (docker.errors.APIError, docker.errors.DockerException) as exc:
            logger.warning("Error cleaning up container for kernel '%s': %s", kernel_id, exc)

    async def _wait_for_healthy(self, kernel_id: str, timeout: int = _HEALTH_TIMEOUT) -> None:
        kernel = self._get_kernel_or_raise(kernel_id)
        url = f"{self._kernel_url(kernel)}/health"
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout

        while loop.time() < deadline:
            try:
                async with httpx.AsyncClient(timeout=httpx.Timeout(5.0)) as client:
                    response = await client.get(url)
                    if response.status_code == 200:
                        data = response.json()
                        kernel.kernel_version = data.get("version")
                        return
            except (httpx.HTTPError, OSError) as exc:
                logger.debug("Health poll for kernel '%s' failed: %s", kernel_id, exc)
            await asyncio.sleep(_HEALTH_POLL_INTERVAL)

        raise TimeoutError(f"Kernel '{kernel_id}' did not become healthy within {timeout}s")

    def _wait_for_healthy_sync(self, kernel_id: str, timeout: int = _HEALTH_TIMEOUT) -> None:
        """Synchronous version of _wait_for_healthy."""
        kernel = self._get_kernel_or_raise(kernel_id)
        url = f"{self._kernel_url(kernel)}/health"
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            try:
                with httpx.Client(timeout=httpx.Timeout(5.0)) as client:
                    response = client.get(url)
                    if response.status_code == 200:
                        data = response.json()
                        kernel.kernel_version = data.get("version")
                        return
            except (httpx.HTTPError, OSError) as exc:
                logger.debug("Health poll for kernel '%s' failed: %s", kernel_id, exc)
            time.sleep(_HEALTH_POLL_INTERVAL)

        raise TimeoutError(f"Kernel '{kernel_id}' did not become healthy within {timeout}s")
