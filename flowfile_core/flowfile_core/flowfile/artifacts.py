"""Artifact context tracking for the FlowGraph.

This module provides metadata tracking for Python artifacts that are
published and consumed by ``python_script`` nodes running on kernel
containers.  The actual objects remain in kernel memory; this module
only tracks *references* (name, source node, type info, etc.) so the
FlowGraph can reason about artifact availability across the DAG.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ArtifactRef:
    """Metadata reference to an artifact (not the object itself)."""

    name: str
    source_node_id: int
    kernel_id: str = ""
    type_name: str = ""
    module: str = ""
    size_bytes: int = 0
    created_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "source_node_id": self.source_node_id,
            "kernel_id": self.kernel_id,
            "type_name": self.type_name,
            "module": self.module,
            "size_bytes": self.size_bytes,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class NodeArtifactState:
    """Artifact state for a single node."""

    published: list[ArtifactRef] = field(default_factory=list)
    available: dict[str, ArtifactRef] = field(default_factory=dict)
    consumed: list[str] = field(default_factory=list)
    deleted: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "published": [r.to_dict() for r in self.published],
            "available": {k: v.to_dict() for k, v in self.available.items()},
            "consumed": list(self.consumed),
            "deleted": list(self.deleted),
        }


class ArtifactContext:
    """Tracks artifact availability across the flow graph.

    This is a metadata-only tracker.  Actual Python objects stay inside
    the kernel container's ``ArtifactStore``.
    """

    def __init__(self) -> None:
        self._node_states: dict[int, NodeArtifactState] = {}
        self._kernel_artifacts: dict[str, dict[str, ArtifactRef]] = {}
        # Reverse index: (kernel_id, artifact_name) → set of node_ids that
        # published it.  Avoids O(N) scan in record_deleted / clear_kernel.
        self._publisher_index: dict[tuple[str, str], set[int]] = {}

    # ------------------------------------------------------------------
    # Recording
    # ------------------------------------------------------------------

    def record_published(
        self,
        node_id: int,
        kernel_id: str,
        artifacts: list[dict[str, Any] | str],
    ) -> list[ArtifactRef]:
        """Record artifacts published by *node_id*.

        ``artifacts`` may be a list of dicts (with at least a ``"name"`` key)
        or a plain list of artifact name strings.

        Returns the created :class:`ArtifactRef` objects.
        """
        state = self._get_or_create_state(node_id)
        refs: list[ArtifactRef] = []
        for item in artifacts:
            if isinstance(item, str):
                item = {"name": item}
            ref = ArtifactRef(
                name=item["name"],
                source_node_id=node_id,
                kernel_id=kernel_id,
                type_name=item.get("type_name", ""),
                module=item.get("module", ""),
                size_bytes=item.get("size_bytes", 0),
                created_at=datetime.now(timezone.utc),
            )
            refs.append(ref)
            state.published.append(ref)

            # Update the per-kernel index
            kernel_map = self._kernel_artifacts.setdefault(kernel_id, {})
            kernel_map[ref.name] = ref

            # Update the reverse index
            key = (kernel_id, ref.name)
            self._publisher_index.setdefault(key, set()).add(node_id)

        logger.debug(
            "Node %s published %d artifact(s) on kernel '%s': %s",
            node_id,
            len(refs),
            kernel_id,
            [r.name for r in refs],
        )
        return refs

    def record_consumed(self, node_id: int, artifact_names: list[str]) -> None:
        """Record that *node_id* consumed (read) the given artifact names."""
        state = self._get_or_create_state(node_id)
        state.consumed.extend(artifact_names)

    def record_deleted(
        self,
        node_id: int,
        kernel_id: str,
        artifact_names: list[str],
    ) -> None:
        """Record that *node_id* deleted the given artifacts from *kernel_id*.

        Removes the artifacts from the kernel index and from published
        lists of the publishing nodes (looked up via reverse index).
        """
        state = self._get_or_create_state(node_id)
        state.deleted.extend(artifact_names)

        kernel_map = self._kernel_artifacts.get(kernel_id, {})
        for name in artifact_names:
            kernel_map.pop(name, None)

            # Use the reverse index to update only the affected nodes
            key = (kernel_id, name)
            publisher_ids = self._publisher_index.pop(key, set())
            for pid in publisher_ids:
                ns = self._node_states.get(pid)
                if ns is not None:
                    ns.published = [
                        r for r in ns.published
                        if not (r.kernel_id == kernel_id and r.name == name)
                    ]

        logger.debug(
            "Node %s deleted %d artifact(s) on kernel '%s': %s",
            node_id,
            len(artifact_names),
            kernel_id,
            artifact_names,
        )

    # ------------------------------------------------------------------
    # Availability computation
    # ------------------------------------------------------------------

    def compute_available(
        self,
        node_id: int,
        kernel_id: str,
        upstream_node_ids: list[int],
    ) -> dict[str, ArtifactRef]:
        """Compute which artifacts are available to *node_id*.

        An artifact is available if it was published by an upstream node
        (direct or transitive) that used the **same** ``kernel_id``.

        The result is stored on the node's :class:`NodeArtifactState` and
        also returned.
        """
        available: dict[str, ArtifactRef] = {}
        for uid in upstream_node_ids:
            upstream_state = self._node_states.get(uid)
            if upstream_state is None:
                continue
            for ref in upstream_state.published:
                if ref.kernel_id == kernel_id:
                    available[ref.name] = ref

        state = self._get_or_create_state(node_id)
        state.available = available

        logger.debug(
            "Node %s has %d available artifact(s): %s",
            node_id,
            len(available),
            list(available.keys()),
        )
        return available

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def get_published_by_node(self, node_id: int) -> list[ArtifactRef]:
        """Return artifacts published by *node_id* (empty list if unknown)."""
        state = self._node_states.get(node_id)
        if state is None:
            return []
        return list(state.published)

    def get_available_for_node(self, node_id: int) -> dict[str, ArtifactRef]:
        """Return the availability map for *node_id* (empty dict if unknown)."""
        state = self._node_states.get(node_id)
        if state is None:
            return {}
        return dict(state.available)

    def get_kernel_artifacts(self, kernel_id: str) -> dict[str, ArtifactRef]:
        """Return all known artifacts for a given kernel."""
        return dict(self._kernel_artifacts.get(kernel_id, {}))

    def get_all_artifacts(self) -> dict[str, ArtifactRef]:
        """Return every tracked artifact across all kernels."""
        result: dict[str, ArtifactRef] = {}
        for kernel_map in self._kernel_artifacts.values():
            result.update(kernel_map)
        return result

    # ------------------------------------------------------------------
    # Clearing
    # ------------------------------------------------------------------

    def clear_kernel(self, kernel_id: str) -> None:
        """Remove tracking for a specific kernel.

        Also removes the corresponding published refs from node states
        and cleans up the reverse index.
        """
        # Clean reverse index entries for this kernel
        keys_to_remove = [k for k in self._publisher_index if k[0] == kernel_id]
        for k in keys_to_remove:
            del self._publisher_index[k]

        self._kernel_artifacts.pop(kernel_id, None)
        for state in self._node_states.values():
            state.published = [r for r in state.published if r.kernel_id != kernel_id]
            state.available = {
                k: v for k, v in state.available.items() if v.kernel_id != kernel_id
            }

    def clear_all(self) -> None:
        """Remove all tracking data."""
        self._node_states.clear()
        self._kernel_artifacts.clear()
        self._publisher_index.clear()

    def snapshot_node_states(self) -> dict[int, NodeArtifactState]:
        """Return a shallow copy of the current per-node states.

        Useful for saving state before ``clear_all()`` so cached
        (skipped) nodes can have their artifact state restored afterwards.
        """
        return dict(self._node_states)

    def restore_node_state(self, node_id: int, state: NodeArtifactState) -> None:
        """Re-insert a previously-snapshotted node state.

        Rebuilds the kernel index and reverse index entries for every
        published artifact in *state*.
        """
        self._node_states[node_id] = state
        for ref in state.published:
            kernel_map = self._kernel_artifacts.setdefault(ref.kernel_id, {})
            kernel_map[ref.name] = ref
            key = (ref.kernel_id, ref.name)
            self._publisher_index.setdefault(key, set()).add(node_id)

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serialisable summary of the context."""
        return {
            "nodes": {
                str(nid): state.to_dict() for nid, state in self._node_states.items()
            },
            "kernels": {
                kid: {name: ref.to_dict() for name, ref in refs.items()}
                for kid, refs in self._kernel_artifacts.items()
            },
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_or_create_state(self, node_id: int) -> NodeArtifactState:
        if node_id not in self._node_states:
            self._node_states[node_id] = NodeArtifactState()
        return self._node_states[node_id]
