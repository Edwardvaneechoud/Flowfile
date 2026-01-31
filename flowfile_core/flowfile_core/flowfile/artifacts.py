"""
Artifact tracking for FlowFile kernel system.

Tracks which artifacts are published by which nodes and computes
artifact availability based on graph topology.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph import FlowGraph


@dataclass
class ArtifactRef:
    """Reference to an artifact (metadata only, not the actual object)."""

    name: str
    source_node_id: int
    kernel_id: str = ""
    type_name: str = ""
    module: str = ""
    size_bytes: int = 0
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __hash__(self) -> int:
        return hash((self.name, self.source_node_id, self.kernel_id))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ArtifactRef):
            return False
        return (
            self.name == other.name
            and self.source_node_id == other.source_node_id
            and self.kernel_id == other.kernel_id
        )


@dataclass
class NodeArtifactState:
    """Artifact state for a single node."""

    published: list[ArtifactRef] = field(default_factory=list)
    available: dict[str, ArtifactRef] = field(default_factory=dict)
    consumed: list[str] = field(default_factory=list)


class ArtifactContext:
    """
    Tracks artifact availability across the flow graph.

    Responsibilities:
    1. Record what each node publishes after execution
    2. Compute what artifacts are available to each node based on graph topology
    3. Provide queries for UI visualization

    Usage:
        context = ArtifactContext()

        # Before node executes
        available = context.compute_available(node_id, kernel_id, upstream_ids)

        # After node executes
        context.record_published(node_id, kernel_id, result.artifacts_published)

        # Query
        context.get_published_by_node(node_id)
        context.get_available_for_node(node_id)
    """

    def __init__(self) -> None:
        self._node_states: dict[int, NodeArtifactState] = {}
        self._kernel_artifacts: dict[str, dict[str, ArtifactRef]] = {}

    def _get_or_create_state(self, node_id: int) -> NodeArtifactState:
        if node_id not in self._node_states:
            self._node_states[node_id] = NodeArtifactState()
        return self._node_states[node_id]

    # -------------------------------------------------------------------------
    # Recording (called during execution)
    # -------------------------------------------------------------------------

    def record_published(
        self,
        node_id: int,
        kernel_id: str,
        artifacts: list[dict],
    ) -> list[ArtifactRef]:
        """
        Record artifacts published by a node after execution.

        Args:
            node_id: The node that published the artifacts
            kernel_id: The kernel where artifacts are stored
            artifacts: List of artifact metadata dicts from ExecuteResult
                       Expected keys: name, type_name (optional), module (optional)

        Returns:
            List of ArtifactRef objects created
        """
        state = self._get_or_create_state(node_id)
        refs: list[ArtifactRef] = []

        for art in artifacts:
            ref = ArtifactRef(
                name=art.get("name", art) if isinstance(art, dict) else str(art),
                source_node_id=node_id,
                kernel_id=kernel_id,
                type_name=art.get("type_name", "") if isinstance(art, dict) else "",
                module=art.get("module", "") if isinstance(art, dict) else "",
                size_bytes=art.get("size_bytes", 0) if isinstance(art, dict) else 0,
            )
            state.published.append(ref)
            refs.append(ref)

            # Track at kernel level
            if kernel_id not in self._kernel_artifacts:
                self._kernel_artifacts[kernel_id] = {}
            self._kernel_artifacts[kernel_id][ref.name] = ref

        return refs

    def record_consumed(self, node_id: int, artifact_names: list[str]) -> None:
        """Record which artifacts a node actually read during execution."""
        state = self._get_or_create_state(node_id)
        state.consumed.extend(artifact_names)

    # -------------------------------------------------------------------------
    # Availability computation
    # -------------------------------------------------------------------------

    def compute_available(
        self,
        node_id: int,
        kernel_id: str,
        upstream_node_ids: list[int],
    ) -> dict[str, ArtifactRef]:
        """
        Compute and store what artifacts are available to a node.

        An artifact is available if it was published by an upstream node
        on the same kernel.

        Args:
            node_id: The node to compute availability for
            kernel_id: The kernel this node will execute on
            upstream_node_ids: IDs of all upstream nodes (direct and transitive)

        Returns:
            Dict mapping artifact name to ArtifactRef
        """
        available: dict[str, ArtifactRef] = {}

        for upstream_id in upstream_node_ids:
            upstream_state = self._node_states.get(upstream_id)
            if not upstream_state:
                continue

            # Add artifacts published by upstream (if same kernel)
            for ref in upstream_state.published:
                if ref.kernel_id == kernel_id:
                    available[ref.name] = ref

            # Add artifacts that were available to upstream (transitive)
            for name, ref in upstream_state.available.items():
                if ref.kernel_id == kernel_id:
                    available[name] = ref

        # Store on this node
        state = self._get_or_create_state(node_id)
        state.available = available

        return available

    # -------------------------------------------------------------------------
    # Queries
    # -------------------------------------------------------------------------

    def get_published_by_node(self, node_id: int) -> list[ArtifactRef]:
        """Get artifacts published by a specific node."""
        state = self._node_states.get(node_id)
        return list(state.published) if state else []

    def get_available_for_node(self, node_id: int) -> dict[str, ArtifactRef]:
        """Get artifacts available to a specific node."""
        state = self._node_states.get(node_id)
        return dict(state.available) if state else {}

    def get_consumed_by_node(self, node_id: int) -> list[str]:
        """Get artifact names that a node actually read."""
        state = self._node_states.get(node_id)
        return list(state.consumed) if state else []

    def get_kernel_artifacts(self, kernel_id: str) -> dict[str, ArtifactRef]:
        """Get all artifacts currently tracked for a kernel."""
        return dict(self._kernel_artifacts.get(kernel_id, {}))

    def get_all_artifacts(self) -> dict[str, ArtifactRef]:
        """Get all artifacts across all kernels."""
        all_artifacts: dict[str, ArtifactRef] = {}
        for kernel_artifacts in self._kernel_artifacts.values():
            all_artifacts.update(kernel_artifacts)
        return all_artifacts

    # -------------------------------------------------------------------------
    # Clearing
    # -------------------------------------------------------------------------

    def clear_kernel(self, kernel_id: str) -> None:
        """Clear artifact tracking for a specific kernel."""
        self._kernel_artifacts.pop(kernel_id, None)

    def clear_all(self) -> None:
        """Clear all artifact tracking (call at flow start)."""
        self._node_states.clear()
        self._kernel_artifacts.clear()

    # -------------------------------------------------------------------------
    # Serialization (for UI/debugging)
    # -------------------------------------------------------------------------

    def to_dict(self) -> dict:
        """Serialize context for debugging or API responses."""
        return {
            "nodes": {
                node_id: {
                    "published": [
                        {"name": r.name, "type": r.type_name, "source": r.source_node_id}
                        for r in state.published
                    ],
                    "available": list(state.available.keys()),
                    "consumed": state.consumed,
                }
                for node_id, state in self._node_states.items()
            },
            "kernels": {
                kernel_id: list(artifacts.keys())
                for kernel_id, artifacts in self._kernel_artifacts.items()
            },
        }
