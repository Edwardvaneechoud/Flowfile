"""Build the secret manifest (names + ``required_by`` only -- never values)."""

from __future__ import annotations

from flowfile_core.workspace.exporters import ExportBundle
from flowfile_core.workspace.layout import ProjectLayout
from flowfile_core.workspace.normalize import canonical_yaml_dump


def export_secret_manifest(secret_refs: dict[str, list[str]], layout: ProjectLayout) -> ExportBundle:
    """Emit ``secrets/secrets.manifest.yaml`` from collected secret references.

    ``secret_refs`` is aggregated by the connection exporter: secret name ->
    rel-paths that reference it. The manifest carries only names + ``required_by``
    so a fresh clone knows which env vars / ``.env`` keys to supply.
    """
    bundle = ExportBundle()
    secrets = [
        {"name": name, "required_by": sorted(set(paths))}
        for name, paths in sorted(secret_refs.items())
    ]
    rel = layout.rel(layout.secrets_manifest_path)
    bundle.artifacts[rel] = canonical_yaml_dump({"secrets": secrets})
    return bundle
