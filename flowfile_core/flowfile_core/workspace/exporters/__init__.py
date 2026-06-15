"""DB -> files exporters. Each returns an :class:`ExportBundle`."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ExportBundle:
    """The artifacts an exporter produced, plus collected secret references.

    * ``artifacts`` maps a project-relative path to its canonical text content.
    * ``secret_refs`` maps a secret *name* to the rel-paths that reference it
      (used to build the secret manifest's ``required_by``).
    """

    artifacts: dict[str, str] = field(default_factory=dict)
    secret_refs: dict[str, list[str]] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)

    def merge(self, other: ExportBundle) -> ExportBundle:
        self.artifacts.update(other.artifacts)
        for name, paths in other.secret_refs.items():
            self.secret_refs.setdefault(name, []).extend(paths)
        self.warnings.extend(other.warnings)
        return self


def drop_none(data: dict) -> dict:
    """Drop ``None`` values so optional fields don't clutter the export."""
    return {k: v for k, v in data.items() if v is not None}
