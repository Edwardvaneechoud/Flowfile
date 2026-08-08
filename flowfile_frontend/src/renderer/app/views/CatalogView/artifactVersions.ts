// Pure helpers for the artifact version table. Kept Vue-free so the paging,
// latest-version and prune rules stay unit-testable (vitest runs in node env).
import type { ArtifactVersionInfo } from "../../types";

export const ARTIFACT_VERSIONS_PAGE_SIZE = 25;

/**
 * Namespace-qualified reference (`catalog.schema::name`) understood by the
 * by-name artifact routes. Falls back to the bare name when the artifact has no
 * namespace path, which the backend still accepts.
 */
export function artifactRef(name: string, namespacePath: string[]): string {
  return namespacePath.length > 0 ? `${namespacePath.join(".")}::${name}` : name;
}

export function totalPages(total: number, pageSize = ARTIFACT_VERSIONS_PAGE_SIZE): number {
  if (pageSize <= 0) return 1;
  return Math.max(1, Math.ceil(total / pageSize));
}

/** Page to land on once rows have been removed — steps back when the current page is gone. */
export function pageAfterDelete(
  page: number,
  total: number,
  pageSize = ARTIFACT_VERSIONS_PAGE_SIZE,
): number {
  return Math.min(Math.max(1, page), totalPages(total, pageSize));
}

/**
 * Highest known version: the artifact's own latest plus anything on the loaded
 * page. Row-id equality is not usable — it goes stale the moment a promote mints
 * a new max version.
 */
export function latestVersionNumber(
  artifactVersion: number,
  versions: { version: number }[],
): number {
  return versions.reduce((max, v) => Math.max(max, v.version), artifactVersion);
}

export function isLatest(version: number, latestVersion: number): boolean {
  return version === latestVersion;
}

/** Versions a prune would remove, keeping the newest `keep` (never fewer than one). */
export function pruneCandidates(
  versions: ArtifactVersionInfo[],
  keep: number,
): { toDelete: ArtifactVersionInfo[]; freedBytes: number } {
  const kept = Math.max(1, Math.floor(keep));
  const sorted = [...versions].sort((a, b) => b.version - a.version);
  const toDelete = sorted.slice(kept);
  const freedBytes = toDelete.reduce((sum, v) => sum + (v.size_bytes ?? 0), 0);
  return { toDelete, freedBytes };
}
