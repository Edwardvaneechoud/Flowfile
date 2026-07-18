// Glob-based type filtering for artifact selects. Mirrors the backend
// AvailableArtifacts type_filter semantics: `*` matches any run of chars,
// every other char is literal, and any glob matching wins.

import type { ArtifactOption, GlobalArtifactOption } from "../interface";

function globToRegExp(glob: string): RegExp {
  const escaped = glob.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const pattern = escaped.replace(/\\\*/g, ".*");
  return new RegExp(`^${pattern}$`);
}

export function matchesTypeFilter(fullType: string, globs?: string[]): boolean {
  if (!globs || globs.length === 0) return true;
  return globs.some((glob) => globToRegExp(glob).test(fullType));
}

export function formatTypeShort(fullType: string): string {
  const idx = fullType.lastIndexOf(".");
  return idx >= 0 ? fullType.slice(idx + 1) : fullType;
}

// [value, label] pair; value is always the bare artifact name.
export type ArtifactSelectOption = [string, string];

export function upstreamArtifactOptions(
  artifacts: ArtifactOption[],
  typeFilter?: string[],
): ArtifactSelectOption[] {
  return artifacts
    .filter((a) => matchesTypeFilter([a.module, a.type_name].filter(Boolean).join("."), typeFilter))
    .map((a): ArtifactSelectOption => [a.name, a.type_name ? `${a.name} (${a.type_name})` : a.name]);
}

export function globalArtifactOptions(
  artifacts: GlobalArtifactOption[],
  typeFilter?: string[],
): ArtifactSelectOption[] {
  return artifacts
    .filter((a) => matchesTypeFilter(a.python_type ?? "", typeFilter))
    .map((a): ArtifactSelectOption => [
      a.name,
      `${a.name} (v${a.version}${a.python_type ? " · " + formatTypeShort(a.python_type) : ""})`,
    ]);
}

// scope "all": upstream options first, then global options whose bare name is not
// already present (dedupe by name, upstream entry wins). Type filter applies to both.
export function buildArtifactOptions(
  scope: "upstream" | "global" | "all" | undefined,
  upstream: ArtifactOption[],
  global: GlobalArtifactOption[],
  typeFilter?: string[],
): ArtifactSelectOption[] {
  const resolved = scope ?? "upstream";
  if (resolved === "global") {
    return globalArtifactOptions(global, typeFilter);
  }
  const upstreamOpts = upstreamArtifactOptions(upstream, typeFilter);
  if (resolved === "upstream") {
    return upstreamOpts;
  }
  const seen = new Set(upstreamOpts.map(([name]) => name));
  const globalOpts = globalArtifactOptions(global, typeFilter).filter(([name]) => !seen.has(name));
  return [...upstreamOpts, ...globalOpts];
}
