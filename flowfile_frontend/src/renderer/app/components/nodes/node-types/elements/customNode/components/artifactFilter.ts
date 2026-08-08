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

// `value` is what gets stored in the node settings: a bare artifact name, or a
// namespace-qualified "catalog.schema::name" reference that core resolves. `key`
// is display-only identity and is always distinct, even when two artifacts share
// a name. Labels never carry a version — a selection always resolves to the
// newest version at run time.
export interface ArtifactSelectOption {
  value: string;
  label: string;
  key: string;
  namespaceId?: number | null;
}

// Static (string[]) and IncomingColumns options stay plain strings; the legacy
// [value, label] tuple is still accepted by the selects.
export type SelectOptionItem = string | [string, string] | ArtifactSelectOption;

export function optionValue(item: SelectOptionItem): string {
  if (typeof item === "string") return item;
  return Array.isArray(item) ? item[0] : item.value;
}

export function optionLabel(item: SelectOptionItem): string {
  if (typeof item === "string") return item;
  return Array.isArray(item) ? item[1] : item.label;
}

export function optionKey(item: SelectOptionItem): string {
  if (typeof item === "string") return item;
  return Array.isArray(item) ? item[0] : item.key;
}

export function upstreamArtifactOptions(
  artifacts: ArtifactOption[],
  typeFilter?: string[],
): ArtifactSelectOption[] {
  return artifacts
    .filter((a) => matchesTypeFilter([a.module, a.type_name].filter(Boolean).join("."), typeFilter))
    .map(
      (a): ArtifactSelectOption => ({
        value: a.name,
        label: a.type_name ? `${a.name} (${a.type_name})` : a.name,
        key: a.name,
      }),
    );
}

// Safety net for older cores that don't serve ?latest_only=true: a name resolves
// to its newest version anyway, so every older row is a duplicate choice.
function newestPerArtifact(artifacts: GlobalArtifactOption[]): GlobalArtifactOption[] {
  const newest = new Map<string, GlobalArtifactOption>();
  for (const a of artifacts) {
    const key = JSON.stringify([a.namespace_id ?? null, a.name]);
    const seen = newest.get(key);
    if (!seen || a.version > seen.version) newest.set(key, a);
  }
  // Map preserves first-insertion order, so the source ordering survives.
  return [...newest.values()];
}

function artifactOptionValue(a: GlobalArtifactOption): string {
  return a.namespace_path ? `${a.namespace_path}::${a.name}` : a.name;
}

function artifactOptionKey(a: GlobalArtifactOption): string {
  return `${a.namespace_id ?? ""}::${a.name}`;
}

function namespaceLabel(a: GlobalArtifactOption): string {
  if (a.namespace_path) return a.namespace_path;
  return a.namespace_id != null ? `ns ${a.namespace_id}` : "default";
}

export function globalArtifactOptions(
  artifacts: GlobalArtifactOption[],
  typeFilter?: string[],
): ArtifactSelectOption[] {
  const collapsed = newestPerArtifact(
    artifacts.filter((a) => matchesTypeFilter(a.python_type ?? "", typeFilter)),
  );
  const counts = new Map<string, number>();
  for (const a of collapsed) counts.set(a.name, (counts.get(a.name) ?? 0) + 1);

  return collapsed.map((a): ArtifactSelectOption => {
    const type = a.python_type ? ` (${formatTypeShort(a.python_type)})` : "";
    const suffix = (counts.get(a.name) ?? 0) > 1 ? ` — ${namespaceLabel(a)}` : "";
    return {
      value: artifactOptionValue(a),
      label: `${a.name}${type}${suffix}`,
      key: artifactOptionKey(a),
      namespaceId: a.namespace_id ?? null,
    };
  });
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
  const seen = new Set(upstreamOpts.map((o) => o.value));
  const globalOpts = globalArtifactOptions(
    global.filter((a) => !seen.has(a.name)),
    typeFilter,
  );
  return [...upstreamOpts, ...globalOpts];
}
