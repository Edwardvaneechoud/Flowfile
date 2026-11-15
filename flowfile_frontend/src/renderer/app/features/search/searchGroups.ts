import type { SearchHit, SearchKind, SearchMatch, SearchResponse } from "../../types/search.types";
import { KIND_META, KIND_ORDER } from "./searchKinds";

export interface SearchGroup {
  kind: SearchKind;
  label: string;
  icon: string;
  hits: SearchHit[];
  total: number;
}

/** The response's hits by kind, the kind with the best hit first; kinds without hits are left out. */
export function groupHits(response: SearchResponse | null): SearchGroup[] {
  if (!response) return [];
  const byKind = new Map<SearchKind, SearchHit[]>();
  for (const hit of response.hits) {
    const list = byKind.get(hit.kind) ?? [];
    list.push(hit);
    byKind.set(hit.kind, list);
  }
  const groups = [...byKind.entries()].map(([kind, hits]) => ({
    kind,
    label: KIND_META[kind].group,
    icon: KIND_META[kind].icon,
    hits,
    total: Math.max(response.counts[kind] ?? hits.length, hits.length),
  }));
  groups.sort(
    (a, b) => b.hits[0].score - a.hits[0].score || KIND_ORDER[a.kind] - KIND_ORDER[b.kind],
  );
  return groups;
}

/** The hits in the order they are shown, for arrow-key navigation. */
export function flattenGroups(groups: readonly SearchGroup[]): SearchHit[] {
  return groups.flatMap((group) => group.hits);
}

export function totalMatches(response: SearchResponse | null): number {
  if (!response) return 0;
  return Object.values(response.counts).reduce<number>((sum, n) => sum + (n ?? 0), 0);
}

const FIELD_LABELS: Record<string, string> = {
  path: "in",
  description: "description",
  column: "column",
  dtype: "type",
  flow: "flow",
  package: "package",
  tag: "tag",
  python_type: "type",
  category: "category",
  cron: "cron",
  table: "table",
  type: "type",
  id: "id",
  title: "title",
};

/** The matches worth showing under a hit: those not already visible as its name or subtitle. */
export function extraMatches(hit: SearchHit): Array<SearchMatch & { label: string }> {
  return hit.matches
    .filter((m) => m.field !== "name" && m.field !== "path" && m.value !== hit.subtitle)
    .map((m) => ({ ...m, label: FIELD_LABELS[m.field] ?? m.field }));
}
