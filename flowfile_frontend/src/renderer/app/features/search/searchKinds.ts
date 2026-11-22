import type { SearchHit, SearchKind } from "../../types/search.types";

export interface SearchKindMeta {
  kind: SearchKind;
  label: string;
  group: string;
  icon: string;
}

// Same order as core's SearchKind enum, which breaks score ties the same way.
export const SEARCH_KINDS: readonly SearchKindMeta[] = [
  {
    kind: "namespace",
    label: "Catalog",
    group: "Catalogs & schemas",
    icon: "fa-solid fa-folder-tree",
  },
  { kind: "table", label: "Table", group: "Tables", icon: "fa-solid fa-table" },
  { kind: "column", label: "Column", group: "Columns", icon: "fa-solid fa-table-columns" },
  { kind: "flow", label: "Flow", group: "Flows", icon: "fa-solid fa-diagram-project" },
  { kind: "schedule", label: "Schedule", group: "Schedules", icon: "fa-solid fa-calendar-days" },
  { kind: "notebook", label: "Notebook", group: "Notebooks", icon: "fa-solid fa-book" },
  {
    kind: "visualization",
    label: "Chart",
    group: "Visualizations",
    icon: "fa-solid fa-chart-column",
  },
  { kind: "dashboard", label: "Dashboard", group: "Dashboards", icon: "fa-solid fa-gauge-high" },
  { kind: "artifact", label: "Artifact", group: "Global artifacts", icon: "fa-solid fa-cube" },
  { kind: "kernel", label: "Kernel", group: "Python kernels", icon: "fa-brands fa-python" },
  {
    kind: "custom_node",
    label: "Custom node",
    group: "Custom nodes",
    icon: "fa-solid fa-puzzle-piece",
  },
];

export const KIND_META: Record<SearchKind, SearchKindMeta> = Object.fromEntries(
  SEARCH_KINDS.map((meta) => [meta.kind, meta]),
) as Record<SearchKind, SearchKindMeta>;

export const KIND_ORDER: Record<SearchKind, number> = Object.fromEntries(
  SEARCH_KINDS.map((meta, index) => [meta.kind, index]),
) as Record<SearchKind, number>;

/** A hit's own label: a schema is not a catalog. */
export function hitKindLabel(hit: SearchHit): string {
  if (hit.kind === "namespace") return hit.details.level === 1 ? "Schema" : "Catalog";
  return KIND_META[hit.kind].label;
}

export function hitIcon(hit: SearchHit): string {
  if (hit.kind === "namespace" && hit.details.level === 1) return "fa-solid fa-folder";
  return KIND_META[hit.kind].icon;
}
