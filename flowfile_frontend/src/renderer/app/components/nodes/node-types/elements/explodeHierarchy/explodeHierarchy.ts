import type {
  ExplodeHierarchyInput,
  HierarchyOutputDetail,
  NodeExplodeHierarchy,
} from "../../../../../types/node.types";

export interface HierarchyOutputOption {
  value: HierarchyOutputDetail;
  label: string;
  description: string;
}

export interface HierarchyOutputColumn {
  name: string;
  description: string;
}

export const HIERARCHY_OUTPUT_OPTIONS: readonly HierarchyOutputOption[] = [
  {
    value: "totals",
    label: "Totals",
    description:
      "One row per item and everything it contains, with quantities summed over all routes.",
  },
  {
    value: "levels",
    label: "Levels",
    description: "Like Totals, split per level: one row per item, component and level.",
  },
  {
    value: "paths",
    label: "Paths",
    description: "One row per route from item to component, in indented-BOM order.",
  },
];

const ANCESTOR = { name: "ancestor", description: "the item being exploded" };
const IS_LEAF = { name: "is_leaf", description: "the component contains nothing itself" };

const OUTPUT_COLUMNS: Record<HierarchyOutputDetail, readonly HierarchyOutputColumn[]> = {
  totals: [
    ANCESTOR,
    { name: "descendant", description: "a component it contains, at any depth" },
    { name: "level", description: "the shallowest depth it occurs at (1 = direct)" },
    { name: "quantity", description: "total quantity of the component in one item" },
    IS_LEAF,
  ],
  levels: [
    ANCESTOR,
    { name: "descendant", description: "a component it contains, at any depth" },
    { name: "level", description: "the depth this row counts (1 = direct)" },
    { name: "quantity", description: "quantity of the component in one item at this depth" },
    IS_LEAF,
  ],
  paths: [
    ANCESTOR,
    { name: "descendant", description: "the component this route ends at" },
    { name: "level", description: "the length of the route (1 = direct)" },
    { name: "parent", description: "the component's direct parent on this route" },
    { name: "quantity_per", description: "the quantity on the last edge of the route" },
    { name: "quantity", description: "quantities multiplied along the route" },
    IS_LEAF,
    { name: "path", description: "every item on the route, as a list" },
  ],
};

const OUTPUT_DETAILS = new Set<string>(HIERARCHY_OUTPUT_OPTIONS.map((o) => o.value));

/** The fixed columns the node produces; Totals and Levels share one schema. */
export const hierarchyOutputColumns = (
  detail: HierarchyOutputDetail,
): readonly HierarchyOutputColumn[] => OUTPUT_COLUMNS[detail] ?? OUTPUT_COLUMNS.totals;

export const hierarchyOutputOption = (detail: HierarchyOutputDetail): HierarchyOutputOption =>
  HIERARCHY_OUTPUT_OPTIONS.find((o) => o.value === detail) ?? HIERARCHY_OUTPUT_OPTIONS[0];

export const createExplodeHierarchyInput = (): ExplodeHierarchyInput => ({
  parent_column: "",
  child_column: "",
  quantity_column: null,
  output_detail: "totals",
  top_level_only: false,
  include_self: false,
  max_depth: null,
});

/** polars-grouper reads max_depth as a u32; the backend rejects anything larger. */
export const MAX_HIERARCHY_DEPTH = 4294967295;

/** Blank, cleared or non-numeric entries mean "unlimited"; the backend only accepts ints >= 0. */
export const normalizeMaxDepth = (value: unknown): number | null => {
  if (value === null || value === undefined || value === "") return null;
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n) || n < 0) return null;
  return Math.min(Math.trunc(n), MAX_HIERARCHY_DEPTH);
};

/**
 * Fill a persisted or edited settings blob out to a complete, wire-ready object.
 *
 * A cleared select yields `""` or `undefined` and a cleared number input `null`
 * or `NaN`; the backend wants `null` for "no quantity column" and "no depth limit".
 */
export const normalizeExplodeHierarchyInput = (
  raw: Partial<ExplodeHierarchyInput> | null | undefined,
): ExplodeHierarchyInput => {
  const base = { ...createExplodeHierarchyInput(), ...(raw ?? {}) };
  return {
    ...base,
    parent_column: base.parent_column ?? "",
    child_column: base.child_column ?? "",
    quantity_column: base.quantity_column || null,
    output_detail: OUTPUT_DETAILS.has(base.output_detail) ? base.output_detail : "totals",
    top_level_only: !!base.top_level_only,
    include_self: !!base.include_self,
    max_depth: normalizeMaxDepth(base.max_depth),
  };
};

export const createExplodeHierarchyNode = (
  flowId = -1,
  nodeId = -1,
  pos_x = 0,
  pos_y = 0,
): NodeExplodeHierarchy => ({
  flow_id: flowId,
  node_id: nodeId,
  pos_x,
  pos_y,
  cache_results: false,
  explode_hierarchy_input: createExplodeHierarchyInput(),
});
