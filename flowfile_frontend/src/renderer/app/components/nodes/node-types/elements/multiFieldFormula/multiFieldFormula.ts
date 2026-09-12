import type {
  MultiFieldFormulaInput,
  NodeMultiFieldFormula,
} from "../../../../../types/node.types";

/** Minimal schema shape the target resolver needs (structurally a `FileColumn`). */
export interface MultiFieldSchemaColumn {
  name: string;
  data_type_group: string;
}

export type MultiFieldSelection = Pick<
  MultiFieldFormulaInput,
  "selection_mode" | "selected_columns" | "selected_data_type"
>;

export type MultiFieldOutputShape = Pick<
  MultiFieldFormulaInput,
  "output_mode" | "output_prefix" | "output_suffix"
>;

export interface MultiFieldPreviewRow {
  source: string;
  output: string;
}

export const createMultiFieldFormulaInput = (): MultiFieldFormulaInput => ({
  formula: "",
  selection_mode: "all",
  selected_columns: [],
  selected_data_type: null,
  output_mode: "replace",
  output_prefix: "",
  output_suffix: "",
  output_data_type: "Auto",
});

/**
 * Fill a persisted settings blob out to a complete, drawer-ready object.
 *
 * A saved flow may carry a missing or `null` `output_data_type` — the engine
 * treats both as Auto, but the Data type select would render empty.
 */
export const normalizeMultiFieldFormulaInput = (
  raw: Partial<MultiFieldFormulaInput> | null | undefined,
): MultiFieldFormulaInput => ({
  ...createMultiFieldFormulaInput(),
  ...(raw ?? {}),
  selected_columns: raw?.selected_columns ?? [],
  output_data_type: raw?.output_data_type || "Auto",
});

export const createMultiFieldFormulaNode = (
  flowId = -1,
  nodeId = -1,
  pos_x = 0,
  pos_y = 0,
): NodeMultiFieldFormula => ({
  flow_id: flowId,
  node_id: nodeId,
  pos_x,
  pos_y,
  cache_results: false,
  multi_field_formula_input: createMultiFieldFormulaInput(),
});

/**
 * Resolve the columns the formula will be applied to.
 *
 * Mirrors the engine's target rules exactly: `all` keeps schema order, `list`
 * keeps the order the user picked and silently drops names that are no longer in
 * the schema (stale drawer state must not break a run), `data_type` filters on
 * the readable group and yields nothing when no group is selected. An unknown
 * mode yields nothing, matching the backend's fall-through.
 */
export const resolveTargetColumns = (
  schema: MultiFieldSchemaColumn[],
  settings: MultiFieldSelection,
): string[] => {
  if (settings.selection_mode === "all") {
    return schema.map((c) => c.name);
  }
  if (settings.selection_mode === "list") {
    const available = new Set(schema.map((c) => c.name));
    return settings.selected_columns.filter((name) => available.has(name));
  }
  if (settings.selection_mode === "data_type") {
    const wanted = settings.selected_data_type;
    if (!wanted) return [];
    return schema.filter((c) => c.data_type_group === wanted).map((c) => c.name);
  }
  return [];
};

/**
 * Map each target column onto the column the run will write.
 *
 * `replace` overwrites in place, so source and output are the same name; `new`
 * appends the configured prefix/suffix. Purely a drawer preview — the engine
 * applies the identical rule, and affix validation stays server-side.
 */
export const previewOutputColumns = (
  targets: string[],
  settings: MultiFieldOutputShape,
): MultiFieldPreviewRow[] =>
  targets.map((source) => ({
    source,
    output:
      settings.output_mode === "new"
        ? `${settings.output_prefix}${source}${settings.output_suffix}`
        : source,
  }));
