/**
 * Pure model behind the Formula node's ordered list of entries.
 *
 * Entries are evaluated sequentially, so entry N sees the input columns plus the
 * output columns of every non-blank entry before it. Kept free of Vue, DOM and
 * axios imports so it runs under vitest's `node` environment.
 */
import type { FieldInput, FormulaInput, NodeFormula } from "@/types/node.types";
/** Minimal column shape the editor needs: a name and, when known, a data type. */
export interface FormulaColumn {
  name: string;
  data_type: string;
}

export const AUTO_DATA_TYPE = "Auto";

export const createFormulaInput = (
  field_name = "",
  data_type = AUTO_DATA_TYPE,
  function_def = "",
): FormulaInput => {
  const fieldInput: FieldInput = {
    name: field_name,
    data_type: data_type,
  };

  return { field: fieldInput, function: function_def };
};

export const createFormulaNode = (
  flowId = -1,
  nodeId = -1,
  pos_x = 0,
  pos_y = 0,
  field_name = "output_field",
  data_type = AUTO_DATA_TYPE,
  function_def = "",
): NodeFormula => ({
  flow_id: flowId,
  node_id: nodeId,
  pos_x: pos_x,
  pos_y: pos_y,
  functions: [createFormulaInput(field_name, data_type, function_def)],
  cache_results: false,
});

/**
 * IN-PLACE: fills `functions` from the legacy single `function` field and makes
 * sure every entry is a fully shaped object. The backend normalises the same
 * way; this keeps the drawer safe against a settings blob saved by an older core.
 */
export const normalizeNodeFormula = (node: NodeFormula): NodeFormula => {
  const source =
    Array.isArray(node.functions) && node.functions.length > 0
      ? node.functions
      : node.function
        ? [node.function]
        : [createFormulaInput()];
  node.functions = source.map((entry) =>
    createFormulaInput(
      entry?.field?.name ?? "",
      entry?.field?.data_type ?? AUTO_DATA_TYPE,
      entry?.function ?? "",
    ),
  );
  return node;
};

/** An entry only contributes a column once it has both a name and an expression. */
export const isBlankEntry = (entry: FormulaInput): boolean =>
  !entry.field.name.trim() || !entry.function.trim();

/**
 * The schema entry `index` sees: the input columns plus the outputs of the
 * non-blank entries before it. Deduped by name, first appearance sets the
 * position while the latest definition sets the data type.
 */
export const accumulatedColumnsAt = (
  baseColumns: readonly FormulaColumn[],
  entries: readonly FormulaInput[],
  index: number,
): FormulaColumn[] => {
  const byName = new Map<string, FormulaColumn>();
  const push = (name: string, dataType: string) => {
    if (!name) return;
    const existing = byName.get(name);
    if (existing) {
      existing.data_type = dataType;
      return;
    }
    byName.set(name, { name, data_type: dataType });
  };

  for (const column of baseColumns) push(column.name, column.data_type ?? "");
  for (let i = 0; i < Math.min(index, entries.length); i++) {
    const entry = entries[i];
    if (isBlankEntry(entry)) continue;
    const dataType = entry.field.data_type;
    push(entry.field.name.trim(), !dataType || dataType === AUTO_DATA_TYPE ? "" : dataType);
  }
  return [...byName.values()];
};

/**
 * Entry index → 1-based number of the earlier formula whose output it overwrites.
 * Only the overwriting row is listed: writing a column twice is deliberate, so the first
 * producer is not at fault and gets no note.
 */
export const overwrittenOutputs = (entries: readonly FormulaInput[]): Map<number, number> => {
  const firstProducer = new Map<string, number>();
  const overwrites = new Map<number, number>();
  entries.forEach((entry, index) => {
    const name = entry.field.name.trim();
    if (!name) return;
    const first = firstProducer.get(name);
    if (first === undefined) firstProducer.set(name, index);
    else overwrites.set(index, first + 1);
  });
  return overwrites;
};

/** Every `[from]` in the expression becomes `[to]`; the bracket-bound match never touches a longer name. */
export const replaceColumnReference = (expression: string, from: string, to: string): string =>
  expression.split(`[${from}]`).join(`[${to}]`);

const entryUids = new WeakMap<FormulaInput, string>();
let uidCounter = 0;

/**
 * Stable per-object key for `v-for`. Reorder keeps object references, so the key
 * travels with the entry instead of with its position — an index key would
 * strand editor text on the row that used to be there.
 */
export const entryUid = (entry: FormulaInput): string => {
  let uid = entryUids.get(entry);
  if (!uid) {
    uid = `formula-entry-${++uidCounter}`;
    entryUids.set(entry, uid);
  }
  return uid;
};

/** Rows are auto-collapsed on open from this many entries up. */
export const AUTO_COLLAPSE_THRESHOLD = 4;

/** One line, no runs of whitespace. Overflow is CSS's job, not this module's. */
export const flattenExpression = (expression: string): string =>
  expression.replace(/\s+/g, " ").trim();

export interface EntrySummary {
  text: string;
  /** True when `text` stands in for a missing expression and should read as muted. */
  placeholder: boolean;
}

/** What a collapsed row shows beside its name field — the expression, nothing else. */
export const entrySummary = (entry: FormulaInput): EntrySummary => {
  const expression = flattenExpression(entry.function);
  if (expression) return { text: expression, placeholder: false };
  return { text: entry.field.name.trim() ? "skipped" : "empty", placeholder: true };
};

/**
 * IN-PLACE: drops the legacy `function` key so a save only ever ships
 * `functions`. Returns the same node — `useNodeSettings` posts the ref itself.
 */
export const toSavePayload = (node: NodeFormula): NodeFormula => {
  delete node.function;
  return node;
};
