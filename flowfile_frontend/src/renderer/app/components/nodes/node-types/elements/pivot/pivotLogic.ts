/**
 * Pure helpers behind the Pivot drawer: the role table is derived from the
 * saved `PivotInput` and written back after every edit.
 *
 * No Vue or DOM imports so it runs under vitest's `node` environment.
 */
import type { AggOption, PivotInput } from "../../../baseNode/nodeInput";
import type { RoleRow, RoleSpec } from "../../../baseNode/selectComponents/columnRoles";

export const PIVOT_ROLES: readonly RoleSpec[] = [
  { value: "index", label: "Index key" },
  { value: "pivot", label: "Pivot column", single: true },
  { value: "value", label: "Value column", single: true },
];

export const PIVOT_AGGREGATIONS: readonly AggOption[] = [
  "sum",
  "count",
  "min",
  "max",
  "n_unique",
  "mean",
  "median",
  "first",
  "last",
  "concat",
];

export const pivotRoleLabel = (role: string): string =>
  PIVOT_ROLES.find((spec) => spec.value === role)?.label ?? role;

/** What a column in this role turns into in the output. */
export const pivotBecomes = (role: string): string =>
  role === "index" ? "one row per value" : role === "pivot" ? "column headers" : "cell values";

/** Index keys first, in their saved order, then the pivot and value columns. */
export const rowsFromPivot = (input: PivotInput): RoleRow[] => [
  ...input.index_columns.map((name) => ({ name, role: "index" })),
  ...(input.pivot_column ? [{ name: input.pivot_column, role: "pivot" }] : []),
  ...(input.value_col ? [{ name: input.value_col, role: "value" }] : []),
];

export const writePivotRows = (input: PivotInput, rows: readonly RoleRow[]): void => {
  input.index_columns = rows.filter((row) => row.role === "index").map((row) => row.name);
  input.pivot_column = rows.find((row) => row.role === "pivot")?.name ?? null;
  input.value_col = rows.find((row) => row.role === "value")?.name ?? null;
};

/** Human labels for what is still missing before the node can run. */
export const missingPivotParts = (input: PivotInput): string[] => [
  ...(input.pivot_column ? [] : ["pivot column"]),
  ...(input.value_col ? [] : ["value column"]),
  ...(input.aggregations.length > 0 ? [] : ["aggregation"]),
];
