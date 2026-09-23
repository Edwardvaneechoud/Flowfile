/**
 * Pure helpers behind the Unpivot drawer: the role table is derived from the
 * saved `UnpivotInput` and written back after every edit.
 *
 * No Vue or DOM imports so it runs under vitest's `node` environment.
 */
import type { DataTypeSelector, UnpivotInput } from "../../../baseNode/nodeInput";
import type { RoleRow, RoleSpec } from "../../../baseNode/selectComponents/columnRoles";

export const UNPIVOT_ROLES: readonly RoleSpec[] = [
  { value: "index", label: "Index key" },
  { value: "value", label: "Value column" },
];

export const DATA_TYPE_OPTIONS: readonly { value: DataTypeSelector; label: string }[] = [
  { value: "all", label: "All other columns" },
  { value: "numeric", label: "Numeric columns" },
  { value: "float", label: "Float columns" },
  { value: "string", label: "String columns" },
  { value: "date", label: "Date columns" },
];

export const dataTypeLabel = (selector: DataTypeSelector | null | undefined): string =>
  DATA_TYPE_OPTIONS.find((option) => option.value === selector)?.label ?? "a data type";

export const unpivotRoleLabel = (role: string): string =>
  UNPIVOT_ROLES.find((spec) => spec.value === role)?.label ?? role;

/** What a column in this role turns into in the output. */
export const unpivotBecomes = (role: string): string =>
  role === "index" ? "kept on every row" : "variable / value rows";

/** Index keys first, in their saved order, then the value columns. */
export const rowsFromUnpivot = (input: UnpivotInput): RoleRow[] => [
  ...input.index_columns.map((name) => ({ name, role: "index" })),
  ...input.value_columns.map((name) => ({ name, role: "value" })),
];

export const writeUnpivotRows = (input: UnpivotInput, rows: readonly RoleRow[]): void => {
  input.index_columns = rows.filter((row) => row.role === "index").map((row) => row.name);
  input.value_columns = rows.filter((row) => row.role === "value").map((row) => row.name);
};

/** Human labels for what is still missing before the node does anything useful. */
export const missingUnpivotParts = (input: UnpivotInput): string[] => {
  if (input.data_type_selector_mode === "data_type") {
    return input.data_type_selector ? [] : ["data type"];
  }
  return input.value_columns.length > 0 ? [] : ["value columns"];
};
