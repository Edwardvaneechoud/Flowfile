/**
 * Pure helpers behind the Unpivot drawer: the role table is derived from the
 * saved `UnpivotInput` and written back after every edit.
 *
 * No Vue or DOM imports so it runs under vitest's `node` environment.
 */
import type { DataTypeSelector, UnpivotInput } from "../../../baseNode/nodeInput";
import { pluralize } from "../../../baseNode/selectComponents/columnPicker";
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
  DATA_TYPE_OPTIONS.find((option) => option.value === (selector ?? "all"))?.label ?? "a data type";

export const unpivotRoleLabel = (role: string): string =>
  UNPIVOT_ROLES.find((spec) => spec.value === role)?.label ?? role;

/** Index keys first, in their saved order, then the value columns. */
export const rowsFromUnpivot = (input: UnpivotInput): RoleRow[] => [
  ...input.index_columns.map((name) => ({ name, role: "index" })),
  ...input.value_columns.map((name) => ({ name, role: "value" })),
];

export const writeUnpivotRows = (input: UnpivotInput, rows: readonly RoleRow[]): void => {
  input.index_columns = rows.filter((row) => row.role === "index").map((row) => row.name);
  input.value_columns = rows.filter((row) => row.role === "value").map((row) => row.name);
};

/** The chosen columns, counted; none chosen means every column outside the index, as the backend reads it. */
export const valueColumnsLabel = (valueColumns: readonly string[]): string =>
  valueColumns.length > 0 ? pluralize(valueColumns.length, "column") : "every other column";
