/**
 * Constants for the Node Designer
 */
import { componentDocs } from "./componentDocs";
import type { AvailableComponent } from "./types";

/** Legacy sessionStorage key; kept only for the one-time draft migration in node-designer-store */
export const STORAGE_KEY = "nodeDesigner_state";

/** Available component types for the palette, derived from the component reference. */
export const availableComponents: AvailableComponent[] = componentDocs.map((c) => ({
  type: c.type,
  label: c.label,
  icon: c.icon,
}));

/** Default process code template */
export const defaultProcessCode = `def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
    # Get the first input LazyFrame
    lf = inputs[0]

    # Your transformation logic here
    # Example: lf = lf.filter(pl.col("column") > 0)

    return lf`;

/** Get component icon by type */
export function getComponentIcon(type: string): string {
  const comp = availableComponents.find((c) => c.type === type);
  return comp?.icon || "fa-solid fa-puzzle-piece";
}

/** Canonical type-group tokens for a ColumnSelector/ColumnActionInput data_types filter
 *  (mirrors the SDK TypeGroup; a bare group token means "any column in that group"). */
export const DATA_TYPE_GROUP_OPTIONS = [
  "Numeric",
  "String",
  "Date",
  "Boolean",
  "Binary",
  "Complex",
] as const;

/** Canonical specific-type tokens (SDK DataType values, minus those that collide with a
 *  group name — "String"/"Date"/"Boolean"/"Binary" are only offered as groups). */
export const DATA_TYPE_SPECIFIC_OPTIONS = [
  "Int8",
  "Int16",
  "Int32",
  "Int64",
  "Int128",
  "UInt8",
  "UInt16",
  "UInt32",
  "UInt64",
  "UInt128",
  "Float16",
  "Float32",
  "Float64",
  "Decimal",
  "Categorical",
  "Datetime",
  "Time",
  "Duration",
  "List",
  "Struct",
  "Array",
] as const;
