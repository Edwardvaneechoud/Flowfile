/**
 * Node Designer module exports
 */

// Types
export type { CustomNodeInfo, IconInfo, KernelInfo } from "./types";

// DesignerState wire contract
export type {
  ComponentState,
  ComponentType,
  DesignerState,
  ParseResult,
  SectionState,
} from "./designerState";

// Constants
export { STORAGE_KEY, availableComponents, getComponentIcon } from "./constants";

// Composables (designer state lives in stores/node-designer-store.ts)
export { useNodeBrowser } from "./composables/useNodeBrowser";
export { usePolarsAutocompletion } from "./composables";
export { toSnakeCase, toPascalCase } from "./mappers";
