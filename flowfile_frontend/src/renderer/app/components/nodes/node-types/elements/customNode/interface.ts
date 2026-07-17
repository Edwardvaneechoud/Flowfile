import axios from "axios";

type InputType = "text" | "number" | "secret" | "array" | "date" | "boolean";

// --- Base component definition ---
interface BaseComponent {
  label?: string;
  value: any;
  default: any;
  input_type: InputType;
}

// --- Specific component types for the discriminated union ---

export interface TextInputComponent extends BaseComponent {
  component_type: "TextInput";
  placeholder?: string;
}

export interface MultiSelectComponent extends BaseComponent {
  component_type: "MultiSelect";
  options: { __type__: "IncomingColumns" } | { __type__: "AvailableArtifacts" } | string[];
}

export interface ToggleSwitchComponent extends BaseComponent {
  component_type: "ToggleSwitch";
}

export interface NumericInputComponent extends BaseComponent {
  component_type: "NumericInput";
  min_value?: number;
  max_value?: number;
  placeholder?: string;
}

export interface SliderInputComponent extends BaseComponent {
  component_type: "SliderInput";
  min_value: number;
  max_value: number;
  step?: number;
}

export interface SingleSelectComponent extends BaseComponent {
  component_type: "SingleSelect";
  options: { __type__: "IncomingColumns" } | { __type__: "AvailableArtifacts" } | string[];
}

export interface ColumnSelectorComponent extends BaseComponent {
  component_type: "ColumnSelector";
  required?: boolean;
  data_types: string[] | "ALL";
  multiple?: boolean;
}

export interface SecretSelectorComponent extends BaseComponent {
  component_type: "SecretSelector";
  required?: boolean;
  description?: string;
  options: { __type__: "AvailableSecrets" };
  name_prefix?: string;
}

export interface ColumnActionRow {
  column: string;
  action: string;
  output_name: string;
}

export interface ActionOption {
  value: string;
  label: string;
}

export interface ColumnActionValue {
  rows: ColumnActionRow[];
  group_by_columns: string[];
  order_by_column: string | null;
}

export interface ColumnActionInputComponent extends BaseComponent {
  component_type: "ColumnActionInput";
  actions: ActionOption[];
  output_name_template: string;
  show_group_by: boolean;
  show_order_by: boolean;
  data_types: string[] | "ALL";
}

// --- Section Component Type ---

export interface VisibleWhen {
  field: string; // dotted "<section>.<toggle>" reference
  equals?: boolean; // defaults to true
}

export interface SectionComponent {
  component_type: "Section";
  title?: string;
  description?: string;
  hidden?: boolean;
  visible_when?: VisibleWhen;
  layout?: "vertical" | "horizontal";
  components: Record<string, UIComponent>;
}

export interface SettingsSchema {
  [sectionKey: string]: SectionComponent;
}

export type UIComponent =
  | TextInputComponent
  | MultiSelectComponent
  | ToggleSwitchComponent
  | NumericInputComponent
  | SliderInputComponent
  | SingleSelectComponent
  | ColumnSelectorComponent
  | SecretSelectorComponent
  | ColumnActionInputComponent;

export type NodeTypeLiteral = "process" | "input" | "output";
export type TransformTypeLiteral = "wide" | "long" | "explode";

// Drift info surfaced when saved settings contain keys the current schema no
// longer defines (populate_values silently drops them otherwise).
export interface SchemaDrift {
  unknown_sections: string[];
  unknown_components: string[];
}

export interface CustomNodeSchema {
  node_name: string;
  node_category: string;
  node_icon: string;
  settings_schema: SettingsSchema;
  number_of_inputs: number;
  number_of_outputs: number;
  environment?: "local" | "kernel";
  dependencies?: string[];
  requires_kernel?: boolean;
  kernel_id?: string | null;
  output_names?: string[];
  node_group?: string;
  title?: string;
  intro?: string;
  node_type: NodeTypeLiteral;
  transform_type: TransformTypeLiteral;
  drift?: SchemaDrift | null;
}

// Structured error thrown when the schema fetch fails with a known surface:
//   - "missing" (404 with {node_item}): node type not installed on this machine
//   - "broken"  (409 with {node_item}): registered but failed to load
//   - "unknown": anything else (network, 500, node-not-in-flow)
export type CustomNodeSchemaErrorKind = "missing" | "broken" | "unknown";

export class CustomNodeSchemaError extends Error {
  kind: CustomNodeSchemaErrorKind;
  nodeItem?: string;
  detail?: string;
  constructor(
    kind: CustomNodeSchemaErrorKind,
    message: string,
    nodeItem?: string,
    detail?: string,
  ) {
    super(message);
    this.name = "CustomNodeSchemaError";
    this.kind = kind;
    this.nodeItem = nodeItem;
    this.detail = detail;
  }
}

// --- API function to fetch the schema ---
/**
 * Fetches the complete UI definition for a custom node from the backend.
 * Throws {@link CustomNodeSchemaError} on the missing/broken node surfaces so
 * the drawer can render the right card instead of a generic error string.
 */
export async function getCustomNodeSchema(
  flowId: number,
  nodeId: number,
): Promise<CustomNodeSchema> {
  try {
    const response = await axios.get<CustomNodeSchema>(
      `/user_defined_components/custom-node-schema`,
      {
        params: { flow_id: flowId, node_id: nodeId },
        headers: { accept: "application/json" },
      },
    );
    return response.data;
  } catch (err) {
    if (axios.isAxiosError(err) && err.response) {
      const status = err.response.status;
      const detail = err.response.data?.detail;
      const nodeItem = typeof detail === "object" ? detail?.node_item : undefined;
      const errorText = typeof detail === "object" ? (detail?.error ?? detail?.message) : detail;
      if (status === 404 && nodeItem) {
        throw new CustomNodeSchemaError("missing", errorText ?? "Node not installed", nodeItem);
      }
      if (status === 409 && nodeItem) {
        throw new CustomNodeSchemaError(
          "broken",
          errorText ?? "Node failed to load",
          nodeItem,
          errorText,
        );
      }
    }
    throw err;
  }
}
