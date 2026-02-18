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
  options: { __type__: "IncomingColumns" } | string[];
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
  options: { __type__: "IncomingColumns" } | string[];
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

// Generic column action row structure
export interface ColumnActionRow {
  column: string;
  action: string;
  output_name: string;
}

// Action option structure from backend
export interface ActionOption {
  value: string;
  label: string;
}

// Generic column action input value structure
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

export interface SectionComponent {
  component_type: "Section";
  title?: string;
  description?: string;
  hidden?: boolean;
  components: Record<string, UIComponent>; // Sections contain other UI components
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

// The final, top-level schema for a custom node
export interface CustomNodeSchema {
  node_name: string;
  node_category: string;
  node_icon: string;
  settings_schema: SettingsSchema;
  number_of_inputs: number;
  number_of_outputs: number;
  kernel_id?: string | null;
  output_names?: string[];
  node_group?: string;
  title?: string;
  intro?: string;
  node_type: NodeTypeLiteral;
  transform_type: TransformTypeLiteral;
}

// --- API function to fetch the schema ---
/**
 * Fetches the complete UI definition for a custom node from the backend.
 */
export async function getCustomNodeSchema(
  flowId: number,
  nodeId: number,
): Promise<CustomNodeSchema> {
  const response = await axios.get<CustomNodeSchema>(
    `/user_defined_components/custom-node-schema`,
    {
      params: { flow_id: flowId, node_id: nodeId },
      headers: { accept: "application/json" },
    },
  );

  return response.data;
}
