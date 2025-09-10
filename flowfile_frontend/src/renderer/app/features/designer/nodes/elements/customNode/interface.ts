import axios from "axios";


type InputType = "text" | "number" | "secret" | "array" | "date" | "boolean";


// --- Base component definition ---
interface BaseComponent {
  label?: string;
  value: any;
  default: any;
  input_type: InputType
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

export type UIComponent = TextInputComponent | MultiSelectComponent | ToggleSwitchComponent;

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
export async function getCustomNodeSchema(): Promise<CustomNodeSchema> {
  const response = await axios.get<CustomNodeSchema>("/custom_components/custom-node-schema");
  return response.data;
}

