/**
 * Type definitions for the Node Designer
 */

/** A single UI component in the node designer */
export interface DesignerComponent {
  component_type: string;
  field_name: string;
  label: string;
  default?: any;
  placeholder?: string;
  min_value?: number;
  max_value?: number;
  step?: number;
  description?: string;
  required?: boolean;
  multiple?: boolean;
  data_types?: string;
  options_source?: string;
  options_string?: string;
  name_prefix?: string; // For SecretSelector: filter secrets by name prefix
}

/** A section containing multiple components */
export interface DesignerSection {
  name: string;
  title: string;
  components: DesignerComponent[];
}

/** Validation error structure */
export interface ValidationError {
  field: string;
  message: string;
}

/** Custom node info from the backend */
export interface CustomNodeInfo {
  file_name: string;
  node_name: string;
  node_category: string;
  title: string;
  intro: string;
}

/** Node metadata for the designer */
export interface NodeMetadata {
  node_name: string;
  node_category: string;
  title: string;
  intro: string;
  number_of_inputs: number;
  number_of_outputs: number;
}

/** Available component definition for the palette */
export interface AvailableComponent {
  type: string;
  label: string;
  icon: string;
}
