import { NodeBase } from '../../../baseNode/nodeInput'

// Base Schema Types
export interface SchemaNestedProperty {
  type: string
  description?: string
  title?: string
  airbyte_secret?: boolean
  default?: any
  const?: string
  items?: {
    type: string
    pattern?: string
  }
}

export interface SchemaProperty {
  title: string
  type: string
  description?: string
  properties?: Record<string, SchemaNestedProperty>
  airbyte_secret?: boolean
  default?: any
  oneOf?: OneOfOption[]
  const?: string
  items?: {
    type: string
    pattern?: string
  }
  examples?: any[]
  uniqueItems?: boolean
}

// OneOf Related Types
export interface OneOfPropertyValue {
  type: string
  description?: string
  title?: string
  airbyte_secret?: boolean
  default?: any
  const?: string
  input_value?: any
  items?: {
    type: string
    pattern?: string
  }
}

export interface OneOfOption {
  title: string
  type: string
  description?: string
  required?: string[]
  properties?: Record<string, OneOfPropertyValue>
  const?: string
}

// Field Related Types
export interface FieldProperty {
  key: string
  type: string
  description: string
  title?: string
  airbyte_secret: boolean
  input_value: string | string[]
  default?: any
  items?: {
    type: string
    pattern?: string
  }
  required?: boolean // Added required field
}

export interface Field {
  title?: string;
  type: string;
  key: string;
  required: boolean;
  properties: FieldProperty[];
  items?: {
    type: string;
    pattern?: string;
  };
  isOpen: boolean;
  description?: string;
  input_value: string | string[] | Record<string, any>; // Updated this type to include Record
  airbyte_secret?: boolean;
  default: any;
  oneOf?: OneOfOption[];
  selectedOption?: number;
  examples?: any[];
  uniqueItems?: boolean;
}

// Airbyte Configuration Types
export interface AirbyteConnectorSpecification {
  properties: Record<string, SchemaProperty>
  required: string[]
}

export interface AirbyteConfigTemplate {
  source_name: string
  docs_url: string | null
  config_spec: AirbyteConnectorSpecification
  available_streams: string[] | null
}

export interface AirbyteConfig {
  source_name: string
  selected_stream: string | null
  config_mode: string
  mapped_config_spec: Record<string, any>
  parsed_config: Field[]
  connection_name?: string
}

export interface AirbyteConnectorInput {
  source_name: string
  config_spec: AirbyteConnectorSpecification
  required?: string[]
  docs_url: string | null
  mapped_config_spec?: Record<string, any>
  selected_stream?: string | null
  available_streams?: string[] | null
  parsed_config?: Field[]
}

export interface AirbyteSource extends AirbyteConfig {
  class_name?: string
  fields?: Field[]
}

// Node Types
export interface NodeExternalSource extends NodeBase {
  identifier: string
  source_settings: AirbyteSource
}

// Type Guards
export function hasOneOf(value: SchemaProperty): value is SchemaProperty & { oneOf: OneOfOption[] } {
  return 'oneOf' in value && Array.isArray(value.oneOf);
}

// Additional type guards for array handling
export function isArrayField(field: Field): boolean {
  return field.type === 'array' && !!field.items;
}

export function hasArrayValue(value: any): value is string[] {
  return Array.isArray(value);
}

// Utility type for array input handling
export interface ArrayInputValue {
  value: string;
}