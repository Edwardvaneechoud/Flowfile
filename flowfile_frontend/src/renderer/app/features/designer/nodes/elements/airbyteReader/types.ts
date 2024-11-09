import { NodeBase } from '../../../baseNode/nodeInput'

export interface SchemaProperty {
  title: string
  type: string
  description?: string
  properties?: Record<string, SchemaNestedProperty>
  airbyte_secret?: boolean
  default?: any
}

export interface SchemaNestedProperty {
  type: string
  description?: string
  title?: string
  airbyte_secret?: boolean
  default?: any
}

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

export interface FieldProperty {
  key: string
  type: string
  description: string
  title?: string
  airbyte_secret: boolean
  input_value: string
  default?: any
}

export interface Field {
  title?: string
  type: string
  key: string
  required: boolean
  properties: FieldProperty[]
  items?: Field[]
  isOpen: boolean
  description?: string
  input_value?: string
  airbyte_secret?: boolean
  default: any
}

interface AirbyteSource extends AirbyteConfig {
  class_name?: string
  fields?: Field[]
}

export interface NodeExternalSource extends NodeBase {
  identifier: string
  source_settings: AirbyteSource
}
