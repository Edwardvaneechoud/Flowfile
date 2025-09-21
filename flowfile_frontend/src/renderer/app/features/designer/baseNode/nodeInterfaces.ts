import { ref } from 'vue'


type DataTypeGroup = "String" | "Date" | "Numeric"

export interface FileColumn {
  name: string
  data_type: string
  is_unique: boolean
  max_value: string
  min_value: string
  number_of_empty_values: number
  number_of_filled_values: number
  number_of_unique_values: number
  size: number
  data_type_group: DataTypeGroup
}

export interface TableExample {
  node_id: string | number
  number_of_records: number
  number_of_columns: number
  name: string
  table_schema: FileColumn[]
  columns: string[]
  data: Record<string, any>[]
  has_example_data: boolean
  has_run_with_current_setup: boolean
}

export interface NodeData {
  flow_id: string | number
  node_id: string | number
  flow_type: string
  left_input?: TableExample | null
  right_input?: TableExample | null
  main_input?: TableExample | null
  main_output?: TableExample
  left_output?: TableExample | null
  right_output?: TableExample | null
  has_run: boolean
  is_cached: boolean
  setting_input?: any | null
}

export const nodeData = ref<NodeData | null>(null)

export interface NodeValidationInput {
  isValid: boolean
  error?: string
}

export interface NodeValidation extends NodeValidationInput {  
  validationTime: number
}

export interface NodeResult {
  node_id: number
  node_name?: string
  start_timestamp: number
  end_timestamp: number
  success?: boolean
  error: string
  run_time: number
  is_running: boolean
}

export interface RunInformation {
  flow_id: number
  start_time: string // datetime in ISO format
  end_time: string // datetime in ISO format
  success: boolean
  nodes_completed: number
  number_of_nodes: number
  node_step_result: NodeResult[]
}

export interface RunInformationDictionary {
  [flow_id: number]: RunInformation
}

export interface NodeDescriptionDictionaryPerGraph {
  [node_id: number]: string
}

export interface NodeDescriptionDictionary {
  [flow_id: number]: NodeDescriptionDictionaryPerGraph
}


export interface ExpressionRef {
  name: string
  doc: string
}

export interface ExpressionsOverview {
  expression_type: string
  expressions: ExpressionRef[]
}

export interface NodeTitleInfo {
  title: string
  intro: string
}