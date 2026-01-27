// Flow-related TypeScript interfaces and types
// Consolidated from features/designer/types.ts and nodes/nodeLogic.ts

import type { NodeResult } from "./node.types";

// ============================================================================
// Flow Execution Types
// ============================================================================

export type ExecutionMode = "Development" | "Performance";
export type ExecutionLocation = "local" | "remote";

// ============================================================================
// Flow Settings
// ============================================================================

export interface FlowSettings {
  flow_id: number;
  name: string;
  description?: string;
  save_location?: string;
  auto_save: boolean;
  modified_on?: number;
  path?: string;
  execution_mode: ExecutionMode;
  execution_location: ExecutionLocation;
  show_detailed_progress: boolean;
  is_running: boolean;
  max_parallel_workers: number;
}

// ============================================================================
// Run Information
// ============================================================================

export interface RunInformation {
  flow_id: number;
  start_time: string; // datetime in ISO format
  end_time: string; // datetime in ISO format
  success: boolean;
  nodes_completed: number;
  number_of_nodes: number;
  node_step_result: NodeResult[];
}

export interface RunInformationDictionary {
  [flow_id: number]: RunInformation;
}

// ============================================================================
// History/Undo-Redo Types
// ============================================================================

export interface HistoryState {
  can_undo: boolean;
  can_redo: boolean;
  undo_description: string | null;
  redo_description: string | null;
  undo_count: number;
  redo_count: number;
}

export interface UndoRedoResult {
  success: boolean;
  action_description: string | null;
  error_message: string | null;
}

export interface OperationResponse {
  success: boolean;
  message: string | null;
  history: HistoryState;
}

// ============================================================================
// Local File Types
// ============================================================================

export interface LocalFileInfo {
  path: string;
  file_name: string;
  file_type: string;
  last_modified_date_timestamp?: number;
  exists: boolean;
}

// ============================================================================
// Node Template Types (for flow designer)
// ============================================================================

export interface NodeTemplate {
  name: string;
  color: string;
  item: string;
  input: number;
  output: number;
  image: string;
  multi: boolean;
  node_group: string;
  prod_ready: boolean;
  drawer_title: string;
  drawer_intro: string;
  custom_node: boolean;
}

export interface NodeInput extends NodeTemplate {
  id: number;
  pos_x: number;
  pos_y: number;
}

// ============================================================================
// Edge Types
// ============================================================================

export interface EdgeInput {
  id: string;
  source: string;
  target: string;
  sourceHandle: string;
  targetHandle: string;
}

// ============================================================================
// Vue Flow Types
// ============================================================================

export interface VueFlowInput {
  node_edges: EdgeInput[];
  node_inputs: NodeInput[];
}
