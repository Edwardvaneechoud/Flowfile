// Flow-related TypeScript interfaces and types
// Consolidated from features/designer/types.ts and nodes/nodeLogic.ts

import type { NodeResult } from "./node.types";
import type { Position } from "@vue-flow/core";

// ============================================================================
// Flow Execution Types
// ============================================================================

export type ExecutionMode = "Development" | "Performance";
export type ExecutionLocation = "local" | "remote";

// ============================================================================
// Flow Parameters
// ============================================================================

export interface FlowParameter {
  name: string;
  default_value: string;
  description: string;
}

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
  show_edge_labels: boolean;
  is_running: boolean;
  max_parallel_workers: number;
  parameters?: FlowParameter[];
  has_unsaved_changes?: boolean;
  display_name?: string | null;
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
  output_names?: string[];
}

export interface NodeInput extends NodeTemplate {
  id: number;
  pos_x: number;
  pos_y: number;
  group_id?: number | null;
  node_reference?: string;
}

// ============================================================================
// Handle Types
// ============================================================================

export interface NodeHandle {
  id: string;
  position: Position;
  label?: string;
  title?: string;
}

// ============================================================================
// Input Name Info (for kernel node autocomplete)
// ============================================================================

export interface InputNameInfo {
  name: string;
  source_node_id: number;
  source_node_type: string;
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
  label?: string;
}

// ============================================================================
// Vue Flow Types
// ============================================================================

export interface VueFlowInput {
  node_edges: EdgeInput[];
  node_inputs: NodeInput[];
  groups: GroupInput[];
}

// ============================================================================
// Node Group Types (visual containers; organizational only)
// Mirror flowfile_core/.../schemas.py field-for-field (no OpenAPI codegen).
// ============================================================================

// mirrors schemas.GroupColor
export type GroupColor = "slate" | "blue" | "green" | "amber" | "rose" | "violet" | "cyan";

// mirrors schemas.FlowfileGroup
export interface GroupInput {
  id: number;
  name: string;
  color?: GroupColor | null;
  x_position: number;
  y_position: number;
  width: number;
  height: number;
  collapsed?: boolean;
  parent_group_id?: number | null;
}

// VueFlow node `data` payload for a group container node.
export interface GroupNodeData {
  id: number;
  label: string;
  color?: GroupColor | null;
  collapsed?: boolean;
}

// mirrors schemas.CreateGroupRequest
export interface CreateGroupRequest {
  node_ids: number[];
  name?: string;
  color?: GroupColor | null;
  x_position?: number | null;
  y_position?: number | null;
  width?: number | null;
  height?: number | null;
  parent_group_id?: number | null;
  child_group_ids?: number[];
}

// mirrors schemas.UpdateGroupRequest
export interface UpdateGroupRequest {
  name?: string;
  color?: GroupColor | null;
  x_position?: number | null;
  y_position?: number | null;
  width?: number | null;
  height?: number | null;
  collapsed?: boolean | null;
}

// mirrors schemas.NodePositionUpdate
export interface NodePositionUpdate {
  node_id: number;
  pos_x: number;
  pos_y: number;
}

// mirrors schemas.GroupBoundsUpdate
export interface GroupBoundsUpdate {
  group_id: number;
  x_position: number;
  y_position: number;
  width: number;
  height: number;
}

// mirrors schemas.UpdateLayoutRequest
export interface UpdateLayoutRequest {
  node_positions: NodePositionUpdate[];
  group_bounds: GroupBoundsUpdate[];
}

// mirrors routes.GroupOperationResponse
export interface GroupOperationResponse extends OperationResponse {
  group: GroupInput | null;
}

// ============================================================================
// Artifact Visualization Types
// ============================================================================

export interface ArtifactPublished {
  name: string;
  type_name: string;
  module: string;
}

export interface ArtifactConsumed {
  name: string;
  source_node_id: number | null;
  type_name: string;
}

export interface NodeArtifactSummary {
  published_count: number;
  consumed_count: number;
  deleted_count: number;
  published: ArtifactPublished[];
  consumed: ArtifactConsumed[];
  deleted: string[];
  kernel_id: string;
}

export interface ArtifactEdge {
  source: number;
  target: number;
  artifact_name: string;
  artifact_type: string;
  kernel_id: string;
}

export interface FlowArtifactData {
  nodes: Record<string, NodeArtifactSummary>;
  edges: ArtifactEdge[];
}
