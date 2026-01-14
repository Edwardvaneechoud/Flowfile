// History Types - TypeScript interfaces for undo/redo functionality

// Backend response format (snake_case)
export interface HistoryStateResponse {
  can_undo: boolean;
  can_redo: boolean;
  undo_description: string | null;
  redo_description: string | null;
  undo_count: number;
  redo_count: number;
}

// Frontend format (camelCase)
export interface HistoryState {
  canUndo: boolean;
  canRedo: boolean;
  undoDescription: string | null;
  redoDescription: string | null;
  undoCount: number;
  redoCount: number;
}

// Backend response format (snake_case)
export interface UndoRedoResultResponse {
  success: boolean;
  action_description: string | null;
  error_message: string | null;
}

// Frontend format (camelCase)
export interface UndoRedoResult {
  success: boolean;
  actionDescription: string | null;
  errorMessage: string | null;
}

// Helper functions to convert between formats
export function toHistoryState(response: HistoryStateResponse): HistoryState {
  return {
    canUndo: response.can_undo,
    canRedo: response.can_redo,
    undoDescription: response.undo_description,
    redoDescription: response.redo_description,
    undoCount: response.undo_count,
    redoCount: response.redo_count,
  };
}

export function toUndoRedoResult(response: UndoRedoResultResponse): UndoRedoResult {
  return {
    success: response.success,
    actionDescription: response.action_description,
    errorMessage: response.error_message,
  };
}

export type HistoryActionType =
  | "add_node"
  | "delete_node"
  | "move_node"
  | "add_connection"
  | "delete_connection"
  | "update_settings"
  | "copy_node"
  | "paste_nodes"
  | "batch";
