// ContextMenu types

export interface ContextMenuOption {
  label: string;
  action: string;
  disabled?: boolean;
  danger?: boolean;
}

export interface ContextMenuPosition {
  x: number;
  y: number;
}
