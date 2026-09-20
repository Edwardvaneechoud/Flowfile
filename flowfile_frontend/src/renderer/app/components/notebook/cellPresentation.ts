// Collapse state per owner id + cell id, session-local: it is a view preference, so it never
// reaches notebook YAML or python_script_input.
import { reactive } from "vue";

export interface CellPresentation {
  codeCollapsed: boolean;
  outputCollapsed: boolean;
}

// Plain Maps, not reactive ones: create-on-demand runs inside render computeds, which must not
// write to a tracked collection.
const presentations = new Map<string, Map<string, CellPresentation>>();

export function cellPresentation(ownerId: string, cellId: string): CellPresentation {
  let owner = presentations.get(ownerId);
  if (!owner) {
    owner = new Map();
    presentations.set(ownerId, owner);
  }
  let presentation = owner.get(cellId);
  if (!presentation) {
    presentation = reactive<CellPresentation>({ codeCollapsed: false, outputCollapsed: false });
    owner.set(cellId, presentation);
  }
  return presentation;
}

export function toggleCodeCollapsed(ownerId: string, cellId: string): boolean {
  const presentation = cellPresentation(ownerId, cellId);
  presentation.codeCollapsed = !presentation.codeCollapsed;
  return presentation.codeCollapsed;
}

export function toggleOutputCollapsed(ownerId: string, cellId: string): boolean {
  const presentation = cellPresentation(ownerId, cellId);
  presentation.outputCollapsed = !presentation.outputCollapsed;
  return presentation.outputCollapsed;
}

export function disposeCellPresentation(ownerId: string, cellId: string): void {
  presentations.get(ownerId)?.delete(cellId);
}

export function disposeOwnerPresentation(ownerId: string): void {
  presentations.delete(ownerId);
}
