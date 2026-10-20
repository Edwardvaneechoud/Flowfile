/**
 * The bottom dock's open and tab predicates, pure: a tab whose panel moved to its own window is
 * hidden, and its trigger (a previewed node, a run's log signal) no longer opens the dock.
 */
import type { DrawerCtx } from "../../types/drawer.types";

export interface BottomDockState {
  previewNodeId: number | null;
  isShowingLogViewer: boolean;
  displayLogViewer: boolean;
  tableOut: boolean;
  logsOut: boolean;
}

export const readBottomDockState = ({ drawer, editor, flow }: DrawerCtx): BottomDockState => ({
  previewNodeId: drawer.previewNodeId,
  isShowingLogViewer: editor.isShowingLogViewer,
  displayLogViewer: editor.displayLogViewer,
  tableOut: editor.isPoppedOut("table", flow.flowId),
  logsOut: editor.isPoppedOut("logs", flow.flowId),
});

export const bottomDockOpen = (state: BottomDockState): boolean =>
  (state.previewNodeId !== null && !state.tableOut) ||
  (state.isShowingLogViewer && !state.logsOut);

export const dataTabVisible = (state: BottomDockState): boolean => !state.tableOut;

export const logsTabVisible = (state: BottomDockState): boolean =>
  state.displayLogViewer && !state.logsOut;

export const logsTabFocused = (state: BottomDockState): boolean =>
  state.isShowingLogViewer && !state.logsOut;
