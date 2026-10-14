import { IS_MAC } from "../../utils/shortcuts";

type ModifierState = { altKey?: boolean; metaKey?: boolean; ctrlKey?: boolean };

/** Keys that make a node click add to (or remove from) the selection: Alt, plus VueFlow's own Cmd/Ctrl default. */
export const multiSelectionKeys = (isMac: boolean): string[] => ["Alt", isMac ? "Meta" : "Control"];

export const MULTI_SELECTION_KEYS = multiSelectionKeys(IS_MAC);

/** True for a click that toggles the node in the selection instead of opening its settings. */
export const isMultiSelectClick = (event: ModifierState | undefined, isMac = IS_MAC): boolean =>
  !!event && (!!event.altKey || !!(isMac ? event.metaKey : event.ctrlKey));
