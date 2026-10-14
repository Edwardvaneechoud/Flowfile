import { IS_MAC } from "../../utils/shortcuts";

type ModifierState = { altKey?: boolean; metaKey?: boolean; ctrlKey?: boolean };

/**
 * True for a click that toggles the node in the selection instead of opening its settings:
 * Alt, or the platform's own Cmd/Ctrl. The canvas turns VueFlow's key listener off, so this
 * is the only place that decides it.
 */
export const isMultiSelectClick = (event: ModifierState | undefined, isMac = IS_MAC): boolean =>
  !!event && (!!event.altKey || !!(isMac ? event.metaKey : event.ctrlKey));
