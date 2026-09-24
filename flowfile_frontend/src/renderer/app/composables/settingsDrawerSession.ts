// The open settings drawer's pending save: what closing the drawer persists, and when.
import { historyShortcutFor } from "./useFlowHotkeys";

const MODIFIER_KEYS = new Set(["Meta", "Control", "Shift", "Alt", "AltGraph", "CapsLock", "Fn"]);

/**
 * Whether an event inside the drawer body is the user working on the settings. Everything
 * counts except a bare modifier key and the canvas undo/redo shortcut, which closes the
 * drawer itself.
 */
export function isDrawerEdit(event: Event, isMac: boolean): boolean {
  if (event.type !== "keydown") return true;
  const key = event as KeyboardEvent;
  return !MODIFIER_KEYS.has(key.key) && historyShortcutFor(key, isMac) === null;
}

export interface DrawerCloseOptions {
  /** Persist only what the user changed (closing to undo/redo commits no defaults). */
  userEditsOnly?: boolean;
}

export interface DrawerSession {
  /** Record a user interaction with the settings. */
  noteEdit(): void;
  /**
   * Record the live node's configuration state (`is_setup` of its load-time GET /node
   * response). The settings a component shows may be a proposal or a local default, so this
   * is the only source for "never configured".
   */
  loaded(isSetup: boolean | undefined): void;
  /** Whether the user changed something that is not saved yet. */
  hasPendingEdits(): boolean;
  /** Save now (Apply); a successful save leaves nothing pending. */
  save(): Promise<unknown>;
  /**
   * The close save. It persists the user's edits since the last save and, unless
   * `userEditsOnly`, what a never-configured node shows (opening such a node configures
   * it). Nothing else: a configured node the user only looked at, and the draft fix-ups a
   * node makes on load, never create an undo step. Nothing at all for a node that is no
   * longer on the canvas. Resolves false when the save was refused.
   */
  close(options?: DrawerCloseOptions): Promise<unknown>;
}

export function createDrawerSession(options: {
  push: () => unknown;
  nodeExists: () => boolean;
}): DrawerSession {
  let edits = 0;
  let saved = 0;
  // Null until the load-time response arrived; a successful save configures the node.
  let neverConfigured: boolean | null = null;
  const save = async (): Promise<unknown> => {
    const upTo = edits;
    const result = await options.push();
    // An edit made while the save was in flight stays pending.
    if (result !== false) {
      saved = Math.max(saved, upTo);
      neverConfigured = false;
    }
    return result;
  };
  return {
    noteEdit: () => {
      edits += 1;
    },
    loaded: (isSetup) => {
      if (neverConfigured === null) neverConfigured = isSetup === false;
    },
    hasPendingEdits: () => edits > saved,
    save,
    close: async ({ userEditsOnly = false }: DrawerCloseOptions = {}) => {
      const pending = edits > saved || (!userEditsOnly && neverConfigured === true);
      return pending && options.nodeExists() ? save() : undefined;
    },
  };
}
