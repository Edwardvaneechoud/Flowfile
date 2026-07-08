import { onMounted, onUnmounted } from "vue";

/**
 * macOS screen-capture overlays (Cmd+Shift+4/5) swallow the keyup of the
 * modifiers held when they open: VueFlow's useKeyPress never sees Shift
 * released, so the canvas latches into rubber-band-selection mode — every
 * plain drag draws a selection box instead of panning — until Shift is
 * physically pressed again. The same swallow latches Meta (multi-select /
 * zoom activation on macOS).
 *
 * Pointer events carry live modifier state, so a shift-less mouse move over a
 * pane still flagged `selection` proves the keyup was lost. Replay keyups on
 * `document` (where useKeyPress listens) for each modifier the event reports
 * as released, unlatching before the user's next drag starts.
 */
export function useStaleModifierGuard(): void {
  const onPointerMove = (event: PointerEvent) => {
    if (event.shiftKey) return;
    const target = event.target as HTMLElement | null;
    if (typeof target?.closest !== "function" || !target.closest(".vue-flow__pane.selection")) {
      return;
    }
    const stale: Array<[string, string]> = [["Shift", "ShiftLeft"]];
    if (!event.metaKey) stale.push(["Meta", "MetaLeft"]);
    if (!event.ctrlKey) stale.push(["Control", "ControlLeft"]);
    for (const [key, code] of stale) {
      document.dispatchEvent(new KeyboardEvent("keyup", { key, code }));
    }
  };

  onMounted(() => window.addEventListener("pointermove", onPointerMove));
  onUnmounted(() => window.removeEventListener("pointermove", onPointerMove));
}
