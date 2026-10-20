/**
 * A pop-out window's side of the designer's message channel: listen first, then report ready, since
 * a message sent before the listener exists is lost on both platforms; the designer answers with the
 * state the window should show. Reported again when the flow id changes (a Save As).
 */
import { onMounted, onUnmounted, watch, type Ref } from "vue";
import { desktop } from "../../../lib/desktop";
import type { PopoutKind, PopoutMessage } from "../../../lib/popoutWindow";

export function usePopoutChannel(options: {
  kind: PopoutKind;
  flowId: Ref<number>;
  onMessage: (message: PopoutMessage) => void;
}): void {
  let unlisten: (() => void) | null = null;
  let unmounted = false;

  const report = (flowId: number) => {
    if (flowId <= 0) return;
    void desktop
      .reportPopoutReady(options.kind, flowId)
      .catch((error) => console.warn("[popout] ready not reported to the designer:", error));
  };

  onMounted(async () => {
    const off = await desktop.onPopoutMessage(options.onMessage);
    if (unmounted) {
      off();
      return;
    }
    unlisten = off;
    report(options.flowId.value);
  });

  watch(options.flowId, (flowId) => {
    if (unlisten) report(flowId);
  });

  onUnmounted(() => {
    unmounted = true;
    unlisten?.();
    unlisten = null;
  });
}
