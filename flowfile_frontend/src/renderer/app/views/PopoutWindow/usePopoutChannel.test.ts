// @vitest-environment happy-dom
// The window listens before it reports ready, hears the designer, and reports again after a Save As.
import { createApp, defineComponent, ref, type App, type Ref } from "vue";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const h = vi.hoisted(() => ({
  log: [] as string[],
  off: vi.fn(),
  desktop: { onPopoutMessage: vi.fn(), reportPopoutReady: vi.fn() },
}));

vi.mock("../../../lib/desktop", () => ({ desktop: h.desktop }));

import { usePopoutChannel } from "./usePopoutChannel";

const settle = async () => {
  for (let i = 0; i < 5; i++) await Promise.resolve();
};

describe("usePopoutChannel", () => {
  let app: App | null = null;
  let handler: ((message: unknown) => void) | null = null;

  const mount = (flowId: Ref<number>, onMessage = vi.fn()) => {
    app = createApp(
      defineComponent({
        setup() {
          usePopoutChannel({ kind: "table", flowId, onMessage });
          return () => null;
        },
      }),
    );
    app.mount(document.createElement("div"));
    return onMessage;
  };

  beforeEach(() => {
    vi.clearAllMocks();
    h.log.length = 0;
    handler = null;
    h.desktop.onPopoutMessage.mockImplementation(async (fn) => {
      h.log.push("listen");
      handler = fn;
      return h.off;
    });
    h.desktop.reportPopoutReady.mockImplementation(async (_kind, flowId) => {
      h.log.push(`ready:${flowId}`);
    });
  });

  afterEach(() => {
    app?.unmount();
    app = null;
  });

  it("listens, then reports ready for its flow, and hands messages on", async () => {
    const onMessage = mount(ref(4));
    await settle();
    expect(h.log).toEqual(["listen", "ready:4"]);
    expect(h.desktop.reportPopoutReady).toHaveBeenCalledWith("table", 4);

    const message = { type: "selection", previewNodeId: 2, previewToken: 1 };
    handler!(message);
    expect(onMessage).toHaveBeenCalledExactlyOnceWith(message);
  });

  it("reports again when the flow moved to a new id", async () => {
    const flowId = ref(4);
    mount(flowId);
    await settle();
    flowId.value = 9;
    await settle();
    expect(h.log).toEqual(["listen", "ready:4", "ready:9"]);
  });

  it("reports nothing without a flow", async () => {
    mount(ref(-1));
    await settle();
    expect(h.log).toEqual(["listen"]);
  });

  it("stops listening when the window's view goes", async () => {
    mount(ref(4));
    await settle();
    app!.unmount();
    app = null;
    expect(h.off).toHaveBeenCalledOnce();
  });
});
