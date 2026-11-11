// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createApp, defineComponent, h, nextTick, reactive, ref, type App } from "vue";
import UndoRedoControls from "./UndoRedoControls.vue";
import { useFlowHotkeys } from "../../composables/useFlowHotkeys";
import { IS_MAC } from "../../utils/shortcuts";

const mocks = vi.hoisted(() => ({
  flow: { flowId: 7, canUndo: true, canRedo: true, requestReload: vi.fn() },
  node: { flow_id: 7, nodeId: -1 },
  editor: {
    activeDrawerComponent: null,
    hasPendingDrawerEdits: vi.fn(() => false),
    executeDrawCloseFunctionOnce: vi.fn(async () => true),
  },
  clearResults: vi.fn(),
  clearPreview: vi.fn(),
  recover: vi.fn(),
  error: vi.fn(),
  flush: vi.fn(async (): Promise<void> => undefined),
  idle: vi.fn(async (): Promise<void> => undefined),
  undo: vi.fn(),
  redo: vi.fn(),
}));
vi.mock("../../api", () => ({ FlowApi: { undo: mocks.undo, redo: mocks.redo } }));
vi.mock("../../stores/flow-store", () => ({ useFlowStore: () => mocks.flow }));
vi.mock("../../stores/column-store", () => ({ useNodeStore: () => mocks.node }));
vi.mock("../../stores/editor-store", () => ({ useEditorStore: () => mocks.editor }));
vi.mock("../../stores/results-store", () => ({
  useResultsStore: () => ({ clearFlowResults: mocks.clearResults }),
}));
vi.mock("../../stores/drawer-store", () => ({
  useDrawerStore: () => ({ clearPreview: mocks.clearPreview }),
}));
vi.mock("../../services/axios.config", () => ({ whenMutationsIdle: mocks.idle }));
vi.mock("../../services/mutationChannel", () => ({ flushPendingEdits: mocks.flush }));
vi.mock("../../services/mutationFailure", () => ({ recoverFromFailedMutation: mocks.recover }));
vi.mock("element-plus", () => ({ ElMessage: { error: mocks.error } }));

const settle = async () => {
  await new Promise((resolve) => setTimeout(resolve, 0));
  await nextTick();
};
let app: App;
let host: HTMLDivElement;
const visible = ref(true);

// Mount the real controls: shortcuts and actual button clicks reach the same public handlers.
const mount = () => {
  app = createApp(
    defineComponent({
      setup() {
        const controls = ref<InstanceType<typeof UndoRedoControls>>();
        useFlowHotkeys({
          flowId: () => mocks.node.flow_id,
          undo: () => controls.value?.handleUndo(),
          redo: () => controls.value?.handleRedo(),
          selectAll: vi.fn(),
          newFlow: vi.fn(),
          save: vi.fn(),
          run: vi.fn(),
          toggleCodeGenerator: vi.fn(),
          openFlowSettings: vi.fn(),
          openFile: vi.fn(),
          toggleAiDrawer: vi.fn(),
        });
        return () => (visible.value ? h(UndoRedoControls, { ref: controls }) : null);
      },
    }),
  );
  app.component(
    "ElPopover",
    defineComponent({
      inheritAttrs: false,
      setup(_, { slots }) {
        return () => slots.reference?.();
      },
    }),
  );
  host = document.createElement("div");
  document.body.append(host);
  app.mount(host);
};
const key = (action: "undo" | "redo") => {
  const event = new KeyboardEvent("keydown", {
    key: "z",
    shiftKey: action === "redo",
    metaKey: IS_MAC,
    ctrlKey: !IS_MAC,
    bubbles: true,
    cancelable: true,
  });
  document.body.dispatchEvent(event);
  return event;
};

beforeEach(() => {
  vi.resetAllMocks();
  Object.assign(mocks.flow, { flowId: 7, canUndo: true, canRedo: true });
  Object.assign(mocks.node, { flow_id: 7, nodeId: -1 });
  mocks.editor.hasPendingDrawerEdits.mockReturnValue(false);
  mocks.editor.executeDrawCloseFunctionOnce.mockResolvedValue(true);
  mocks.undo.mockResolvedValue({ success: true });
  mocks.redo.mockResolvedValue({ success: true });
  visible.value = true;
});
afterEach(() => {
  app?.unmount();
  host?.remove();
});

describe("undo/redo dispatch and queue", () => {
  it("runs rapid mixed keyboard and button presses in order, without duplicate listeners", async () => {
    // Two independent histories; boundary actions must not issue a mutation.
    const positions = reactive({ 7: 2, 8: 1 });
    const order: string[] = [];
    const update = () => {
      mocks.flow.canUndo = positions[7] > 0;
      mocks.flow.canRedo = positions[7] < 3;
    };
    mocks.undo.mockImplementation(async (id: 7 | 8) => {
      order.push("undo");
      positions[id]--;
      update();
      return { success: true };
    });
    mocks.redo.mockImplementation(async (id: 7 | 8) => {
      order.push("redo");
      positions[id]++;
      update();
      return { success: true };
    });
    mount();
    key("undo");
    host.querySelectorAll<HTMLButtonElement>("button")[1].click();
    key("undo");
    key("undo");
    key("undo");
    key("redo");
    key("redo");
    key("redo");
    key("redo");
    key("undo");
    await settle();
    expect(order).toEqual(["undo", "redo", "undo", "undo", "redo", "redo", "redo", "undo"]);
    expect(positions).toEqual({ 7: 2, 8: 1 });
    expect(mocks.clearResults.mock.calls).toEqual(order.map(() => [7]));
  });

  it("does not throw with an absent toolbar or consume history keys with no flow", async () => {
    mount();
    visible.value = false;
    await nextTick();
    key("undo");
    await settle();
    expect(mocks.undo).not.toHaveBeenCalled();
    mocks.node.flow_id = mocks.flow.flowId = -1;
    expect(key("undo").defaultPrevented).toBe(false);
  });
});
