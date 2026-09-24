// saveSettings is what Run, node switches and minimize await: only a real refusal may block them.

import { nextTick, ref } from "vue";
import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  updateSettings: vi.fn(),
  messageError: vi.fn(),
  disarmRefusedSave: vi.fn(),
}));

vi.mock("../stores/node-store", () => ({
  useNodeStore: () => ({ updateSettings: mocks.updateSettings }),
}));
vi.mock("../stores/editor-store", () => ({
  useEditorStore: () => ({ disarmRefusedSave: mocks.disarmRefusedSave }),
}));
vi.mock("element-plus", () => ({ ElMessage: { error: mocks.messageError } }));

import type { NodeBase } from "../types/node.types";
import { useNodeSettings } from "./useNodeSettings";

const loadedNode = () => ({ flow_id: 1, node_id: 4, is_setup: false }) as unknown as NodeBase;

describe("useNodeSettings.saveSettings", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.spyOn(console, "warn").mockImplementation(() => undefined);
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    mocks.updateSettings.mockResolvedValue({});
  });

  it("resolves true with nothing to save while the node is not loaded", async () => {
    const onBeforeSave = vi.fn(() => false);
    const { saveSettings, pushNodeData } = useNodeSettings({ nodeRef: ref(null), onBeforeSave });

    expect(await saveSettings()).toBe(true);
    expect(await pushNodeData()).toBe(true);
    expect(onBeforeSave).not.toHaveBeenCalled();
    expect(mocks.updateSettings).not.toHaveBeenCalled();
  });

  it("saves a loaded node and marks it set up", async () => {
    const nodeRef = ref<NodeBase | null>(loadedNode());
    const onAfterSave = vi.fn();
    const { saveSettings } = useNodeSettings({ nodeRef, onAfterSave });

    expect(await saveSettings()).toBe(true);
    expect(mocks.updateSettings).toHaveBeenCalledWith(nodeRef);
    expect(nodeRef.value?.is_setup).toBe(true);
    expect(onAfterSave).toHaveBeenCalledOnce();
  });

  it("disarms the drawer's pending discard once a save succeeds", async () => {
    const { saveSettings } = useNodeSettings({ nodeRef: ref<NodeBase | null>(loadedNode()) });

    expect(await saveSettings()).toBe(true);
    expect(mocks.disarmRefusedSave).toHaveBeenCalledOnce();
  });

  it("lets a drawer go when its refused settings are unchanged since they loaded", async () => {
    const nodeRef = ref<NodeBase | null>(null);
    const { pushNodeData } = useNodeSettings({ nodeRef, onBeforeSave: () => false });
    nodeRef.value = loadedNode();
    await nextTick();

    expect(await pushNodeData()).toBe(true);
    expect(mocks.updateSettings).not.toHaveBeenCalled();

    (nodeRef.value as unknown as { description: string }).description = "edited";
    expect(await pushNodeData()).toBe(false);
  });

  it("refuses when onBeforeSave returns false", async () => {
    const { saveSettings } = useNodeSettings({
      nodeRef: ref<NodeBase | null>(loadedNode()),
      onBeforeSave: () => false,
    });

    expect(await saveSettings()).toBe(false);
    expect(mocks.updateSettings).not.toHaveBeenCalled();
  });

  it("refuses and shows the server's reason when the save request fails", async () => {
    mocks.updateSettings.mockRejectedValue({ response: { data: { detail: "Flow is running" } } });
    const { saveSettings } = useNodeSettings({ nodeRef: ref<NodeBase | null>(loadedNode()) });

    expect(await saveSettings()).toBe(false);
    expect(mocks.messageError).toHaveBeenCalledWith(
      expect.objectContaining({ message: "Flow is running" }),
    );
    expect(mocks.disarmRefusedSave).not.toHaveBeenCalled();
  });
});
