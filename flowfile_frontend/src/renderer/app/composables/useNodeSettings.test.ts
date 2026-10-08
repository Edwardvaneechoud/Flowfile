// saveSettings is what Run, node switches and minimize await: only a real refusal may block them.

import { nextTick, ref } from "vue";
import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  updateSettings: vi.fn(),
  messageError: vi.fn(),
  disarmRefusedSave: vi.fn(),
  resolveConflict: vi.fn(),
  fingerprint: undefined as string | undefined,
}));

vi.mock("../stores/node-store", () => ({
  useNodeStore: () => ({ updateSettings: mocks.updateSettings }),
}));
vi.mock("../stores/editor-store", () => ({
  useEditorStore: () => ({ disarmRefusedSave: mocks.disarmRefusedSave }),
}));
vi.mock("../stores/flow-store", () => ({ useFlowStore: () => ({ vueFlowInstance: null }) }));
vi.mock("./useDragAndDrop", () => ({ removeCommittedEdges: vi.fn() }));
vi.mock("element-plus", () => ({ ElMessage: { error: mocks.messageError } }));
vi.mock("./settingsConflict", () => ({
  isSettingsConflict: (error: unknown) =>
    (error as { response?: { status?: number } })?.response?.status === 409,
  loadedSettingsFingerprint: () => mocks.fingerprint,
  resolveSettingsConflict: mocks.resolveConflict,
}));

import type { NodeBase } from "../types/node.types";
import { useNodeSettings } from "./useNodeSettings";

const loadedNode = () => ({ flow_id: 1, node_id: 4, is_setup: false }) as unknown as NodeBase;

describe("useNodeSettings.saveSettings", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.spyOn(console, "warn").mockImplementation(() => undefined);
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    mocks.updateSettings.mockResolvedValue({});
    mocks.fingerprint = undefined;
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
    expect(mocks.updateSettings).toHaveBeenCalledWith(nodeRef, undefined, undefined);
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

describe("useNodeSettings conflicts", () => {
  const conflict = {
    response: { status: 409, data: { detail: { code: "NODE_SETTINGS_CHANGED" } } },
  };

  beforeEach(() => {
    vi.clearAllMocks();
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    mocks.fingerprint = "fp1";
  });

  it("sends the fingerprint the node loaded with", async () => {
    mocks.updateSettings.mockResolvedValue({});
    const nodeRef = ref<NodeBase | null>(loadedNode());
    const { saveSettings } = useNodeSettings({ nodeRef });

    expect(await saveSettings()).toBe(true);
    expect(mocks.updateSettings).toHaveBeenCalledWith(nodeRef, undefined, undefined, {
      expectedFingerprint: "fp1",
    });
  });

  it("Keep mine resends without the expectation and counts as saved", async () => {
    mocks.updateSettings.mockRejectedValueOnce(conflict).mockResolvedValue({});
    mocks.resolveConflict.mockImplementation(
      async (_e: unknown, overwrite: () => Promise<void>) => {
        await overwrite();
        return "saved";
      },
    );
    const nodeRef = ref<NodeBase | null>(loadedNode());
    const onAfterSave = vi.fn();
    const { saveSettings, pushNodeData } = useNodeSettings({ nodeRef, onAfterSave });

    expect(await saveSettings()).toBe(true);
    expect(mocks.updateSettings).toHaveBeenLastCalledWith(nodeRef, undefined, undefined);
    expect(onAfterSave).toHaveBeenCalledOnce();
    expect(mocks.disarmRefusedSave).toHaveBeenCalledOnce();
    mocks.updateSettings.mockRejectedValue(new Error("down"));
    (nodeRef.value as unknown as { description: string }).description = "edited";
    expect(await pushNodeData()).toBe(false);
  });

  it("Discard mine refuses the save but lets the drawer go", async () => {
    mocks.updateSettings.mockRejectedValue(conflict);
    mocks.resolveConflict.mockResolvedValue("discarded");
    const nodeRef = ref<NodeBase | null>(loadedNode());
    const { saveSettings, pushNodeData } = useNodeSettings({ nodeRef });
    (nodeRef.value as unknown as { description: string }).description = "edited";

    expect(await saveSettings()).toBe(false);
    expect(mocks.disarmRefusedSave).toHaveBeenCalledOnce();
    expect(mocks.messageError).not.toHaveBeenCalled();
    expect(await pushNodeData()).toBe(true);
  });

  it("closing the box keeps the edited draft and blocks the leave", async () => {
    mocks.updateSettings.mockRejectedValue(conflict);
    mocks.resolveConflict.mockResolvedValue("kept-open");
    const nodeRef = ref<NodeBase | null>(loadedNode());
    const { pushNodeData } = useNodeSettings({ nodeRef });
    (nodeRef.value as unknown as { description: string }).description = "edited";

    expect(await pushNodeData()).toBe(false);
    expect(mocks.disarmRefusedSave).not.toHaveBeenCalled();
  });
});
