// A settings save refused as a conflict: the user keeps or discards the draft; nothing is overwritten silently.

import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  confirm: vi.fn(),
  messageError: vi.fn(),
  messageInfo: vi.fn(),
  editorStore: {
    clearCloseFunction: vi.fn(),
    activeDrawerComponent: { name: "drawer" } as unknown,
    isDrawerOpen: true,
  },
  nodeStore: { nodeId: 4, nodeData: null as unknown },
}));

vi.mock("element-plus", () => ({
  ElMessageBox: { confirm: mocks.confirm },
  ElMessage: { error: mocks.messageError, info: mocks.messageInfo },
}));
vi.mock("../stores/editor-store", () => ({ useEditorStore: () => mocks.editorStore }));
vi.mock("../stores/node-store", () => ({ useNodeStore: () => mocks.nodeStore }));

import {
  discardDrawerDraft,
  isSettingsConflict,
  loadedSettingsFingerprint,
  resolveSettingsConflict,
} from "./settingsConflict";

const conflict = {
  response: {
    status: 409,
    data: {
      detail: {
        code: "NODE_SETTINGS_CHANGED",
        message: "These settings changed in another window since you opened them.",
        settings_fingerprint: "live",
      },
    },
  },
};

describe("isSettingsConflict", () => {
  it("recognises only the NODE_SETTINGS_CHANGED 409", () => {
    expect(isSettingsConflict(conflict)).toBe(true);
    expect(
      isSettingsConflict({ response: { status: 409, data: { detail: "Flow is busy" } } }),
    ).toBe(false);
    expect(
      isSettingsConflict({
        response: { status: 422, data: { detail: { code: "NODE_SETTINGS_CHANGED" } } },
      }),
    ).toBe(false);
    expect(isSettingsConflict(new Error("network"))).toBe(false);
  });
});

describe("loadedSettingsFingerprint", () => {
  it("is the cached node data's fingerprint for that node only", () => {
    mocks.nodeStore.nodeData = { node_id: "4", settings_fingerprint: "fp4" };
    expect(loadedSettingsFingerprint(4)).toBe("fp4");
    expect(loadedSettingsFingerprint("4")).toBe("fp4");
    expect(loadedSettingsFingerprint(5)).toBeUndefined();
    mocks.nodeStore.nodeData = { node_id: 4, settings_fingerprint: null };
    expect(loadedSettingsFingerprint(4)).toBeUndefined();
    mocks.nodeStore.nodeData = null;
    expect(loadedSettingsFingerprint(4)).toBeUndefined();
  });
});

describe("resolveSettingsConflict", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.editorStore.activeDrawerComponent = { name: "drawer" };
    mocks.editorStore.isDrawerOpen = true;
    mocks.nodeStore.nodeId = 4;
    mocks.nodeStore.nodeData = { node_id: 4, settings_fingerprint: "stale" };
  });

  it("asks with the server's message and overwrites on Keep mine", async () => {
    mocks.confirm.mockResolvedValue("confirm");
    const overwrite = vi.fn(async () => undefined);
    const discard = vi.fn();

    expect(await resolveSettingsConflict(conflict, overwrite, discard)).toBe("saved");
    expect(mocks.confirm.mock.calls[0][0]).toContain(
      "These settings changed in another window since you opened them.",
    );
    expect(overwrite).toHaveBeenCalledOnce();
    expect(discard).not.toHaveBeenCalled();
  });

  it("keeps the drawer when the overwrite itself fails", async () => {
    mocks.confirm.mockResolvedValue("confirm");
    const overwrite = vi.fn(async () => {
      throw { response: { data: { detail: "Flow is running" } } };
    });

    expect(await resolveSettingsConflict(conflict, overwrite, vi.fn())).toBe("kept-open");
    expect(mocks.messageError).toHaveBeenCalledWith(
      expect.objectContaining({ message: "Flow is running" }),
    );
  });

  it("discards on Discard mine and keeps the drawer when the box is closed", async () => {
    const overwrite = vi.fn(async () => undefined);
    const discard = vi.fn();
    mocks.confirm.mockRejectedValue("cancel");
    expect(await resolveSettingsConflict(conflict, overwrite, discard)).toBe("discarded");
    expect(discard).toHaveBeenCalledOnce();

    mocks.confirm.mockRejectedValue("close");
    expect(await resolveSettingsConflict(conflict, overwrite, discard)).toBe("kept-open");
    expect(discard).toHaveBeenCalledOnce();
    expect(overwrite).not.toHaveBeenCalled();
  });

  it("discardDrawerDraft closes the drawer and drops the node-data cache", () => {
    discardDrawerDraft();
    expect(mocks.editorStore.clearCloseFunction).toHaveBeenCalledOnce();
    expect(mocks.editorStore.activeDrawerComponent).toBeNull();
    expect(mocks.editorStore.isDrawerOpen).toBe(false);
    expect(mocks.nodeStore.nodeId).toBe(-1);
    expect(mocks.nodeStore.nodeData).toBeNull();
    expect(mocks.messageInfo).toHaveBeenCalledOnce();
  });
});
