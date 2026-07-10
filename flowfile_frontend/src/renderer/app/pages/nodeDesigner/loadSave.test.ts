// Unit test for duplicateNode: a duplicate is fresh unsaved work, so it must be
// left dirty AND must not wipe the shared "__new__" localStorage draft slot
// (regression — markSaved's discardDraft used to clear an unrelated pending
// new-node draft because sourceFile was already null).

import { setActivePinia, createPinia } from "pinia";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("element-plus", () => ({
  ElMessage: Object.assign(vi.fn(), {
    success: vi.fn(),
    error: vi.fn(),
    warning: vi.fn(),
    info: vi.fn(),
  }),
  ElMessageBox: { confirm: vi.fn() },
}));

vi.mock("../../api/nodeDesigner", () => ({
  SaveConflictError: class SaveConflictError extends Error {},
  getCustomNode: vi.fn(),
  previewCustomNode: vi.fn(),
  saveCustomNode: vi.fn(),
}));

import { getCustomNode, type GetCustomNodeResponse } from "../../api/nodeDesigner";
import { duplicateNode } from "./loadSave";
import { newDesignerState } from "./designerState";
import {
  DRAFT_KEY_PREFIX,
  NEW_NODE_DRAFT_KEY,
  useNodeDesignerStore,
} from "@/stores/node-designer-store";

const NEW_DRAFT_KEY = DRAFT_KEY_PREFIX + NEW_NODE_DRAFT_KEY;

class MemoryStorage {
  data = new Map<string, string>();
  getItem(key: string): string | null {
    return this.data.has(key) ? (this.data.get(key) as string) : null;
  }
  setItem(key: string, value: string): void {
    this.data.set(key, value);
  }
  removeItem(key: string): void {
    this.data.delete(key);
  }
}

let localStorageMock: MemoryStorage;
const g = globalThis as { localStorage?: unknown };

beforeEach(() => {
  setActivePinia(createPinia());
  vi.useFakeTimers();
  localStorageMock = new MemoryStorage();
  g.localStorage = localStorageMock;
  vi.mocked(getCustomNode).mockReset();
});

afterEach(() => {
  vi.useRealTimers();
  delete g.localStorage;
});

function mockNode(nodeName: string): void {
  const ds = newDesignerState();
  ds.node_name = nodeName;
  ds.process_code =
    "def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:\n    return inputs[0].head(3)";
  const response: GetCustomNodeResponse = {
    file_name: "original.py",
    file_hash: "hash-abc",
    code: "",
    parse_result: { mode: "designer", designer_state: ds, issues: [] },
  };
  vi.mocked(getCustomNode).mockResolvedValue(response);
}

describe("duplicateNode", () => {
  it("loads a copy with no source file, cleared name, and dirty state", async () => {
    mockNode("Original");
    const store = useNodeDesignerStore();

    await duplicateNode("original.py");

    expect(store.sourceFile).toBeNull();
    expect(store.fileHash).toBeNull();
    expect(store.designerState.node_name).toBe("");
    expect(store.isDirty).toBe(true);
  });

  it("does not wipe an unrelated pending new-node draft", async () => {
    localStorageMock.setItem(NEW_DRAFT_KEY, "unrelated-pending-draft");
    mockNode("Original");
    const store = useNodeDesignerStore();

    await duplicateNode("original.py");

    // The shared "__new__" slot must survive: duplicateNode must not discardDraft() it.
    expect(store.sourceFile).toBeNull();
    expect(localStorageMock.getItem(NEW_DRAFT_KEY)).toBe("unrelated-pending-draft");
  });
});
