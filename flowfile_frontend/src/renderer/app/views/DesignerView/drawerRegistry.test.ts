// Minimizing the right drawer saves the open node settings first; a refused save keeps it open.
// The code generator is DesignerView's split pane, so the drawer neither shows nor closes it.

import { setActivePinia, createPinia } from "pinia";
import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("./NodeSettingsDrawer.vue", () => ({ default: {} }));
vi.mock("./LogViewer/LogViewer.vue", () => ({ default: {} }));
vi.mock("../../features/designer/editor/results.vue", () => ({ default: {} }));
vi.mock("../../features/ai/AiAssistant.vue", () => ({ default: {} }));
vi.mock("../../features/designer/dataPreview.vue", () => ({ default: {} }));
vi.mock("element-plus", () => ({ ElMessage: { warning: vi.fn() } }));

import { nextMinimizedState } from "../../components/common/DraggableItem/minimize";
import { useEditorStore } from "../../stores/editor-store";
import type { DrawerCtx } from "../../types/drawer.types";
import { drawers } from "./drawerRegistry";

const rightDrawer = drawers.find((d) => d.id === "rightDrawer")!;

const openSettings = (save: () => Promise<unknown>) => {
  const editor = useEditorStore();
  const drawerComponent = { name: "CloudStorageWriter" };
  editor.openDrawer(drawerComponent, { title: "Write", intro: "" });
  editor.showFlowResult = true;
  editor.setCloseFunction(save);
  const ctx = { editor, node: { nodeId: 7 } } as unknown as DrawerCtx;
  const minimize = () => nextMinimizedState(false, () => rightDrawer.onMinimize!(ctx));
  return { editor, drawerComponent, minimize };
};

describe("rightDrawer.onMinimize", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
  });

  it("saves, clears the save and closes every tab", async () => {
    const save = vi.fn(async () => true);
    const { editor, minimize } = openSettings(save);

    expect(await minimize()).toBe(true);
    expect(save).toHaveBeenCalledOnce();
    expect(editor.drawCloseFunction).toBeNull();
    expect(editor.isDrawerOpen).toBe(false);
    expect(editor.activeDrawerComponent).toBeNull();
    expect(editor.showFlowResult).toBe(false);
  });

  it("keeps the drawer, its node component and its save when the save is refused", async () => {
    const save = vi.fn(async () => false);
    const { editor, drawerComponent, minimize } = openSettings(save);

    expect(await minimize()).toBe(false);
    expect(editor.isDrawerOpen).toBe(true);
    expect(editor.activeDrawerComponent).toStrictEqual(drawerComponent);
    expect(editor.drawCloseFunction).toBe(save);
    expect(editor.showFlowResult).toBe(true);
  });

  it("stays open with the edits when the save is refused because a flow runs", async () => {
    const save = vi.fn(async () => false);
    const { editor, minimize } = openSettings(save);
    editor.isRunning = true;

    expect(await minimize()).toBe(false);
    expect(editor.isDrawerOpen).toBe(true);
    expect(editor.drawCloseFunction).toBe(save);
  });

  it("closes results without a settings save when no node is open, leaving the code pane", async () => {
    const editor = useEditorStore();
    const save = vi.fn(async () => false);
    editor.setCloseFunction(save);
    editor.showFlowResult = true;
    editor.setCodeGeneratorVisibility(true);
    const ctx = { editor, node: { nodeId: -1 } } as unknown as DrawerCtx;

    expect(await nextMinimizedState(false, () => rightDrawer.onMinimize!(ctx))).toBe(true);
    expect(save).not.toHaveBeenCalled();
    expect(editor.showFlowResult).toBe(false);
    expect(editor.showCodeGenerator).toBe(true);
  });
});

describe("rightDrawer visibility", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
  });

  it("has only Settings and Results tabs", () => {
    expect(rightDrawer.tabs.map((t) => t.id)).toEqual(["settings", "results"]);
  });

  it("stays closed when only the code pane is open", () => {
    const editor = useEditorStore();
    editor.setCodeGeneratorVisibility(true);
    const ctx = { editor } as unknown as DrawerCtx;
    expect(rightDrawer.visibleWhen!(ctx)).toBe(false);

    editor.showFlowResult = true;
    expect(rightDrawer.visibleWhen!(ctx)).toBe(true);
  });
});

describe("hideAllPanels", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
  });

  it("closes the code pane unless asked to keep it", () => {
    const editor = useEditorStore();
    editor.setCodeGeneratorVisibility(true);
    editor.showFlowResult = true;

    editor.hideAllPanels({ keepCodePane: true });
    expect(editor.showFlowResult).toBe(false);
    expect(editor.showCodeGenerator).toBe(true);

    editor.hideAllPanels();
    expect(editor.showCodeGenerator).toBe(false);
  });
});
