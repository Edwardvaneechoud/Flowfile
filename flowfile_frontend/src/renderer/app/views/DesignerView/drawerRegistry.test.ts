// Minimizing the right drawer saves the open node settings first; a refused save keeps it open.
// The bottom dock hides a tab whose panel is in its own window and no longer opens for it.

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
const bottomDock = drawers.find((d) => d.id === "bottomDock")!;

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

describe("bottomDock while a tab is popped out", () => {
  const tab = (id: string) => bottomDock.tabs.find((t) => t.id === id)!;
  const ctx = (previewNodeId: number | null = null, flowId = 4) =>
    ({ editor: useEditorStore(), drawer: { previewNodeId }, flow: { flowId } }) as unknown as DrawerCtx;

  beforeEach(() => {
    setActivePinia(createPinia());
  });

  it("hides the Data tab and ignores a preview while the Data window is out", () => {
    const editor = useEditorStore();
    expect(bottomDock.visibleWhen!(ctx(3))).toBe(true);
    editor.markPoppedOut("table", 4);
    expect(bottomDock.visibleWhen!(ctx(3))).toBe(false);
    expect(tab("data").visibleWhen(ctx(3))).toBe(false);
    expect(tab("logs").visibleWhen(ctx(3))).toBe(true);
    expect(bottomDock.visibleWhen!(ctx(3, 9))).toBe(true);
  });

  it("hides the Logs tab and ignores the run signal while the Logs window is out", () => {
    const editor = useEditorStore();
    editor.isShowingLogViewer = true;
    expect(bottomDock.visibleWhen!(ctx())).toBe(true);
    expect(tab("logs").focusWhen!(ctx())).toBe(true);
    editor.markPoppedOut("logs", 4);
    expect(bottomDock.visibleWhen!(ctx())).toBe(false);
    expect(tab("logs").visibleWhen(ctx())).toBe(false);
    expect(tab("logs").focusWhen!(ctx())).toBe(false);
    expect(tab("data").visibleWhen(ctx())).toBe(true);
  });

  it("offers a pop-out for both tabs of an open flow only", () => {
    expect(tab("data").popout?.kind).toBe("table");
    expect(tab("logs").popout?.kind).toBe("logs");
    expect(tab("data").popout!.enabled!(ctx())).toBe(true);
    expect(tab("logs").popout!.enabled!(ctx(null, -1))).toBe(false);
  });
});
