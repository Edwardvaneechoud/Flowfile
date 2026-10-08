// @vitest-environment happy-dom
// Popping out is a move: the dock closes and remembers the flow until its window is gone again.
import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  desktop: {
    openNotebookWindow: vi.fn(),
    focusNotebookWindow: vi.fn(),
    closeNotebookWindow: vi.fn(),
    listNotebookWindows: vi.fn(),
    onNotebookWindowClosed: vi.fn(),
  },
  editorStore: {
    poppedOut: [] as number[],
    markNotebookPoppedOut: vi.fn((id: number) => mocks.editorStore.poppedOut.push(id)),
    clearNotebookPoppedOut: vi.fn((id: number) => {
      mocks.editorStore.poppedOut = mocks.editorStore.poppedOut.filter((x) => x !== id);
    }),
    isNotebookPoppedOut: vi.fn((id: number) => mocks.editorStore.poppedOut.includes(id)),
    setCodeGeneratorVisibility: vi.fn(),
    openCodePane: vi.fn(),
  },
  messageError: vi.fn(),
}));

vi.mock("../../lib/desktop", () => ({ desktop: mocks.desktop }));
vi.mock("../stores/editor-store", () => ({ useEditorStore: () => mocks.editorStore }));
vi.mock("element-plus", () => ({ ElMessage: { error: mocks.messageError } }));

import { _resetForTests, useNotebookPopout } from "./useNotebookPopout";

const settle = async () => {
  for (let i = 0; i < 5; i++) await Promise.resolve();
};

describe("useNotebookPopout", () => {
  let onClosed: ((flowId: number) => void) | null = null;

  beforeEach(() => {
    vi.clearAllMocks();
    _resetForTests();
    mocks.editorStore.poppedOut = [];
    onClosed = null;
    mocks.desktop.onNotebookWindowClosed.mockImplementation(async (handler) => {
      onClosed = handler;
      return () => undefined;
    });
    mocks.desktop.listNotebookWindows.mockResolvedValue([]);
    mocks.desktop.openNotebookWindow.mockResolvedValue(undefined);
    mocks.desktop.closeNotebookWindow.mockResolvedValue(undefined);
    mocks.desktop.focusNotebookWindow.mockResolvedValue(undefined);
    vi.spyOn(console, "error").mockImplementation(() => undefined);
  });

  it("opens the window on the pop-out route, marks the flow and closes the dock", async () => {
    const { popOut, isPoppedOut } = useNotebookPopout();

    expect(await popOut(4)).toBe(true);
    expect(mocks.desktop.openNotebookWindow).toHaveBeenCalledWith(4, {
      url: `${window.location.origin}${window.location.pathname}#/notebook?flow=4`,
      name: "flowfile-notebook-4",
    });
    expect(isPoppedOut(4)).toBe(true);
    expect(mocks.editorStore.setCodeGeneratorVisibility).toHaveBeenCalledWith(false);
  });

  it("keeps the dock when the window could not open", async () => {
    mocks.desktop.openNotebookWindow.mockRejectedValue(new Error("blocked"));
    const { popOut, isPoppedOut } = useNotebookPopout();

    expect(await popOut(4)).toBe(false);
    expect(isPoppedOut(4)).toBe(false);
    expect(mocks.messageError).toHaveBeenCalledOnce();
    expect(mocks.editorStore.setCodeGeneratorVisibility).not.toHaveBeenCalled();
  });

  it("forgets a flow whose window closed by itself", async () => {
    const { popOut, isPoppedOut } = useNotebookPopout();
    await popOut(4);
    await settle();

    expect(onClosed).not.toBeNull();
    onClosed!(4);
    expect(isPoppedOut(4)).toBe(false);
  });

  it("bringing back closes the window and reopens the pane in notebook mode", async () => {
    const { popOut, bringBack, isPoppedOut } = useNotebookPopout();
    await popOut(4);

    await bringBack(4);
    expect(mocks.desktop.closeNotebookWindow).toHaveBeenCalledWith(4);
    expect(isPoppedOut(4)).toBe(false);
    expect(mocks.editorStore.openCodePane).toHaveBeenCalledWith("notebook");
  });

  it("adopts the windows already open and installs the listener once", async () => {
    mocks.desktop.listNotebookWindows.mockResolvedValue([2, 3]);
    const first = useNotebookPopout();
    await settle();
    expect(first.isPoppedOut(2)).toBe(true);
    expect(first.isPoppedOut(3)).toBe(true);

    useNotebookPopout();
    expect(mocks.desktop.onNotebookWindowClosed).toHaveBeenCalledTimes(1);
    expect(mocks.desktop.listNotebookWindows).toHaveBeenCalledTimes(1);
  });
});
