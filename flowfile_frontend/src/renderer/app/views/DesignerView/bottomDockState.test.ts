// The dock opens for a preview or a run's logs, never for a tab that lives in its own window.
import { describe, expect, it } from "vitest";
import {
  bottomDockOpen,
  dataTabVisible,
  logsTabFocused,
  logsTabVisible,
  type BottomDockState,
} from "./bottomDockState";

const state = (overrides: Partial<BottomDockState> = {}): BottomDockState => ({
  previewNodeId: null,
  isShowingLogViewer: false,
  displayLogViewer: true,
  tableOut: false,
  logsOut: false,
  ...overrides,
});

describe("bottomDockOpen", () => {
  it("opens for a previewed node or the log signal", () => {
    expect(bottomDockOpen(state())).toBe(false);
    expect(bottomDockOpen(state({ previewNodeId: 3 }))).toBe(true);
    expect(bottomDockOpen(state({ isShowingLogViewer: true }))).toBe(true);
  });

  it("ignores the trigger of a tab that is in its own window", () => {
    expect(bottomDockOpen(state({ previewNodeId: 3, tableOut: true }))).toBe(false);
    expect(bottomDockOpen(state({ isShowingLogViewer: true, logsOut: true }))).toBe(false);
    expect(bottomDockOpen(state({ previewNodeId: 3, isShowingLogViewer: true, tableOut: true }))).toBe(
      true,
    );
    expect(bottomDockOpen(state({ previewNodeId: 3, isShowingLogViewer: true, logsOut: true }))).toBe(
      true,
    );
    expect(
      bottomDockOpen(
        state({ previewNodeId: 3, isShowingLogViewer: true, tableOut: true, logsOut: true }),
      ),
    ).toBe(false);
  });
});

describe("the tabs", () => {
  it("hide while popped out", () => {
    expect(dataTabVisible(state())).toBe(true);
    expect(dataTabVisible(state({ tableOut: true }))).toBe(false);
    expect(logsTabVisible(state())).toBe(true);
    expect(logsTabVisible(state({ logsOut: true }))).toBe(false);
    expect(logsTabVisible(state({ displayLogViewer: false }))).toBe(false);
  });

  it("logs take focus on the run signal only while in the dock", () => {
    expect(logsTabFocused(state({ isShowingLogViewer: true }))).toBe(true);
    expect(logsTabFocused(state({ isShowingLogViewer: true, logsOut: true }))).toBe(false);
    expect(logsTabFocused(state())).toBe(false);
  });
});
