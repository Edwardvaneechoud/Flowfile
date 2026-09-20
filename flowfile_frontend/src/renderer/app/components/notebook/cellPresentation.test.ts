import { describe, it, expect } from "vitest";
import { computed, watchEffect } from "vue";
import {
  cellPresentation,
  disposeOwnerPresentation,
  toggleCodeCollapsed,
  toggleOutputCollapsed,
} from "./cellPresentation";

describe("cellPresentation", () => {
  it("creates on demand and returns the same object for the same cell", () => {
    const first = cellPresentation("owner-a", "c1");
    expect(first).toEqual({ codeCollapsed: false, outputCollapsed: false });
    expect(cellPresentation("owner-a", "c1")).toBe(first);
    expect(cellPresentation("owner-a", "c2")).not.toBe(first);
  });

  it("toggles code collapse and reports the new state", () => {
    const presentation = cellPresentation("owner-b", "c1");
    expect(toggleCodeCollapsed("owner-b", "c1")).toBe(true);
    expect(presentation.codeCollapsed).toBe(true);
    expect(presentation.outputCollapsed).toBe(false);
    expect(toggleCodeCollapsed("owner-b", "c1")).toBe(false);
    expect(presentation.codeCollapsed).toBe(false);
  });

  it("toggles output collapse independently of code collapse", () => {
    const presentation = cellPresentation("owner-c", "c1");
    toggleOutputCollapsed("owner-c", "c1");
    expect(presentation.outputCollapsed).toBe(true);
    expect(presentation.codeCollapsed).toBe(false);
  });

  it("is reactive: computed and watchEffect see toggles", () => {
    const presentation = cellPresentation("owner-d", "c1");
    const label = computed(() => (presentation.codeCollapsed ? "Expand code" : "Collapse code"));
    expect(label.value).toBe("Collapse code");

    const seen: boolean[] = [];
    const stop = watchEffect(() => seen.push(presentation.outputCollapsed), { flush: "sync" });

    toggleCodeCollapsed("owner-d", "c1");
    expect(label.value).toBe("Expand code");

    toggleOutputCollapsed("owner-d", "c1");
    toggleOutputCollapsed("owner-d", "c1");
    stop();
    expect(seen).toEqual([false, true, false]);
  });

  it("keeps owners isolated", () => {
    const left = cellPresentation("owner-e", "c1");
    const right = cellPresentation("owner-f", "c1");
    expect(left).not.toBe(right);
    toggleCodeCollapsed("owner-e", "c1");
    expect(left.codeCollapsed).toBe(true);
    expect(right.codeCollapsed).toBe(false);
  });

  it("forgets an owner's state on dispose", () => {
    const before = cellPresentation("owner-g", "c1");
    toggleCodeCollapsed("owner-g", "c1");
    toggleOutputCollapsed("owner-g", "c1");
    disposeOwnerPresentation("owner-g");

    const after = cellPresentation("owner-g", "c1");
    expect(after).not.toBe(before);
    expect(after).toEqual({ codeCollapsed: false, outputCollapsed: false });
  });
});
