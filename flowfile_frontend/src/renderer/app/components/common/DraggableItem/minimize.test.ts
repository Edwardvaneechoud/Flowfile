import { describe, expect, it, vi } from "vitest";

import { nextMinimizedState } from "./minimize";

describe("nextMinimizedState", () => {
  it("minimizes after onMinimize finishes", async () => {
    const onMinimize = vi.fn(async () => undefined);
    expect(await nextMinimizedState(false, onMinimize)).toBe(true);
    expect(onMinimize).toHaveBeenCalledOnce();
  });

  it("stays open when onMinimize resolves false (a refused save)", async () => {
    expect(await nextMinimizedState(false, async () => false)).toBe(false);
    expect(await nextMinimizedState(false, () => false)).toBe(false);
  });

  it("toggles without a handler", async () => {
    expect(await nextMinimizedState(false, null)).toBe(true);
    expect(await nextMinimizedState(true, undefined)).toBe(false);
  });

  it("restores a minimized panel without calling onMinimize", async () => {
    const onMinimize = vi.fn(() => false);
    expect(await nextMinimizedState(true, onMinimize)).toBe(false);
    expect(onMinimize).not.toHaveBeenCalled();
  });
});
