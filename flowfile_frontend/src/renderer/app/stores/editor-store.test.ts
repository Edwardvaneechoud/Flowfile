// Run flushes the open drawer through executeDrawCloseFunction, so it must await the save.

import { setActivePinia, createPinia } from "pinia";
import { describe, expect, it } from "vitest";

import { useEditorStore } from "./editor-store";

describe("executeDrawCloseFunction", () => {
  it("waits for the registered save to finish before resolving", async () => {
    setActivePinia(createPinia());
    const store = useEditorStore();
    let finished = false;
    store.setCloseFunction(async () => {
      await new Promise((resolve) => setTimeout(resolve, 5));
      finished = true;
    });

    await store.executeDrawCloseFunction();
    expect(finished).toBe(true);
  });
});
