// The cloud reader/writer drawers pick their body from this.

import { describe, expect, it } from "vitest";

import { settingsLoadView } from "./settingsLoadView";

const LOAD_ERROR =
  "Could not load this node's settings. Check that Flowfile is running, then retry.";

describe("settingsLoadView", () => {
  it("shows the skeleton while a load, or a Retry that cleared the error, is in flight", () => {
    expect(settingsLoadView(false, null)).toBe("loading");
  });

  it("shows the error with Retry after a failed load, not the skeleton", () => {
    expect(settingsLoadView(false, LOAD_ERROR)).toBe("error");
  });

  it("shows the form once the settings loaded", () => {
    expect(settingsLoadView(true, null)).toBe("form");
  });
});
