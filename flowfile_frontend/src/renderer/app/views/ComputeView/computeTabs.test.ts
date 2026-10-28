import { describe, expect, it } from "vitest";
import { COMPUTE_TAB_KEYS, computeTabs, resolveComputeTab } from "./computeTabs";

describe("resolveComputeTab", () => {
  it("answers every current tab as itself", () => {
    for (const key of COMPUTE_TAB_KEYS) expect(resolveComputeTab(key)).toBe(key);
  });

  it("sends the retired Performance tab to Backends, where the default worker's pool lives", () => {
    expect(resolveComputeTab("performance")).toBe("backends");
  });

  it("answers null for anything else", () => {
    expect(resolveComputeTab("nope")).toBeNull();
    expect(resolveComputeTab(undefined)).toBeNull();
    expect(resolveComputeTab(["backends"])).toBeNull();
  });

  it("lists no retired tab in the tab bar or the sidebar", () => {
    expect(computeTabs.map((t) => t.key)).not.toContain("performance");
  });
});
