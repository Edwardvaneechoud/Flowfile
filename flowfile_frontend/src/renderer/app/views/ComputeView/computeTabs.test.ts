import { describe, expect, it } from "vitest";
import { COMPUTE_TAB_KEYS, computeTabs, resolveComputeTab, showsTab } from "./computeTabs";

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

  it("shows the Hub tab to administrators, and to everyone on a server that belongs to a hub", () => {
    const shown = (isAdmin: boolean, joined: boolean) =>
      computeTabs.filter((t) => showsTab(t, isAdmin, joined)).map((t) => t.key);
    expect(shown(true, false)).toEqual([...COMPUTE_TAB_KEYS]);
    expect(shown(false, false)).toEqual(["kernels", "privacy"]);
    expect(shown(false, true)).toEqual(["kernels", "hub", "privacy"]);
  });

  it("lists no retired tab in the tab bar or the sidebar", () => {
    expect(computeTabs.map((t) => t.key)).not.toContain("performance");
  });
});
