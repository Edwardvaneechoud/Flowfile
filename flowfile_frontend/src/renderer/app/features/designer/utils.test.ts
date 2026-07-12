import { describe, expect, it, vi } from "vitest";

// utils.ts pulls in the axios baseURL resolver (touches window) and the node
// template fetcher; stub both so the pure icon-contract logic imports cleanly.
vi.mock("../../../config/constants", () => ({ flowfileCorebaseURL: "http://core/" }));
vi.mock("../../composables/useNodes", () => ({ fetchNodeTemplates: async () => [] }));

import { STANDARD_NODE_ICONS, getImageUrl, isBuiltinIcon } from "./utils";

describe("standard node icons", () => {
  it("offers PNG twins only (the community store requires PNG)", () => {
    expect(STANDARD_NODE_ICONS.length).toBeGreaterThan(0);
    for (const name of STANDARD_NODE_ICONS) {
      expect(name.endsWith(".png")).toBe(true);
    }
  });

  it("registers every offered icon as a built-in", () => {
    // A name offered but absent from BUILTIN_ICONS silently 404s (styleguide),
    // so the picker set must be a subset of the bundled built-ins.
    for (const name of STANDARD_NODE_ICONS) {
      expect(isBuiltinIcon(name)).toBe(true);
    }
  });

  it("excludes brand/connector marks and the default", () => {
    expect(STANDARD_NODE_ICONS).not.toContain("google_analytics.png");
    expect(STANDARD_NODE_ICONS).not.toContain("kafka_source.png");
    expect(STANDARD_NODE_ICONS).not.toContain("user-defined-icon.png");
  });

  it("includes representative operation glyphs", () => {
    expect(STANDARD_NODE_ICONS).toContain("group_by.png");
    expect(STANDARD_NODE_ICONS).toContain("filter.png");
    expect(STANDARD_NODE_ICONS).toContain("join.png");
  });

  it("resolves a standard PNG as a bundled asset, not a backend URL", () => {
    const url = getImageUrl("group_by.png");
    expect(url).not.toContain("user_defined_components/icon");
    expect(url).toContain("group_by.png");
  });
});
