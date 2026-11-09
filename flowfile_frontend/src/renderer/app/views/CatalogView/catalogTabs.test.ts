import { describe, expect, it } from "vitest";
import {
  CATALOG_SECTION_GROUPS,
  catalogSectionOf,
  catalogSectionOfTab,
  catalogTabs,
  catalogTabsInSection,
  shownWithoutCompute,
} from "./catalogTabs";

describe("catalog sections", () => {
  it("assigns every tab group to exactly one section", () => {
    const groups = new Set(catalogTabs.map((t) => t.group));
    for (const group of groups) {
      const owners = (
        Object.keys(CATALOG_SECTION_GROUPS) as (keyof typeof CATALOG_SECTION_GROUPS)[]
      ).filter((s) => CATALOG_SECTION_GROUPS[s].includes(group));
      expect(owners, group).toHaveLength(1);
      expect(catalogSectionOf(group)).toBe(owners[0]);
    }
  });

  it("keeps browse, analyze and operate tabs (APIs included) in the catalog section", () => {
    for (const key of [
      "catalog",
      "favorites",
      "sql",
      "notebook",
      "visuals",
      "runs",
      "schedules",
      "alerts",
      "apis",
    ]) {
      expect(catalogSectionOfTab(key), key).toBe("catalog");
    }
  });

  it("puts only the extension pages in the extend section", () => {
    expect(catalogTabsInSection("extend").map((t) => t.key)).toEqual(["customNodes", "community"]);
  });

  it("returns null for an unknown or absent tab", () => {
    expect(catalogSectionOfTab("dashboards")).toBeNull();
    expect(catalogSectionOfTab(undefined)).toBeNull();
  });

  it("partitions every tab across the sections without loss or overlap", () => {
    const all = [...catalogTabsInSection("catalog"), ...catalogTabsInSection("extend")].map(
      (t) => t.key,
    );
    expect([...all].sort()).toEqual(catalogTabs.map((t) => t.key).sort());
  });
});

describe("the catalog where this core computes nothing (a hub's own page)", () => {
  it("shows what the catalog holds: the catalog, favorites, run history and schedules", () => {
    expect(catalogTabs.filter((t) => shownWithoutCompute(t.key)).map((t) => t.key)).toEqual([
      "catalog",
      "favorites",
      "runs",
      "schedules",
    ]);
  });

  it("hides what needs a worker", () => {
    for (const key of [
      "sql",
      "notebook",
      "visuals",
      "alerts",
      "apis",
      "customNodes",
      "community",
    ]) {
      expect(shownWithoutCompute(key), key).toBe(false);
    }
  });
});
