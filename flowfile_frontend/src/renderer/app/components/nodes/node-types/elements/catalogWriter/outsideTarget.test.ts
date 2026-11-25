import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

import { ns } from "../../../../../__fixtures__/catalogTree";
import type { CatalogWriteMode } from "../../../../../types/node.types";
import {
  OUTSIDE_WRITE_MODES,
  outsideAllows,
  outsideCatalogName,
  outsideFallback,
  outsideRefusal,
  outsideSavedRefusal,
} from "./outsideTarget";
import * as outsideTarget from "./outsideTarget";

const general = ns({
  id: 1,
  name: "General",
  children: [ns({ id: 2, name: "default", level: 1, parent_id: 1 })],
});
const polaris = ns({
  id: 10,
  name: "polaris",
  kept_elsewhere: true,
  children: [ns({ id: 11, name: "sales", level: 1, parent_id: 10, kept_elsewhere: true })],
});
const hub = ns({
  id: -5,
  name: "main",
  source: "hub",
  children: [ns({ id: -6, name: "sales", level: 1, parent_id: -5, source: "hub" })],
});
const tree = [general, polaris, hub];

const base = {
  write_mode: "overwrite" as CatalogWriteMode,
  merge_keys: [] as string[],
  track_changes: false,
  partition_by: [] as string[],
};

const ALL_MODES: CatalogWriteMode[] = [
  "overwrite",
  "error",
  "append",
  "upsert",
  "update",
  "delete",
  "scd2",
  "virtual",
];

describe("outsideCatalogName", () => {
  it("names the catalog kept elsewhere by the schema's id", () => {
    expect(outsideCatalogName(tree, { source: "local", namespace_id: 11 })).toBe("polaris");
  });

  it("names it by catalog.schema when the id is not set", () => {
    expect(
      outsideCatalogName(tree, {
        source: "local",
        namespace_id: null,
        namespace_full_name: "polaris.sales",
      }),
    ).toBe("polaris");
  });

  it("names it when only the catalog row carries the flag", () => {
    const bare = ns({
      id: 20,
      name: "lake",
      kept_elsewhere: true,
      children: [ns({ id: 21, name: "raw", level: 1, parent_id: 20 })],
    });
    expect(outsideCatalogName([bare], { source: "local", namespace_id: 21 })).toBe("lake");
  });

  it("is null for a local schema, a hub schema, an unknown one and no target", () => {
    expect(outsideCatalogName(tree, { source: "local", namespace_id: 2 })).toBeNull();
    expect(
      outsideCatalogName(tree, {
        source: "hub",
        namespace_id: null,
        namespace_full_name: "main.sales",
      }),
    ).toBeNull();
    expect(outsideCatalogName(tree, { source: "local", namespace_id: 99 })).toBeNull();
    expect(outsideCatalogName(tree, { source: "local", namespace_id: null })).toBeNull();
    expect(outsideCatalogName([], { source: "local", namespace_id: 11 })).toBeNull();
    expect(outsideCatalogName(tree, null)).toBeNull();
  });
});

describe("modes and options for a target kept elsewhere", () => {
  it("allows Overwrite and Append only", () => {
    expect(OUTSIDE_WRITE_MODES).toEqual(["overwrite", "append"]);
    expect(ALL_MODES.filter((m) => outsideAllows("polaris", m))).toEqual(["overwrite", "append"]);
  });

  it("words each refused mode and option's hover title in one sentence", () => {
    // flowfile_core external_catalogs.outside_write_refusal words a refused save or run the same.
    expect(outsideRefusal("polaris", "upsert")).toBe(
      "polaris keeps this table. Flowfile can Append to it or Overwrite it; Upsert is not available there.",
    );
    const labels: [Parameters<typeof outsideRefusal>[1], string][] = [
      ["error", "Error if exists"],
      ["update", "Update"],
      ["delete", "Delete"],
      ["scd2", "SCD2"],
      ["virtual", "Virtual Table"],
      ["track_changes", "Track changes"],
      ["partition_by", "Partition by"],
    ];
    for (const [what, label] of labels) {
      expect(outsideRefusal("polaris", what)).toBe(
        `polaris keeps this table. Flowfile can Append to it or Overwrite it; ${label} is not available there.`,
      );
    }
    expect(outsideRefusal("polaris", "overwrite")).toBeNull();
    expect(outsideRefusal("polaris", "append")).toBeNull();
  });

  it("falls back from a refused mode to Overwrite and says so", () => {
    const { patch, note } = outsideFallback("polaris", {
      write_mode: "upsert",
      merge_keys: ["id"],
      track_changes: true,
      partition_by: ["grp"],
    });
    expect(patch).toEqual({
      write_mode: "overwrite",
      merge_keys: [],
      track_changes: false,
      partition_by: [],
    });
    expect(note).toBe(
      "polaris keeps this table. Flowfile can Append to it or Overwrite it, so this writer was set from Upsert to Overwrite.",
    );
    expect(outsideFallback("polaris", { ...base, write_mode: "virtual" }).patch.write_mode).toBe(
      "overwrite",
    );
  });

  it("keeps Append and drops only change tracking and partitioning", () => {
    const { patch, note } = outsideFallback("polaris", {
      ...base,
      write_mode: "append",
      track_changes: true,
      partition_by: ["grp"],
    });
    expect(patch).toEqual({ track_changes: false, partition_by: [] });
    expect(note).toBe(
      "polaris keeps this table. Flowfile can Append to it or Overwrite it, so this writer no longer asks for Track changes or Partition by.",
    );
  });

  it("has no summary line of what is not available; the drawer gives each reason as a hover title", () => {
    expect(Object.keys(outsideTarget)).not.toContain("outsideNote");
    const drawer = readFileSync(new URL("./CatalogWriter.vue", import.meta.url), "utf8");
    const template = drawer.slice(0, drawer.indexOf("<script"));
    expect(template).toContain(':title="outsideRefusal(outsideCatalog, m.value) ?? undefined"');
    expect(template).toContain(
      ":title=\"outsideRefusal(outsideCatalog, 'partition_by') ?? undefined\"",
    );
    expect(template.match(/\{\{[^}]*outsideRefusal/g)).toBeNull();
  });

  it("changes nothing on an allowed setting", () => {
    expect(outsideFallback("polaris", { ...base, write_mode: "append" })).toEqual({
      patch: {},
      note: null,
    });
  });
});

describe("a saved writer whose target is kept elsewhere", () => {
  const refused = (label: string) =>
    `polaris keeps this table. Flowfile can Append to it or Overwrite it; ${label} is not available there.`;

  it("is told what Run would refuse in core's sentence, the mode first, as core checks", () => {
    expect(
      outsideSavedRefusal("polaris", {
        write_mode: "upsert",
        track_changes: true,
        partition_by: ["grp"],
      }),
    ).toBe(refused("Upsert"));
    expect(outsideSavedRefusal("polaris", { ...base, write_mode: "virtual" })).toBe(
      refused("Virtual Table"),
    );
    expect(
      outsideSavedRefusal("polaris", {
        ...base,
        write_mode: "append",
        track_changes: true,
        partition_by: ["grp"],
      }),
    ).toBe(refused("Track changes"));
    expect(outsideSavedRefusal("polaris", { ...base, partition_by: ["grp"] })).toBe(
      refused("Partition by"),
    );
  });

  it("is told nothing when its settings are taken, or its target is an ordinary one", () => {
    expect(outsideSavedRefusal("polaris", base)).toBeNull();
    expect(outsideSavedRefusal("polaris", { ...base, write_mode: "append" })).toBeNull();
    expect(
      outsideSavedRefusal(null, { write_mode: "upsert", track_changes: true, partition_by: ["a"] }),
    ).toBeNull();
  });

  it("says nothing after a pick that changes nothing, so an earlier catalog's note goes", () => {
    const saved = { ...base, write_mode: "upsert" as CatalogWriteMode, merge_keys: ["id"] };
    const onA = outsideFallback("polaris", saved);
    expect(onA.note).toMatch(/^polaris keeps this table\./);
    const afterA = { ...saved, ...onA.patch };
    expect(outsideFallback("lakekeeper", afterA)).toEqual({ patch: {}, note: null });
    expect(outsideFallback(null, afterA)).toEqual({ patch: {}, note: null });
    // The drawer takes each pick's note as it is, and every non-hub pick asks for one.
    const drawer = readFileSync(new URL("./CatalogWriter.vue", import.meta.url), "utf8");
    const script = drawer.slice(drawer.indexOf("<script"));
    const apply = script.slice(script.indexOf("async function applyOutsideFallback"));
    const body = apply.slice(0, apply.indexOf("\n}\n"));
    const setsNote = body.indexOf("outsideFallbackNote.value = note;");
    expect(setsNote).toBeGreaterThan(-1);
    expect(setsNote).toBeLessThan(body.indexOf("if (!note) return;"));
    expect(body).not.toContain("!catalog) return");
    const onPick = script.slice(script.indexOf("async function onNamespaceChange"));
    expect(onPick.slice(0, onPick.indexOf("\n}\n"))).toContain(
      "else await applyOutsideFallback();",
    );
  });

  it("keeps its saved settings when the drawer opens: only a person's pick of the schema changes them", () => {
    const drawer = readFileSync(new URL("./CatalogWriter.vue", import.meta.url), "utf8");
    const script = drawer.slice(drawer.indexOf("<script"));
    const watcher = script.slice(script.indexOf("watch(outsideCatalog"));
    expect(watcher.slice(0, watcher.indexOf("});"))).not.toContain("applyOutsideFallback");
    const calls = script.match(/applyOutsideFallback\(/g) ?? [];
    // The definition and the one call in onNamespaceChange.
    expect(calls).toHaveLength(2);
    const onPick = script.slice(script.indexOf("async function onNamespaceChange"));
    expect(onPick.slice(0, onPick.indexOf("\n}\n"))).toContain("applyOutsideFallback(");
  });
});

describe("an ordinary target is unchanged", () => {
  it("allows every mode, refuses nothing and changes nothing", () => {
    for (const mode of ALL_MODES) {
      expect(outsideAllows(null, mode)).toBe(true);
      expect(outsideRefusal(null, mode)).toBeNull();
      expect(
        outsideFallback(null, {
          write_mode: mode,
          merge_keys: ["id"],
          track_changes: true,
          partition_by: ["a"],
        }),
      ).toEqual({ patch: {}, note: null });
    }
    expect(outsideRefusal(null, "track_changes")).toBeNull();
    expect(outsideRefusal(null, "partition_by")).toBeNull();
  });
});
