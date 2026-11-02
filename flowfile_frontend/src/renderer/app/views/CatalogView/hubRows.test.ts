// Guards which catalog rows count as the hub's, the filter local actions use, and the
// facts a hub table's panel shows without a preview.

import { describe, it, expect } from "vitest";
import {
  HUB_MANAGED_NOTE,
  HUB_NAMESPACE_TITLE,
  HUB_PREVIEW_NOTE,
  HUB_TABLE_TITLE,
  hubTableFacts,
  isHubRow,
  withoutHubRows,
} from "./hubRows";
import type { CatalogTable } from "../../types";

const table = (over: Partial<CatalogTable> = {}) =>
  ({
    id: -7,
    name: "orders",
    row_count: 1200,
    column_count: 4,
    size_bytes: 2048,
    is_remote_storage: false,
    source: "hub",
    hub_id: 7,
    hub_namespace_id: 3,
    ...over,
  }) as CatalogTable;

describe("isHubRow", () => {
  it("is true only for rows the hub listed", () => {
    expect(isHubRow({ source: "hub" })).toBe(true);
    expect(isHubRow({ source: "local" })).toBe(false);
    expect(isHubRow({})).toBe(false);
    expect(isHubRow({ source: null })).toBe(false);
  });
});

describe("withoutHubRows", () => {
  it("drops the hub's rows and keeps the local ones in order", () => {
    const rows = [
      { id: 1, source: "local" as const },
      { id: -2, source: "hub" as const },
      { id: 3 },
      { id: -4, source: "hub" as const },
    ];
    expect(withoutHubRows(rows).map((r) => r.id)).toEqual([1, 3]);
  });

  it("leaves a local list as it is", () => {
    const rows = [{ id: 1, source: "local" as const }, { id: 2 }];
    expect(withoutHubRows(rows)).toEqual(rows);
  });
});

describe("hubTableFacts", () => {
  it("lists rows, columns, size and storage from the listing", () => {
    expect(hubTableFacts(table())).toEqual([
      { label: "Rows", value: (1200).toLocaleString() },
      { label: "Columns", value: "4" },
      { label: "Size", value: "2.0 KB" },
      { label: "Storage", value: "Hub filesystem" },
    ]);
  });

  it("says object storage and shows -- for what the hub does not know", () => {
    const facts = hubTableFacts(
      table({ row_count: null, column_count: null, size_bytes: null, is_remote_storage: true }),
    );
    expect(facts.map((f) => f.value)).toEqual(["--", "--", "--", "Object storage"]);
  });
});

describe("strings", () => {
  it("reads as the hub's catalogs are managed there", () => {
    expect(HUB_NAMESPACE_TITLE).toBe(
      "This catalog lives on the hub. Create, rename and delete it there.",
    );
    expect(HUB_TABLE_TITLE).toBe("This table lives on the hub.");
    expect(HUB_MANAGED_NOTE).toBe("This catalog is managed on the hub.");
    expect(HUB_PREVIEW_NOTE).toBe(
      "Preview isn't available for tables on the hub. Read it with a catalog reader node.",
    );
  });
});
