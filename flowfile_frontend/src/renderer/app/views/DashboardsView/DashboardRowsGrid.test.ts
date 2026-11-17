// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const { computeAdHocMock, computeSavedMock } = vi.hoisted(() => ({
  computeAdHocMock: vi.fn(),
  computeSavedMock: vi.fn(),
}));

vi.mock("../../api/catalog.api", () => ({
  CatalogApi: {
    computeAdHocVisualization: computeAdHocMock,
    computeSavedVisualization: computeSavedMock,
  },
}));

import { mount, settle, type Mounted } from "../../__fixtures__/mount";
import type { DashboardFilter } from "../../types";
import DashboardRowsGrid from "./DashboardRowsGrid.vue";

const fields = [
  { fid: "region", semanticType: "nominal" },
  { fid: "amount", semanticType: "quantitative" },
];

// With no columns chosen the grid still names every column it shows.
const ALL_COLUMNS = { type: "view", query: [{ op: "raw", fields: ["region", "amount"] }] };

const rows = [
  { region: "West", amount: 1500.5 },
  { region: "East", amount: 70 },
];

const regionFilter: DashboardFilter = {
  id: "f",
  field_name: "region",
  kind: "categorical",
  state: { selected: ["West", "East"] },
  target: "all",
  target_tile_ids: [],
  datasource_id: 5,
};

const answer = (_source: unknown, payload: { workflow: { type: string }[] }) => {
  const isCount = payload.workflow.some(
    (s) => s.type === "view" && JSON.stringify(s).includes("table_row_count"),
  );
  return Promise.resolve({ rows: isCount ? [{ table_row_count: 2 }] : rows, error: null });
};

let mounted: Mounted | undefined;
afterEach(() => mounted?.unmount());

beforeEach(() => {
  vi.spyOn(console, "warn").mockImplementation(() => undefined);
  computeAdHocMock.mockReset().mockImplementation(answer);
  computeSavedMock.mockReset().mockImplementation((_id, opts) => answer(null, opts.payload));
});

const pagePayloads = (mock: typeof computeAdHocMock) =>
  mock.mock.calls.map((c) => c[1]).filter((p) => !JSON.stringify(p).includes("table_row_count"));

describe("DashboardRowsGrid", () => {
  it("reads a page and the total of a table on the worker, with the filters first", async () => {
    mounted = mount(DashboardRowsGrid, {
      source: { kind: "table", tableId: 5 },
      config: {
        columns: [{ field: "amount", label: "Amount", decimals: 2 }, { field: "region" }],
        sort_by: [],
        sort_direction: "ascending",
        page_size: 25,
      },
      fields,
      filters: [regionFilter],
    });
    await settle();

    expect(computeAdHocMock).toHaveBeenCalledTimes(2);
    const [source, page, maxRows] = computeAdHocMock.mock.calls.find(
      (c) => !JSON.stringify(c[1]).includes("table_row_count"),
    )!;
    expect(source).toEqual({ source_type: "table", table_id: 5 });
    expect(maxRows).toBe(25);
    expect(page).toEqual({
      workflow: [
        {
          type: "filter",
          filters: [{ fid: "region", rule: { type: "one of", value: ["West", "East"] } }],
        },
        { type: "view", query: [{ op: "raw", fields: ["amount", "region"] }] },
      ],
      limit: 25,
      offset: 0,
    });

    const headers = Array.from(mounted.el.querySelectorAll("th")).map((th) =>
      th.textContent?.trim(),
    );
    expect(headers).toEqual(["Amount", "region"]);
    expect(mounted.el.querySelector("tbody td")?.textContent?.trim()).toBe(
      (1500.5).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
    );
    expect(mounted.text()).toContain("1–2 of 2 rows");
  });

  it("sorts on the worker when a header is clicked, and does not count again", async () => {
    mounted = mount(DashboardRowsGrid, {
      source: { kind: "table", tableId: 5 },
      config: null,
      fields,
      filters: [],
    });
    await settle();
    computeAdHocMock.mockClear();

    (mounted.el.querySelectorAll("th")[1] as HTMLElement).click();
    await settle();
    expect(pagePayloads(computeAdHocMock)).toEqual([
      {
        workflow: [{ type: "sort", by: ["amount", "region"], sort: "ascending" }, ALL_COLUMNS],
        limit: 25,
        offset: 0,
      },
    ]);
    expect(computeAdHocMock).toHaveBeenCalledTimes(1);
    expect(mounted.el.querySelectorAll("th")[1].getAttribute("aria-sort")).toBe("ascending");
    // The tiebreaker sorts on the worker but the header marks only the clicked column.
    expect(mounted.el.querySelectorAll("th")[0].getAttribute("aria-sort")).toBe("none");

    (mounted.el.querySelectorAll("th")[1] as HTMLElement).click();
    await settle();
    expect(pagePayloads(computeAdHocMock)[1].workflow).toEqual([
      { type: "sort", by: ["amount", "region"], sort: "descending" },
      ALL_COLUMNS,
    ]);
  });

  it("reads a chart's source through the chart's own route", async () => {
    mounted = mount(DashboardRowsGrid, {
      source: { kind: "viz", vizId: 3 },
      config: { columns: [], sort_by: ["amount"], sort_direction: "descending", page_size: 10 },
      fields,
      filters: [],
    });
    await settle();

    expect(computeAdHocMock).not.toHaveBeenCalled();
    const page = computeSavedMock.mock.calls
      .map((c) => c[1])
      .find((opts) => !JSON.stringify(opts.payload).includes("table_row_count"));
    expect(page).toEqual({
      payload: {
        workflow: [{ type: "sort", by: ["amount", "region"], sort: "descending" }, ALL_COLUMNS],
        limit: 10,
        offset: 0,
      },
      maxRows: 10,
    });
  });

  it("waits for the columns before it asks for rows", async () => {
    mounted = mount(DashboardRowsGrid, {
      source: { kind: "table", tableId: 5 },
      config: null,
      fields: null,
      filters: [],
    });
    await settle();
    expect(computeAdHocMock).not.toHaveBeenCalled();
  });

  it("reports a source that is gone", async () => {
    computeAdHocMock.mockRejectedValue({ response: { status: 404 } });
    const onMissing = vi.fn();
    mounted = mount(DashboardRowsGrid, {
      source: { kind: "table", tableId: 5 },
      config: null,
      fields,
      filters: [],
      onMissing,
    });
    await settle();
    expect(onMissing).toHaveBeenCalled();
  });

  it("says when no rows match", async () => {
    computeAdHocMock.mockImplementation((_s, payload) =>
      Promise.resolve({
        rows: JSON.stringify(payload).includes("table_row_count") ? [{ table_row_count: 0 }] : [],
        error: null,
      }),
    );
    mounted = mount(DashboardRowsGrid, {
      source: { kind: "table", tableId: 5 },
      config: null,
      fields,
      filters: [],
    });
    await settle();
    expect(mounted.el.querySelector(".rows-empty")).not.toBeNull();
    expect(mounted.text()).toContain("0 rows");
  });
});
