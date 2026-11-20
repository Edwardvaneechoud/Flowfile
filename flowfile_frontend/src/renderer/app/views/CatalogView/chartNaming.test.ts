import { describe, expect, it } from "vitest";
import {
  CHART_NAME_REQUIRED,
  chartNameError,
  dashboardChartNames,
  distinctName,
  nameFromAiTitle,
  suggestChartName,
  tableNameInSql,
} from "./chartNaming";

describe("chartNameError", () => {
  it("refuses a blank or whitespace name", () => {
    expect(chartNameError("")).toBe(CHART_NAME_REQUIRED);
    expect(chartNameError("   ")).toBe(CHART_NAME_REQUIRED);
  });

  it("accepts any other name", () => {
    expect(chartNameError(" Revenue ")).toBeNull();
  });
});

describe("tableNameInSql", () => {
  it("takes the last part of the first table read", () => {
    expect(tableNameInSql("SELECT * FROM sales.orders")).toBe("orders");
    expect(tableNameInSql('select a from hub."My schema"."Order lines" o join x')).toBe(
      "Order lines",
    );
    expect(tableNameInSql("SELECT region, SUM(r)\nFROM\n  orders GROUP BY region")).toBe("orders");
  });

  it("is null for a query that reads no table", () => {
    expect(tableNameInSql("SELECT 1")).toBeNull();
  });
});

describe("distinctName", () => {
  it("keeps a free name", () => {
    expect(distinctName("orders", ["customers"])).toBe("orders");
  });

  it("numbers a name already taken, ignoring case and spaces", () => {
    expect(distinctName("orders", [" Orders "])).toBe("orders 2");
    expect(distinctName("orders", ["orders", "orders 2", "ORDERS 3"])).toBe("orders 4");
  });
});

describe("dashboardChartNames", () => {
  it("lists the chart tiles' titles and chart names, nothing else", () => {
    const names: Record<number, string> = { 1: "orders", 2: "customers" };
    expect(
      dashboardChartNames(
        [
          { type: "viz", viz_id: 1, title: null },
          { type: "viz", viz_id: 2, title: "Top customers" },
          { type: "kpi", viz_id: 2, title: "Ignored" },
          { type: "text", viz_id: null },
          { type: "viz", viz_id: 9 },
        ],
        (id) => names[id],
      ),
    ).toEqual(["orders", "Top customers", "customers"]);
  });
});

describe("suggestChartName", () => {
  it("uses the table's name", () => {
    expect(suggestChartName({ source_type: "table", table_id: 1 }, "orders")).toBe("orders");
  });

  it("uses the table a query reads", () => {
    expect(
      suggestChartName({ source_type: "sql", sql_query: "SELECT * FROM a.customers" }, null),
    ).toBe("customers");
  });

  it("falls back to a generic name, never Untitled chart", () => {
    expect(suggestChartName({ source_type: "table", table_id: 1 }, null)).toBe("Table chart");
    expect(suggestChartName({ source_type: "sql", sql_query: "SELECT 1" }, null)).toBe(
      "Query chart",
    );
  });

  it("is distinct from the charts already on the dashboard", () => {
    expect(
      suggestChartName({ source_type: "table", table_id: 1 }, "orders", ["orders", "orders 2"]),
    ).toBe("orders 3");
  });
});

describe("nameFromAiTitle", () => {
  it("uses the title, tidied and made distinct", () => {
    expect(nameFromAiTitle("  Revenue   by region ")).toBe("Revenue by region");
    expect(nameFromAiTitle("Revenue by region", ["revenue by region"])).toBe("Revenue by region 2");
  });

  it("gives nothing for a missing or blank title, and cuts a long one", () => {
    expect(nameFromAiTitle(null)).toBeNull();
    expect(nameFromAiTitle("   ")).toBeNull();
    expect(nameFromAiTitle("x".repeat(300))).toHaveLength(120);
  });
});
