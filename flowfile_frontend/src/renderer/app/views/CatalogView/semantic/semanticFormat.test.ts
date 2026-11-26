import { describe, expect, it } from "vitest";
import { tableOutsideSchema } from "./semanticFormat";

describe("tableOutsideSchema", () => {
  it("names the first table of another schema", () => {
    expect(tableOutsideSchema(["shop.sales.orders", "shop.crm.customers"], "shop.sales")).toBe(
      "shop.crm.customers",
    );
  });

  it("is null when every table is in the schema, or the tables are not known", () => {
    expect(tableOutsideSchema(["shop.sales.orders", "shop.sales.lines"], "shop.sales")).toBeNull();
    expect(tableOutsideSchema(undefined, "shop.sales")).toBeNull();
  });

  it("does not take a schema whose name starts the same for this one", () => {
    expect(tableOutsideSchema(["shop.sales_eu.orders"], "shop.sales")).toBe("shop.sales_eu.orders");
  });
});
