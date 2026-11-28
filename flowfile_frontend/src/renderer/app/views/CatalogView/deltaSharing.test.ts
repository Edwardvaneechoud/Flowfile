import { describe, expect, it } from "vitest";
import {
  apiBase,
  isLoopbackEndpoint,
  pandasSnippet,
  profileFileText,
  safeShareName,
  shareNameError,
  tablesToOffer,
} from "./deltaSharing";

const profile = {
  profile: {
    shareCredentialsVersion: 1,
    endpoint: "https://data.example.com/api/delta-sharing",
    bearerToken: "ffk_abc",
  },
  file_name: "retail.share",
  key_prefix: "ffk_abc",
  table_urls: ["retail.sales.orders"],
};

describe("deltaSharing helpers", () => {
  it("checks share names the way core does", () => {
    expect(shareNameError("retail")).toBeNull();
    expect(shareNameError("retail-2024_q1")).toBeNull();
    expect(shareNameError("  ")).toMatch(/name/);
    expect(shareNameError("bad name")).toMatch(/letters/);
    expect(shareNameError("-leading")).toMatch(/letters/);
  });

  it("makes protocol-safe names", () => {
    expect(safeShareName("Sales Orders 2024")).toBe("Sales_Orders_2024");
    expect(safeShareName("***")).toBe("table");
  });

  it("writes the profile file clients read", () => {
    const parsed = JSON.parse(profileFileText(profile));
    expect(parsed).toEqual(profile.profile);
  });

  it("names the first shared table in the snippet", () => {
    expect(pandasSnippet(profile)).toContain('load_as_pandas(f"{profile}#retail.sales.orders")');
    expect(pandasSnippet({ ...profile, table_urls: [] })).toContain("<share>.<schema>.<table>");
  });

  it("strips the trailing slash of the API base", () => {
    expect(apiBase("http://127.0.0.1:63578/")).toBe("http://127.0.0.1:63578");
    expect(apiBase("https://host/api/")).toBe("https://host/api");
  });

  it("tells a loopback endpoint", () => {
    expect(isLoopbackEndpoint("http://127.0.0.1:63578/delta-sharing")).toBe(true);
    expect(isLoopbackEndpoint("http://localhost:8080/api/delta-sharing")).toBe(true);
    expect(isLoopbackEndpoint("https://data.example.com/delta-sharing")).toBe(false);
    expect(isLoopbackEndpoint("not a url")).toBe(false);
  });

  it("offers tables not yet shared, shareable ones first", () => {
    const eligible = [
      {
        table_id: 1,
        catalog_name: "b.t",
        default_schema_name: "b",
        default_table_name: "t",
        refusal: null,
      },
      {
        table_id: 2,
        catalog_name: "a.t",
        default_schema_name: "a",
        default_table_name: "t",
        refusal: "no",
      },
      {
        table_id: 3,
        catalog_name: "c.t",
        default_schema_name: "c",
        default_table_name: "t",
        refusal: null,
      },
    ];
    expect(tablesToOffer(eligible, [3]).map((t) => t.table_id)).toEqual([1, 2]);
  });
});
