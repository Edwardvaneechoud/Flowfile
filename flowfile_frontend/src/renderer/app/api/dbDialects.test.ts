import { beforeEach, describe, expect, it, vi } from "vitest";
import axios from "axios";

vi.mock("axios");

const mockedGet = vi.mocked(axios.get);

const loadModule = async () => {
  vi.resetModules();
  return import("./dbDialects");
};

describe("getDbDialects", () => {
  beforeEach(() => {
    mockedGet.mockReset();
  });

  it("falls back to the built-in dialects when the request fails, then retries", async () => {
    const { getDbDialects, FALLBACK_DIALECTS } = await loadModule();

    mockedGet.mockRejectedValueOnce(new Error("network down"));
    expect(await getDbDialects()).toEqual(FALLBACK_DIALECTS);

    const fetched = [
      {
        name: "duckdb",
        display_name: "DuckDB",
        file_based: true,
        default_port: null,
        supports_ssl: false,
        available: true,
      },
    ];
    mockedGet.mockResolvedValueOnce({ data: fetched });
    expect(await getDbDialects()).toEqual(fetched);
    expect(mockedGet).toHaveBeenCalledTimes(2);
  });

  it("caches the successful response and filters unavailable dialects", async () => {
    const { getDbDialects } = await loadModule();

    const fetched = [
      {
        name: "postgresql",
        display_name: "PostgreSQL",
        file_based: false,
        default_port: 5432,
        supports_ssl: true,
        available: true,
      },
      {
        name: "snowflake",
        display_name: "Snowflake",
        file_based: false,
        default_port: 443,
        supports_ssl: true,
        available: false,
      },
    ];
    mockedGet.mockResolvedValue({ data: fetched });

    const result = await getDbDialects();
    expect(result.map((dialect) => dialect.name)).toEqual(["postgresql"]);

    await getDbDialects();
    expect(mockedGet).toHaveBeenCalledTimes(1);
  });

  it("falls back when the catalog is empty", async () => {
    const { getDbDialects, FALLBACK_DIALECTS } = await loadModule();
    mockedGet.mockResolvedValue({ data: [] });
    expect(await getDbDialects()).toEqual(FALLBACK_DIALECTS);
  });

  it("keeps the fallback list in sync with the backend's built-in dialects", async () => {
    const { FALLBACK_DIALECTS } = await loadModule();
    expect(FALLBACK_DIALECTS.map((dialect) => dialect.name)).toEqual([
      "postgresql",
      "mysql",
      "sqlite",
      "duckdb",
      "mssql",
      "snowflake",
    ]);
  });

  it("carries the snowflake form metadata in the fallback entry", async () => {
    const { FALLBACK_DIALECTS } = await loadModule();
    const snowflake = FALLBACK_DIALECTS.find((dialect) => dialect.name === "snowflake");
    expect(snowflake?.extra_fields?.map((field) => field.name)).toEqual([
      "account",
      "warehouse",
      "role",
    ]);
    expect(snowflake?.extra_fields?.[0].required).toBe(true);
    expect(snowflake?.hidden_fields).toEqual(["host", "port", "ssl"]);
  });
});
