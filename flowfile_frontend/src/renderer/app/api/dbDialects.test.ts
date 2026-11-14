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
    expect(await getDbDialects()).toEqual([
      { ...fetched[0], auth_methods: [], saved_connection_only: false, browses_databases: false },
    ]);
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
        name: "bigquery",
        display_name: "BigQuery",
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
      "denodo",
      "snowflake",
    ]);
  });

  it("keeps the auth methods and saved-connection flag the catalog reports", async () => {
    const { getDbDialects } = await loadModule();
    const snowflake = {
      name: "snowflake",
      display_name: "Snowflake",
      file_based: false,
      default_port: 443,
      supports_ssl: false,
      available: true,
      auth_methods: ["access_token", "key_pair"],
      saved_connection_only: true,
      browses_databases: true,
    };
    mockedGet.mockResolvedValue({ data: [snowflake] });
    expect(await getDbDialects()).toEqual([snowflake]);
  });

  it("reads a missing browses_databases from an older core as false", async () => {
    const { getDbDialects } = await loadModule();
    const older = {
      name: "snowflake",
      display_name: "Snowflake",
      file_based: false,
      default_port: 443,
      supports_ssl: false,
      available: true,
      auth_methods: ["access_token"],
      saved_connection_only: true,
    };
    mockedGet.mockResolvedValue({ data: [older] });
    expect((await getDbDialects())[0].browses_databases).toBe(false);
  });

  it("marks only Snowflake as browsing databases in the fallback list", async () => {
    const { FALLBACK_DIALECTS } = await loadModule();
    expect(
      FALLBACK_DIALECTS.filter((dialect) => dialect.browses_databases).map((d) => d.name),
    ).toEqual(["snowflake"]);
  });

  it("offers every Snowflake sign-in method in the fallback list", async () => {
    const { FALLBACK_DIALECTS } = await loadModule();
    const snowflake = FALLBACK_DIALECTS.find((dialect) => dialect.name === "snowflake");
    expect(snowflake?.auth_methods).toEqual(["oauth", "access_token", "key_pair"]);
    expect(snowflake?.saved_connection_only).toBe(true);
    expect(
      FALLBACK_DIALECTS.filter((dialect) => dialect.name !== "snowflake").every(
        (dialect) => dialect.auth_methods.length === 0 && !dialect.saved_connection_only,
      ),
    ).toBe(true);
  });
  it("lists the dialects core reports as not installed, with their install line", async () => {
    const { getDbDialects, getUnavailableDbDialects, installNote, notInstalledLabel } =
      await loadModule();
    const postgres = {
      name: "postgresql",
      display_name: "PostgreSQL",
      file_based: false,
      default_port: 5432,
      supports_ssl: true,
      available: true,
    };
    const snowflake = {
      name: "snowflake",
      display_name: "Snowflake",
      file_based: false,
      default_port: 443,
      supports_ssl: false,
      available: false,
      auth_methods: ["access_token", "key_pair"],
      saved_connection_only: true,
      install_hint: 'pip install "flowfile[snowflake]"',
    };
    mockedGet.mockResolvedValue({ data: [postgres, snowflake] });

    expect((await getDbDialects()).map((dialect) => dialect.name)).toEqual(["postgresql"]);
    const missing = await getUnavailableDbDialects();
    expect(missing.map((dialect) => dialect.name)).toEqual(["snowflake"]);
    expect(notInstalledLabel(missing[0])).toBe("Snowflake (not installed)");
    expect(installNote(missing[0])).toBe(
      'Snowflake is not installed. Run: pip install "flowfile[snowflake]"',
    );
    expect(mockedGet).toHaveBeenCalledTimes(1);
  });

  it("gives no install line when core sends no hint, and lists nothing when the request fails", async () => {
    const { getUnavailableDbDialects, installNote } = await loadModule();
    const legacy = {
      name: "bigquery",
      display_name: "BigQuery",
      file_based: false,
      default_port: 443,
      supports_ssl: true,
      available: false,
      auth_methods: [],
      saved_connection_only: false,
    };
    expect(installNote(legacy)).toBeNull();
    expect(installNote({ ...legacy, install_hint: null })).toBeNull();

    mockedGet.mockRejectedValueOnce(new Error("network down"));
    expect(await getUnavailableDbDialects()).toEqual([]);
  });
});
