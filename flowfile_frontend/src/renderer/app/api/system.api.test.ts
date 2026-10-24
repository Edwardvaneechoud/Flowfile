import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  get: vi.fn(),
  post: vi.fn(),
}));

vi.mock("../services/axios.config", () => ({
  default: { get: mocks.get, post: mocks.post },
}));

import { getExchangeStore, setExchangeStore } from "./system.api";

const answer = {
  uri: "s3://bucket/flowfile",
  connection: "exchange",
  max_rows: 1000,
  source: "saved",
  env_override: false,
  saved: { uri: "s3://bucket/flowfile", connection: "exchange", max_rows: 1000 },
  connections: ["exchange", "other"],
};

beforeEach(() => {
  mocks.get.mockReset();
  mocks.post.mockReset();
});

describe("the exchange store API", () => {
  it("reads the store in camelCase", async () => {
    mocks.get.mockResolvedValue({ data: answer });

    const state = await getExchangeStore();

    expect(mocks.get).toHaveBeenCalledWith("/system/exchange_store");
    expect(state).toEqual({
      uri: "s3://bucket/flowfile",
      connection: "exchange",
      maxRows: 1000,
      source: "saved",
      envOverride: false,
      saved: { uri: "s3://bucket/flowfile", connection: "exchange", maxRows: 1000 },
      connections: ["exchange", "other"],
    });
  });

  it("posts the store in the route's field names", async () => {
    mocks.post.mockResolvedValue({ data: answer });

    await setExchangeStore({ uri: "s3://bucket/flowfile", connection: "exchange", maxRows: 1000 });

    expect(mocks.post).toHaveBeenCalledWith("/system/exchange_store", {
      uri: "s3://bucket/flowfile",
      connection: "exchange",
      max_rows: 1000,
    });
  });

  it("clears with a null uri", async () => {
    mocks.post.mockResolvedValue({ data: { ...answer, uri: null, source: "none" } });

    const state = await setExchangeStore({ uri: null, connection: null, maxRows: null });

    expect(mocks.post).toHaveBeenCalledWith("/system/exchange_store", {
      uri: null,
      connection: null,
      max_rows: null,
    });
    expect(state.source).toBe("none");
  });
});
