import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ExchangeStoreInput, ExchangeStoreState } from "../../api/system.api";

const mocks = vi.hoisted(() => ({
  get: vi.fn(),
  set: vi.fn(),
}));

vi.mock("../../api/system.api", () => ({
  getExchangeStore: mocks.get,
  setExchangeStore: mocks.set,
}));

import { useExchangeStore } from "./useExchangeStore";

const none: ExchangeStoreState = {
  uri: null,
  connection: null,
  maxRows: null,
  source: "none",
  envOverride: false,
  saved: { uri: null, connection: null, maxRows: null },
  connections: ["exchange"],
};

const saved: ExchangeStoreState = {
  ...none,
  uri: "s3://bucket/flowfile",
  connection: "exchange",
  maxRows: 1000,
  source: "saved",
  saved: { uri: "s3://bucket/flowfile", connection: "exchange", maxRows: 1000 },
};

const httpError = (status: number, detail: unknown) =>
  Object.assign(new Error(`Request failed with status code ${status}`), {
    response: { status, data: { detail } },
  });

beforeEach(() => {
  mocks.get.mockReset().mockResolvedValue(none);
  mocks.set
    .mockReset()
    .mockImplementation(async (input: ExchangeStoreInput) =>
      input.uri ? { ...saved, saved: { ...input } } : none,
    );
});

describe("useExchangeStore", () => {
  it("loads the store as core uses it", async () => {
    const store = useExchangeStore();

    await store.load();

    expect(store.state.value).toEqual(none);
    expect(store.loadError.value).toBe("");
  });

  it("keeps the route's words when loading fails", async () => {
    mocks.get.mockRejectedValue(
      httpError(500, "FLOWFILE_EXCHANGE_MAX_ROWS must be a whole number"),
    );
    const store = useExchangeStore();

    await store.load();

    expect(store.state.value).toBeNull();
    expect(store.loadError.value).toBe("FLOWFILE_EXCHANGE_MAX_ROWS must be a whole number");
  });

  it("saves and takes the answered state", async () => {
    const store = useExchangeStore();
    await store.load();

    const next = await store.save({
      uri: "s3://bucket/flowfile",
      connection: "exchange",
      maxRows: 1000,
    });

    expect(mocks.set).toHaveBeenCalledWith({
      uri: "s3://bucket/flowfile",
      connection: "exchange",
      maxRows: 1000,
    });
    expect(next.source).toBe("saved");
    expect(store.state.value?.uri).toBe("s3://bucket/flowfile");
    expect(store.saving.value).toBe(false);
  });

  it("surfaces a refusal in the route's words and keeps the state", async () => {
    mocks.set.mockRejectedValue(
      httpError(422, "No S3 connection named 'x' can open the exchange store"),
    );
    const store = useExchangeStore();
    await store.load();

    await expect(
      store.save({ uri: "s3://bucket/flowfile", connection: "x", maxRows: null }),
    ).rejects.toThrow("No S3 connection named 'x' can open the exchange store");
    expect(store.state.value).toEqual(none);
  });

  it("clears by saving no uri", async () => {
    const store = useExchangeStore();
    mocks.get.mockResolvedValue(saved);
    await store.load();

    await store.clear();

    expect(mocks.set).toHaveBeenCalledWith({ uri: null, connection: null, maxRows: null });
    expect(store.state.value?.source).toBe("none");
  });
});
