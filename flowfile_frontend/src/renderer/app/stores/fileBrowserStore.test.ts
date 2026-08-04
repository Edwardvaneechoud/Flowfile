// Unit tests for the file-browser path memory: the existing local contexts must keep
// round-tripping, and a cloud path must never be replayed against a different
// connection. Pinia + a fake localStorage are set up per test.

import { createPinia, setActivePinia } from "pinia";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { useFileBrowserStore } from "./fileBrowserStore";

function fakeLocalStorage(): Storage {
  const store: Record<string, string> = {};
  return {
    getItem: (key: string) => (key in store ? store[key] : null),
    setItem: (key: string, value: string) => {
      store[key] = String(value);
    },
    removeItem: (key: string) => {
      delete store[key];
    },
    clear: () => {
      for (const key of Object.keys(store)) delete store[key];
    },
    key: (index: number) => Object.keys(store)[index] ?? null,
    get length() {
      return Object.keys(store).length;
    },
  } as Storage;
}

beforeEach(() => {
  globalThis.localStorage = fakeLocalStorage();
  setActivePinia(createPinia());
});

afterEach(() => {
  globalThis.localStorage.clear();
});

describe("local contexts", () => {
  it("round-trips a path per context", () => {
    const store = useFileBrowserStore();
    store.setCurrentPath("flows", "/home/me/flows");
    store.setCurrentPath("dataFiles", "/home/me/data");

    expect(store.getCurrentPath("flows")).toBe("/home/me/flows");
    expect(store.getCurrentPath("dataFiles")).toBe("/home/me/data");
  });

  it("records no scope when none is given", () => {
    const store = useFileBrowserStore();
    store.setCurrentPath("output", "/home/me/out");
    expect(store.getScope("output")).toBeNull();
    expect(localStorage.getItem("fileBrowser_output_lastScope")).toBeNull();
  });

  it("does not persist paths inside Flowfile's internal storage", () => {
    const store = useFileBrowserStore();
    store.setCurrentPath("flows", "/home/me/.flowfile/flows");
    expect(localStorage.getItem("fileBrowser_flows_lastPath")).toBeNull();
  });
});

describe("cloud contexts", () => {
  it("remembers the path together with its scope", () => {
    const store = useFileBrowserStore();
    store.setCurrentPath("cloudRead", "s3://prod-bucket/data", "cloud:s3:prod");

    expect(store.getCurrentPath("cloudRead")).toBe("s3://prod-bucket/data");
    expect(store.getScope("cloudRead")).toBe("cloud:s3:prod");
    expect(localStorage.getItem("fileBrowser_cloudRead_lastScope")).toBe("cloud:s3:prod");
  });

  it("keeps read and write locations independent", () => {
    const store = useFileBrowserStore();
    store.setCurrentPath("cloudRead", "s3://bucket/in", "cloud:s3:prod");
    store.setCurrentPath("cloudWrite", "s3://bucket/out", "cloud:s3:prod");

    expect(store.getCurrentPath("cloudRead")).toBe("s3://bucket/in");
    expect(store.getCurrentPath("cloudWrite")).toBe("s3://bucket/out");
  });

  it("persists a cloud uri rather than discarding it as an internal path", () => {
    const store = useFileBrowserStore();
    store.setCurrentPath("cloudRead", "s3://bucket/.flowfile/data", "cloud:s3:prod");
    expect(localStorage.getItem("fileBrowser_cloudRead_lastPath")).toBe(
      "s3://bucket/.flowfile/data",
    );
  });

  it("surfaces a scope mismatch so a caller can discard the remembered path", () => {
    const store = useFileBrowserStore();
    store.setCurrentPath("cloudRead", "s3://prod-bucket/data", "cloud:s3:prod");
    expect(store.getScope("cloudRead")).not.toBe("cloud:s3:staging");
  });

  it("switching connections replaces the scope rather than accumulating keys", () => {
    const store = useFileBrowserStore();
    store.setCurrentPath("cloudRead", "s3://prod/data", "cloud:s3:prod");
    store.setCurrentPath("cloudRead", "s3://staging/data", "cloud:s3:staging");

    expect(store.getScope("cloudRead")).toBe("cloud:s3:staging");
    const cloudKeys = Object.keys(localStorage as unknown as Record<string, string>).filter((key) =>
      key.startsWith("fileBrowser_cloudRead"),
    );
    expect(cloudKeys.length).toBeLessThanOrEqual(2);
  });

  it("clears both the path and the scope on reset", () => {
    const store = useFileBrowserStore();
    store.setCurrentPath("cloudRead", "s3://bucket/data", "cloud:s3:prod");
    store.resetContext("cloudRead");

    expect(store.getCurrentPath("cloudRead")).toBe("");
    expect(store.getScope("cloudRead")).toBeNull();
    expect(localStorage.getItem("fileBrowser_cloudRead_lastPath")).toBeNull();
    expect(localStorage.getItem("fileBrowser_cloudRead_lastScope")).toBeNull();
  });
});

describe("sort preferences", () => {
  it("persists the sort key and direction", () => {
    const store = useFileBrowserStore();
    store.setSortBy("size");
    store.toggleSortDirection();

    expect(localStorage.getItem("fileBrowser_sortBy")).toBe("size");
    expect(localStorage.getItem("fileBrowser_sortDirection")).toBe("desc");
  });
});
