import { afterEach, describe, expect, it, vi } from "vitest";

import { fetchLatestVersion, isNewerVersion, LATEST_RELEASE_URL } from "./latestVersion";

describe("isNewerVersion", () => {
  it.each([
    ["0.18.0", "0.17.3", true],
    ["0.17.10", "0.17.9", true],
    ["0.17.3", "0.17.3", false],
    ["0.17.2", "0.17.3", false],
    ["1.0", "0.99.99", true],
    ["0.17.3", "0.17.3.1", false],
    ["weird", "0.1", false],
  ])("%s newer than %s → %s", (candidate, current, expected) => {
    expect(isNewerVersion(candidate, current)).toBe(expected);
  });
});

describe("fetchLatestVersion", () => {
  afterEach(() => vi.unstubAllGlobals());

  it("reads the tag from the latest GitHub release, minus the v prefix", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ tag_name: "v0.18.0" }),
    });
    vi.stubGlobal("fetch", fetchMock);

    expect(await fetchLatestVersion()).toBe("0.18.0");
    expect(fetchMock).toHaveBeenCalledWith(LATEST_RELEASE_URL, expect.anything());
  });

  it("rejects a non-2xx or malformed answer", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: false, status: 403 }));
    await expect(fetchLatestVersion()).rejects.toThrow("403");

    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: true, json: async () => ({}) }));
    await expect(fetchLatestVersion()).rejects.toThrow("tag_name");
  });
});
