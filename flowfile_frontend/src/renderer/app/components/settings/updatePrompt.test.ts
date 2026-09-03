import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  IGNORED_VERSION_KEY,
  loadIgnoredVersion,
  platformNote,
  releasePageUrl,
  saveIgnoredVersion,
  shouldPromptForUpdate,
} from "./updatePrompt";

function fakeLocalStorage() {
  const map = new Map<string, string>();
  return {
    getItem: (k: string) => map.get(k) ?? null,
    setItem: (k: string, v: string) => void map.set(k, v),
    removeItem: (k: string) => void map.delete(k),
    clear: () => map.clear(),
  } as unknown as Storage;
}

describe("shouldPromptForUpdate", () => {
  it("stays silent when the feed offers nothing", () => {
    expect(
      shouldPromptForUpdate({ availableVersion: null, ignoredVersion: null, force: false }),
    ).toBe(false);
  });

  it("stays silent for a version the user skipped", () => {
    expect(
      shouldPromptForUpdate({ availableVersion: "0.17.0", ignoredVersion: "0.17.0", force: false }),
    ).toBe(false);
  });

  it("prompts again once something newer than the skipped version ships", () => {
    expect(
      shouldPromptForUpdate({ availableVersion: "0.18.0", ignoredVersion: "0.17.0", force: false }),
    ).toBe(true);
  });

  it("a manual check re-offers even the skipped version", () => {
    expect(
      shouldPromptForUpdate({ availableVersion: "0.17.0", ignoredVersion: "0.17.0", force: true }),
    ).toBe(true);
    expect(
      shouldPromptForUpdate({ availableVersion: null, ignoredVersion: null, force: true }),
    ).toBe(false);
  });
});

describe("skipped-version storage", () => {
  beforeEach(() => {
    vi.stubGlobal("localStorage", fakeLocalStorage());
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("round-trips the skipped version", () => {
    expect(loadIgnoredVersion()).toBeNull();
    saveIgnoredVersion("0.17.0");
    expect(loadIgnoredVersion()).toBe("0.17.0");
    expect(localStorage.getItem(IGNORED_VERSION_KEY)).toBe("0.17.0");
  });

  it("never throws when localStorage is unavailable", () => {
    vi.stubGlobal("localStorage", {
      getItem: () => {
        throw new Error("denied");
      },
      setItem: () => {
        throw new Error("denied");
      },
    } as unknown as Storage);

    expect(loadIgnoredVersion()).toBeNull();
    expect(() => saveIgnoredVersion("0.17.0")).not.toThrow();
  });
});

describe("update copy", () => {
  it("tells each platform what its installer does, and stays silent in web mode", () => {
    expect(platformNote("mac")).toMatch(/restarts itself/i);
    expect(platformNote("windows")).toMatch(/installer/i);
    expect(platformNote("linux")).toMatch(/password/i);
    expect(platformNote(null)).toBe("");
  });

  it("links the release page on the lowercase owner path", () => {
    expect(releasePageUrl("0.18.0")).toBe(
      "https://github.com/edwardvaneechoud/Flowfile/releases/tag/v0.18.0",
    );
  });
});
