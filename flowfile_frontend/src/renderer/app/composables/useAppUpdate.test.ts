// The shared version/update-check glue behind About, the home footer and the
// sidebar help menu, exercised in browser mode (no Tauri: the check reads the
// latest GitHub release). The desktop branch is a thin call into
// stores/update-store, covered there.

import { setActivePinia, createPinia } from "pinia";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const { getAppVersionMock, openExternalMock } = vi.hoisted(() => ({
  getAppVersionMock: vi.fn(),
  openExternalMock: vi.fn(),
}));

vi.mock("../../lib/desktop", () => ({
  isDesktop: false,
  desktop: {
    checkForUpdate: vi.fn(),
    getAppVersion: getAppVersionMock,
    openExternal: openExternalMock,
  },
}));

vi.mock("../api/system.api", () => ({ createDbBackup: vi.fn() }));

import { useAppUpdate } from "./useAppUpdate";

function fakeLocalStorage() {
  const map = new Map<string, string>();
  return {
    getItem: (k: string) => map.get(k) ?? null,
    setItem: (k: string, v: string) => void map.set(k, v),
    removeItem: (k: string) => void map.delete(k),
    clear: () => map.clear(),
  } as unknown as Storage;
}

function stubLatestRelease(tag: string | null) {
  vi.stubGlobal(
    "fetch",
    tag === null
      ? vi.fn().mockRejectedValue(new TypeError("Failed to fetch"))
      : vi.fn().mockResolvedValue({ ok: true, json: async () => ({ tag_name: tag }) }),
  );
}

beforeEach(() => {
  vi.stubGlobal("localStorage", fakeLocalStorage());
  vi.stubGlobal("__APP_VERSION__", "0.17.3");
  vi.spyOn(console, "warn").mockImplementation(() => undefined);
  setActivePinia(createPinia());
  getAppVersionMock.mockReset().mockResolvedValue("");
  openExternalMock.mockReset();
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("useAppUpdate (browser mode)", () => {
  it("resolves the running version", async () => {
    const { version } = useAppUpdate();
    await vi.waitFor(() => expect(version.value).toBe("0.17.3"));
  });

  it("reports an up-to-date check and clears it again", async () => {
    stubLatestRelease("v0.17.3");
    const { statusText, checkForUpdates, resetStatus } = useAppUpdate();

    expect(statusText.value).toBe("");
    expect(await checkForUpdates()).toBe(false);
    expect(statusText.value).toBe("You're up to date");

    resetStatus();
    expect(statusText.value).toBe("");
  });

  it("names a newer release and points Release notes at it", async () => {
    stubLatestRelease("v0.18.0");
    const { statusText, checkForUpdates, openReleaseNotes } = useAppUpdate();

    expect(await checkForUpdates()).toBe(false);
    expect(statusText.value).toBe("Flowfile 0.18.0 is available");

    openReleaseNotes();
    expect(openExternalMock).toHaveBeenCalledWith(
      "https://github.com/edwardvaneechoud/Flowfile/releases/tag/v0.18.0",
    );
  });

  it("reports a failed check", async () => {
    stubLatestRelease(null);
    const { statusText, checkForUpdates } = useAppUpdate();

    expect(await checkForUpdates()).toBe(false);
    expect(statusText.value).toBe("Couldn't check for updates");
  });

  it("opens the running version's release page before any check", async () => {
    const { version, openReleaseNotes } = useAppUpdate();
    await vi.waitFor(() => expect(version.value).toBe("0.17.3"));

    openReleaseNotes();
    expect(openExternalMock).toHaveBeenCalledWith(
      "https://github.com/edwardvaneechoud/Flowfile/releases/tag/v0.17.3",
    );
  });
});
