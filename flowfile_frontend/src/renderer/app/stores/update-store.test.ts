// Unit tests for the update store: the install ladder (download → backup → install)
// and the visibility gates. The Tauri bridge and the backup API are mocked; the
// pure prompt logic is covered by components/settings/updatePrompt.test.ts.

import { setActivePinia, createPinia } from "pinia";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const { calls, checkForUpdateMock, downloadUpdateMock, installUpdateMock, restartAppMock } =
  vi.hoisted(() => ({
    calls: [] as string[],
    checkForUpdateMock: vi.fn(),
    downloadUpdateMock: vi.fn(),
    installUpdateMock: vi.fn(),
    restartAppMock: vi.fn(),
  }));

const { createDbBackupMock } = vi.hoisted(() => ({ createDbBackupMock: vi.fn() }));

vi.mock("../../lib/desktop", () => ({
  isDesktop: false,
  desktop: {
    checkForUpdate: checkForUpdateMock,
    downloadUpdate: downloadUpdateMock,
    installUpdate: installUpdateMock,
    restartApp: restartAppMock,
  },
}));

vi.mock("../api/system.api", () => ({
  createDbBackup: createDbBackupMock,
}));

import { useUpdateStore } from "./update-store";

function fakeLocalStorage() {
  const map = new Map<string, string>();
  return {
    getItem: (k: string) => map.get(k) ?? null,
    setItem: (k: string, v: string) => void map.set(k, v),
    removeItem: (k: string) => void map.delete(k),
    clear: () => map.clear(),
  } as unknown as Storage;
}

const backup = {
  path: "/home/u/.flowfile/db_backups/flowfile_catalog.pre-update.20260903T101500Z.db",
};

function offeredStore() {
  const store = useUpdateStore();
  store.info = { version: "0.18.0", currentVersion: "0.17.0" };
  return store;
}

beforeEach(() => {
  vi.stubGlobal("localStorage", fakeLocalStorage());
  setActivePinia(createPinia());
  calls.length = 0;
  checkForUpdateMock.mockReset();
  downloadUpdateMock.mockReset().mockImplementation(async () => void calls.push("download"));
  installUpdateMock.mockReset().mockImplementation(async () => void calls.push("install"));
  restartAppMock.mockReset();
  createDbBackupMock.mockReset().mockImplementation(async () => {
    calls.push("backup");
    return backup;
  });
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("install ladder", () => {
  it("downloads, snapshots the database, then installs", async () => {
    const store = offeredStore();

    await store.install();

    expect(calls).toEqual(["download", "backup", "install"]);
    expect(createDbBackupMock).toHaveBeenCalledWith("pre_update");
    expect(store.backupPath).toBe(backup.path);
    expect(store.phase).toBe("installing");
  });

  it("reports download progress", async () => {
    const store = offeredStore();
    downloadUpdateMock.mockImplementation(
      async (onProgress: (d: number, t: number | null) => void) => {
        onProgress(512, 2048);
      },
    );

    await store.install();

    expect(store.progress).toEqual({ downloaded: 512, total: 2048 });
  });

  it("a failed download stops before the backup and the install", async () => {
    const store = offeredStore();
    downloadUpdateMock.mockRejectedValue(new Error("connection reset"));

    await store.install();

    expect(store.phase).toBe("download-failed");
    expect(store.error).toBe("connection reset");
    expect(createDbBackupMock).not.toHaveBeenCalled();
    expect(installUpdateMock).not.toHaveBeenCalled();
  });

  it("a failed backup halts before the install, and the user can continue anyway", async () => {
    const store = offeredStore();
    createDbBackupMock.mockRejectedValue(new Error("Request failed with status code 503"));

    await store.install();

    expect(store.phase).toBe("backup-failed");
    expect(installUpdateMock).not.toHaveBeenCalled();

    await store.continueWithoutBackup();

    expect(store.phase).toBe("installing");
    expect(installUpdateMock).toHaveBeenCalledTimes(1);
    expect(store.error).toBeNull();
  });

  it("a failed install leaves the recoverable phase", async () => {
    const store = offeredStore();
    installUpdateMock.mockRejectedValue(new Error("bundle replace failed"));

    await store.install();

    expect(store.phase).toBe("install-failed");
    expect(store.error).toBe("bundle replace failed");

    await store.restart();
    expect(restartAppMock).toHaveBeenCalledTimes(1);
  });
});

describe("visibility", () => {
  it("offers an update until it is dismissed", () => {
    const store = offeredStore();
    expect(store.promptVisible).toBe(true);

    store.dismiss();
    expect(store.promptVisible).toBe(false);
  });

  it("skipVersion persists the version and hides the prompt", () => {
    const store = offeredStore();

    store.skipVersion();

    expect(store.promptVisible).toBe(false);
    expect(localStorage.getItem("flowfile.update.ignoredVersion.v1")).toBe("0.18.0");
    // A fresh store (next launch) reads the tombstone and stays silent.
    setActivePinia(createPinia());
    const next = useUpdateStore();
    next.info = { version: "0.18.0", currentVersion: "0.17.0" };
    expect(next.promptVisible).toBe(false);
  });

  it("resetPhase returns to the offer screen", async () => {
    const store = offeredStore();
    createDbBackupMock.mockRejectedValue(new Error("503"));
    await store.install();

    store.resetPhase();

    expect(store.phase).toBe("idle");
    expect(store.error).toBeNull();
    expect(store.backupPath).toBeNull();
    expect(store.progress).toEqual({ downloaded: 0, total: null });
  });
});

describe("checking", () => {
  it("never checks in web mode", async () => {
    const store = useUpdateStore();

    await store.checkOnStartup();

    expect(checkForUpdateMock).not.toHaveBeenCalled();
    expect(store.checked).toBe(false);
  });

  it("a manual check re-offers a skipped version", async () => {
    const store = useUpdateStore();
    checkForUpdateMock.mockResolvedValue({ version: "0.18.0", currentVersion: "0.17.0" });
    store.ignoredVersion = "0.18.0";
    store.dismissed = true;

    const info = await store.checkNow();

    expect(info?.version).toBe("0.18.0");
    expect(store.promptVisible).toBe(true);
  });

  it("hides the previous offer while a manual check is in flight", async () => {
    const store = offeredStore();
    store.dismissed = true;
    let resolveCheck: (info: null) => void = () => undefined;
    checkForUpdateMock.mockImplementation(
      () => new Promise<null>((resolve) => (resolveCheck = resolve)),
    );

    const pending = store.checkNow();
    expect(store.promptVisible).toBe(false);

    resolveCheck(null);
    await pending;
    expect(store.promptVisible).toBe(false);
  });

  it("a failed manual check never throws and reports itself", async () => {
    const store = useUpdateStore();
    checkForUpdateMock.mockRejectedValue(new Error("404 feed"));

    await expect(store.checkNow()).resolves.toBeNull();

    expect(store.checkError).toBe(true);
    expect(store.checking).toBe(false);
    expect(store.promptVisible).toBe(false);
  });
});
