import { beforeEach, describe, expect, it, vi } from "vitest";
import { createPinia, setActivePinia } from "pinia";
import type { TelemetryStatus } from "../api/telemetry.api";

const { getStatusMock, setConsentMock } = vi.hoisted(() => ({
  getStatusMock: vi.fn(),
  setConsentMock: vi.fn(),
}));

vi.mock("../api/telemetry.api", () => ({
  getTelemetryStatus: getStatusMock,
  setTelemetryConsent: setConsentMock,
}));

// Mocked because the real auth service reaches DOM globals through axios.config at import time.
vi.mock("../services/auth.service", () => ({
  default: { logout: vi.fn() },
}));

import { useAuthStore } from "./auth-store";
import { useTelemetryStore } from "./telemetry-store";

const status = (overrides: Partial<TelemetryStatus> = {}): TelemetryStatus => ({
  available: true,
  consent: null,
  envKillSwitch: false,
  endpointConfigured: true,
  canManage: true,
  ...overrides,
});

describe("telemetry-store", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
    getStatusMock.mockReset();
    setConsentMock.mockReset();
  });

  it("loads status once and caches it", async () => {
    const store = useTelemetryStore();
    getStatusMock.mockResolvedValue(status());

    await store.loadStatus();
    await store.loadStatus();

    expect(getStatusMock).toHaveBeenCalledTimes(1);
    expect(store.loaded).toBe(true);
    expect(store.status?.consent).toBeNull();
  });

  it("refetches when forced", async () => {
    const store = useTelemetryStore();
    getStatusMock.mockResolvedValue(status());

    await store.loadStatus();
    await store.loadStatus(true);

    expect(getStatusMock).toHaveBeenCalledTimes(2);
  });

  it("swallows a failed status load and stays not-loaded", async () => {
    const store = useTelemetryStore();
    getStatusMock.mockRejectedValue(new Error("network"));

    await expect(store.loadStatus()).resolves.toBeNull();

    expect(store.loaded).toBe(false);
    expect(store.status).toBeNull();
    // The card must be able to say "unknown" instead of rendering a plain Off.
    expect(store.loadFailed).toBe(true);
  });

  it("flags a failed refresh as unknown while keeping the stale status", async () => {
    const store = useTelemetryStore();
    getStatusMock.mockResolvedValueOnce(status({ consent: true }));
    await store.loadStatus();
    expect(store.loadFailed).toBe(false);

    getStatusMock.mockRejectedValueOnce(new Error("network"));
    await store.loadStatus(true);

    expect(store.loadFailed).toBe(true);
    expect(store.status?.consent).toBe(true);
  });

  it("clears the unknown flag once a read or a write succeeds", async () => {
    const store = useTelemetryStore();
    getStatusMock.mockRejectedValueOnce(new Error("network"));
    await store.loadStatus();
    expect(store.loadFailed).toBe(true);

    setConsentMock.mockResolvedValue(status({ consent: true }));
    await store.setConsent(true);

    expect(store.loadFailed).toBe(false);
  });

  it("never POSTs spontaneously while consent is undecided", async () => {
    const store = useTelemetryStore();
    getStatusMock.mockResolvedValue(status({ consent: null }));

    await store.loadStatus();
    await store.loadStatus(true);

    expect(store.status?.consent).toBeNull();
    expect(setConsentMock).not.toHaveBeenCalled();
  });

  it("setConsent adopts the server response, never the optimistic value", async () => {
    const store = useTelemetryStore();
    getStatusMock.mockResolvedValue(status());
    await store.loadStatus();
    // Server declines to persist (e.g. kill switch flipped) — its word wins.
    setConsentMock.mockResolvedValue(status({ consent: false, envKillSwitch: true }));

    await store.setConsent(true);

    expect(setConsentMock).toHaveBeenCalledWith(true);
    expect(store.status?.consent).toBe(false);
    expect(store.status?.envKillSwitch).toBe(true);
  });

  it("a rejected setConsent leaves state unchanged and rethrows", async () => {
    const store = useTelemetryStore();
    getStatusMock.mockResolvedValue(status({ consent: null }));
    await store.loadStatus();
    const failure = Object.assign(new Error("403"), { response: { status: 403 } });
    setConsentMock.mockRejectedValue(failure);

    await expect(store.setConsent(true)).rejects.toBe(failure);

    expect(store.status?.consent).toBeNull();
    expect(store.loaded).toBe(true);
  });

  it("is dropped on logout so the next user is not judged by this one's authority", async () => {
    const store = useTelemetryStore();
    getStatusMock.mockResolvedValue(status({ canManage: true }));
    await store.loadStatus();

    useAuthStore().logout();

    expect(store.status).toBeNull();
    expect(store.loaded).toBe(false);
    expect(store.loadFailed).toBe(false);

    getStatusMock.mockResolvedValue(status({ canManage: false }));
    await store.loadStatus();
    expect(getStatusMock).toHaveBeenCalledTimes(2);
    expect(store.status?.canManage).toBe(false);
  });
});
