// Unit tests for useMultiUser: it derives the docker/multi-user flag and the project
// capability flags (enabled/confined) from a single GET /health/status fetch.
//
// The setup service is mocked, and vi.resetModules() gives each test a fresh module
// instance (the composable keeps module-level singleton refs + a resolved latch).

import { describe, it, expect, vi, beforeEach } from "vitest";

const mocks = vi.hoisted(() => ({ getSetupStatus: vi.fn() }));

vi.mock("../services/setup.service", () => ({
  default: { getSetupStatus: mocks.getSetupStatus },
}));

beforeEach(() => {
  vi.resetModules();
  vi.clearAllMocks();
});

describe("useMultiUser", () => {
  it("maps docker + project flags from /health/status", async () => {
    mocks.getSetupStatus.mockResolvedValue({
      mode: "docker",
      projects_enabled: true,
      projects_confined: true,
    });
    const { useMultiUser } = await import("./useMultiUser");
    const { isMultiUser, projectsEnabled, projectsConfined, refresh } = useMultiUser();
    await refresh();
    expect(isMultiUser.value).toBe(true);
    expect(projectsEnabled.value).toBe(true);
    expect(projectsConfined.value).toBe(true);
  });

  it("treats desktop mode as enabled but not confined", async () => {
    mocks.getSetupStatus.mockResolvedValue({
      mode: "tauri",
      projects_enabled: true,
      projects_confined: false,
    });
    const { useMultiUser } = await import("./useMultiUser");
    const { isMultiUser, projectsEnabled, projectsConfined, refresh } = useMultiUser();
    await refresh();
    expect(isMultiUser.value).toBe(false);
    expect(projectsEnabled.value).toBe(true);
    expect(projectsConfined.value).toBe(false);
  });

  it("defaults every flag to false when the status fetch fails", async () => {
    mocks.getSetupStatus.mockRejectedValue(new Error("down"));
    const { useMultiUser } = await import("./useMultiUser");
    const { isMultiUser, projectsEnabled, projectsConfined, refresh } = useMultiUser();
    await refresh();
    expect(isMultiUser.value).toBe(false);
    expect(projectsEnabled.value).toBe(false);
    expect(projectsConfined.value).toBe(false);
  });
});
