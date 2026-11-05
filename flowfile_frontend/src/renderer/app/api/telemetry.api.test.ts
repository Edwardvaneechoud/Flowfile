import { beforeEach, describe, expect, it, vi, type Mock } from "vitest";

vi.mock("../services/axios.config", () => ({
  default: { get: vi.fn(), post: vi.fn() },
}));

const loadModule = async () => {
  vi.resetModules();
  const axios = (await import("../services/axios.config")).default;
  const api = await import("./telemetry.api");
  return {
    get: axios.get as unknown as Mock,
    post: axios.post as unknown as Mock,
    api,
  };
};

const pyStatus = {
  available: true,
  consent: null,
  env_kill_switch: false,
  endpoint_configured: true,
  can_manage: true,
};

describe("telemetry.api", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("gets /telemetry/status without a trailing slash and maps snake_case to camelCase", async () => {
    const { get, api } = await loadModule();
    get.mockResolvedValueOnce({ data: pyStatus });

    const result = await api.getTelemetryStatus();

    // A trailing slash would make FastAPI answer 307 — the redirect trap.
    expect(get).toHaveBeenCalledWith("/telemetry/status");
    expect(result).toEqual({
      available: true,
      consent: null,
      envKillSwitch: false,
      endpointConfigured: true,
      canManage: true,
    });
  });

  it("posts /telemetry/consent without a trailing slash and maps the response", async () => {
    const { post, api } = await loadModule();
    post.mockResolvedValueOnce({
      data: { ...pyStatus, consent: true, env_kill_switch: true, can_manage: false },
    });

    const result = await api.setTelemetryConsent(true);

    expect(post).toHaveBeenCalledWith("/telemetry/consent", { enabled: true });
    expect(result).toEqual({
      available: true,
      consent: true,
      envKillSwitch: true,
      endpointConfigured: true,
      canManage: false,
    });
  });

  it("propagates rejections so the settings card can toast", async () => {
    const { post, api } = await loadModule();
    post.mockRejectedValueOnce({ response: { status: 403, data: { detail: "Not a manager" } } });

    await expect(api.setTelemetryConsent(false)).rejects.toMatchObject({
      response: { status: 403 },
    });
  });
});
