import { beforeEach, describe, expect, it, vi, type Mock } from "vitest";

vi.mock("../services/axios.config", () => ({
  default: { get: vi.fn(), post: vi.fn(), put: vi.fn(), delete: vi.fn() },
}));

const loadModule = async () => {
  vi.resetModules();
  const axios = (await import("../services/axios.config")).default;
  const api = await import("./secrets.api");
  return { put: axios.put as unknown as Mock, api };
};

describe("secrets.api", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("changes a secret's value with a PUT of the value alone, its name escaped", async () => {
    const { put, api } = await loadModule();
    put.mockResolvedValue({ data: {} });

    await api.SecretsApi.change("db/main", "n3w");

    expect(put).toHaveBeenCalledWith("/secrets/secrets/db%2Fmain", { value: "n3w" });
  });

  it("says the server's sentence when a change is refused", async () => {
    const { put, api } = await loadModule();
    put.mockRejectedValue({ response: { data: { detail: "Secret not found" } } });

    await expect(api.SecretsApi.change("gone", "v")).rejects.toThrow("Secret not found");
  });
});
