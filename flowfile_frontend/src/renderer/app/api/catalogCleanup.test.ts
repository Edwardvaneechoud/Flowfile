import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("../services/axios.config", () => ({
  default: { post: vi.fn() },
}));

const loadModule = async () => {
  vi.resetModules();
  const axios = (await import("../services/axios.config")).default;
  const { CatalogApi } = await import("./catalog.api");
  return { axios: vi.mocked(axios), CatalogApi };
};

describe("CatalogApi.cleanupNamespaceFlows", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("posts to the namespace cleanup route without a trailing slash", async () => {
    const { axios, CatalogApi } = await loadModule();
    axios.post.mockResolvedValueOnce({ data: { deleted: 3, kept: 1 } });

    const result = await CatalogApi.cleanupNamespaceFlows(42);

    // A trailing slash here would make FastAPI answer 307 and axios re-POST to
    // an absolute URL — the redirect trap this codebase keeps tripping over.
    expect(axios.post).toHaveBeenCalledWith("/catalog/namespaces/42/cleanup_flows");
    expect(result).toEqual({ deleted: 3, kept: 1 });
  });

  it("propagates a rejection so the caller can surface the server detail", async () => {
    const { axios, CatalogApi } = await loadModule();
    axios.post.mockRejectedValueOnce({ response: { data: { detail: "Not authorized" } } });

    await expect(CatalogApi.cleanupNamespaceFlows(7)).rejects.toMatchObject({
      response: { data: { detail: "Not authorized" } },
    });
  });
});
