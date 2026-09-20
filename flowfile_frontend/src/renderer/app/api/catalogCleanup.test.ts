import { beforeEach, describe, expect, it, vi, type Mock } from "vitest";

vi.mock("../services/axios.config", () => ({
  default: { post: vi.fn(), get: vi.fn() },
}));

const loadModule = async () => {
  vi.resetModules();
  const axios = (await import("../services/axios.config")).default;
  const { CatalogApi } = await import("./catalog.api");
  // The instance's `post` is overloaded, so vi.mocked() (shallow) keeps the
  // original signature and the mock helpers don't typecheck — cast instead.
  return { post: axios.post as unknown as Mock, get: axios.get as unknown as Mock, CatalogApi };
};

describe("CatalogApi.cleanupNamespaceFlows", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("posts to the namespace cleanup route without a trailing slash", async () => {
    const { post, CatalogApi } = await loadModule();
    post.mockResolvedValueOnce({ data: { deleted: 3, kept: 1 } });

    const result = await CatalogApi.cleanupNamespaceFlows(42);

    // A trailing slash here would make FastAPI answer 307 and axios re-POST to
    // an absolute URL — the redirect trap this codebase keeps tripping over.
    expect(post).toHaveBeenCalledWith("/catalog/namespaces/42/cleanup_flows");
    expect(result).toEqual({ deleted: 3, kept: 1 });
  });

  it("propagates a rejection so the caller can surface the server detail", async () => {
    const { post, CatalogApi } = await loadModule();
    post.mockRejectedValueOnce({ response: { data: { detail: "Not authorized" } } });

    await expect(CatalogApi.cleanupNamespaceFlows(7)).rejects.toMatchObject({
      response: { data: { detail: "Not authorized" } },
    });
  });
});

describe("CatalogApi.resolveTableStrict", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("asks for a strict resolve and returns the table", async () => {
    const { get, CatalogApi } = await loadModule();
    get.mockResolvedValueOnce({ data: { table: { id: 3, name: "orders" } } });

    const table = await CatalogApi.resolveTableStrict("orders", 9);

    expect(get).toHaveBeenCalledWith("/catalog/tables/resolve", {
      params: { q: "orders", strict: true, namespace_id: 9 },
    });
    expect(table).toMatchObject({ id: 3, name: "orders" });
  });

  it("returns null on an ambiguous name (409) instead of throwing", async () => {
    const { get, CatalogApi } = await loadModule();
    get.mockRejectedValueOnce({ response: { status: 409 } });

    await expect(CatalogApi.resolveTableStrict("orders")).resolves.toBeNull();
  });
});
