import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({ get: vi.fn(), post: vi.fn() }));

vi.mock("../services/axios.config", () => ({
  default: { get: mocks.get, post: mocks.post },
}));

import {
  listHubFlowVersions,
  listHubFlows,
  listHubNamespaces,
  openFromHub,
  publishCheck,
  publishToHub,
  refusalOf,
} from "./hubFlows.api";

const pyVersion = {
  flow_uuid: "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
  version: 2,
  author_id: 7,
  published_at: "2026-11-01T12:01:00Z",
  message: "theirs",
};
const version = {
  flowUuid: "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
  version: 2,
  authorId: 7,
  publishedAt: "2026-11-01T12:01:00Z",
  message: "theirs",
};

beforeEach(() => {
  mocks.get.mockReset();
  mocks.post.mockReset();
});

describe("listing", () => {
  it("gets the hub's namespaces without a trailing slash and maps snake_case to camelCase", async () => {
    mocks.get.mockResolvedValue({
      data: [{ id: 2, name: "sales", parent_id: 1, level: 1, description: null, owner_id: 1 }],
    });

    const result = await listHubNamespaces();

    expect(mocks.get).toHaveBeenCalledWith("/hub_client/namespaces");
    expect(result).toEqual([{ id: 2, name: "sales", parentId: 1, level: 1, description: null }]);
  });

  it("asks for a namespace's children through parent_id", async () => {
    mocks.get.mockResolvedValue({
      data: [{ id: 2, name: "sales", parent_id: 1, level: 1, description: null, owner_id: 1 }],
    });

    const result = await listHubNamespaces(1);

    expect(mocks.get).toHaveBeenCalledWith("/hub_client/namespaces", { params: { parent_id: 1 } });
    expect(result).toEqual([{ id: 2, name: "sales", parentId: 1, level: 1, description: null }]);
  });

  it("narrows the flows to a namespace through the query and sends none without one", async () => {
    mocks.get.mockResolvedValue({
      data: [
        {
          registration_id: 3,
          flow_uuid: "u",
          name: "orders",
          namespace_id: 2,
          owner_id: 1,
          latest_version: null,
        },
      ],
    });

    const result = await listHubFlows(2);
    await listHubFlows();

    expect(mocks.get).toHaveBeenNthCalledWith(1, "/hub_client/flows", {
      params: { namespace_id: 2 },
    });
    expect(mocks.get).toHaveBeenNthCalledWith(2, "/hub_client/flows", { params: undefined });
    expect(result).toEqual([
      {
        registrationId: 3,
        flowUuid: "u",
        name: "orders",
        namespaceId: 2,
        ownerId: 1,
        latestVersion: null,
      },
    ]);
  });

  it("lists a flow's versions under its uuid", async () => {
    mocks.get.mockResolvedValue({ data: [pyVersion] });

    const result = await listHubFlowVersions(pyVersion.flow_uuid);

    expect(mocks.get).toHaveBeenCalledWith(`/hub_client/flows/${pyVersion.flow_uuid}/versions`);
    expect(result).toEqual([version]);
  });
});

describe("publishCheck", () => {
  it("asks for the flow's local paths and maps the nodes", async () => {
    mocks.get.mockResolvedValue({
      data: [{ node_id: 2, node_type: "read", label: "sales.csv (csv)", path: "/data/sales.csv" }],
    });

    const result = await publishCheck(42);

    expect(mocks.get).toHaveBeenCalledWith("/hub_client/publish_check", {
      params: { flow_id: 42 },
    });
    expect(result).toEqual([
      { nodeId: 2, nodeType: "read", label: "sales.csv (csv)", path: "/data/sales.csv" },
    ]);
  });
});

describe("publishToHub", () => {
  it("posts the flow, namespace, message and overwrite version in the route's field names", async () => {
    mocks.post.mockResolvedValue({
      data: { flow_uuid: "u", version: 3, name: "Sales report", namespace_id: 2 },
    });

    const result = await publishToHub({
      flowId: 42,
      namespaceId: 2,
      message: "again",
      overwriteVersion: 2,
    });

    expect(mocks.post).toHaveBeenCalledWith("/hub_client/publish", {
      flow_id: 42,
      namespace_id: 2,
      message: "again",
      overwrite_version: 2,
    });
    expect(result).toEqual({ flowUuid: "u", version: 3, name: "Sales report", namespaceId: 2 });
  });

  it("sends nulls for what was not given, so a first publish has no base", async () => {
    mocks.post.mockResolvedValue({
      data: { flow_uuid: "u", version: 1, name: "n", namespace_id: null },
    });

    await publishToHub({ flowId: 42 });

    expect(mocks.post.mock.calls[0][1]).toEqual({
      flow_id: 42,
      namespace_id: null,
      message: null,
      overwrite_version: null,
    });
  });
});

describe("openFromHub", () => {
  it("posts the uuid, version and replace flag and maps the answer", async () => {
    mocks.post.mockResolvedValue({
      data: { flow_path: "/flows/hub/orders_aaaaaaaa.yaml", flow_uuid: "u", version: 2 },
    });

    const result = await openFromHub({ flowUuid: "u", version: 2, replace: true });

    expect(mocks.post).toHaveBeenCalledWith("/hub_client/open", {
      flow_uuid: "u",
      version: 2,
      replace: true,
    });
    expect(result).toEqual({
      flowPath: "/flows/hub/orders_aaaaaaaa.yaml",
      flowUuid: "u",
      version: 2,
    });
  });
});

describe("refusalOf", () => {
  const failure = (detail: unknown) => ({ response: { status: 409, data: { detail } } });

  it("reads a stale base with both versions, the base possibly missing", () => {
    expect(
      refusalOf(failure({ kind: "stale_base", flow_uuid: "u", base: null, current: pyVersion })),
    ).toEqual({ kind: "stale_base", flowUuid: "u", base: null, current: version });
  });

  it("reads the other kinds", () => {
    expect(refusalOf(failure({ kind: "refused", status: 403, detail: "no write access" }))).toEqual(
      {
        kind: "refused",
        status: 403,
        detail: "no write access",
      },
    );
    expect(refusalOf(failure({ kind: "other_hub", hub_url: "https://other" }))).toEqual({
      kind: "other_hub",
      hubUrl: "https://other",
    });
    expect(refusalOf(failure({ kind: "local_copy_exists", flow_path: "/f.yaml" }))).toEqual({
      kind: "local_copy_exists",
      flowPath: "/f.yaml",
    });
  });

  it("is null for a plain string detail, an unknown kind or no response", () => {
    expect(refusalOf(failure("save the flow first"))).toBeNull();
    expect(refusalOf(failure({ kind: "something_else" }))).toBeNull();
    expect(refusalOf(new Error("network"))).toBeNull();
  });
});
