import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({ get: vi.fn(), post: vi.fn() }));

vi.mock("../services/axios.config", () => ({
  default: { get: mocks.get, post: mocks.post },
}));

import {
  listHubFlowVersions,
  listHubFlows,
  openFromHub,
  publishCheck,
  publishToHub,
  refusalOf,
} from "./hubFlows.api";

beforeEach(() => {
  mocks.get.mockReset().mockResolvedValue({ data: [] });
  mocks.post.mockReset().mockResolvedValue({ data: {} });
});

describe("the routes", () => {
  it("narrows the flows to a namespace through the query and sends none without one", async () => {
    await listHubFlows(2);
    await listHubFlows();

    expect(mocks.get).toHaveBeenNthCalledWith(1, "/hub_client/flows", {
      params: { namespace_id: 2 },
    });
    expect(mocks.get).toHaveBeenNthCalledWith(2, "/hub_client/flows", { params: undefined });
  });

  it("lists a flow's versions under its uuid", async () => {
    await listHubFlowVersions("u");

    expect(mocks.get).toHaveBeenCalledWith("/hub_client/flows/u/versions");
  });

  it("asks for the flow's local paths by its id", async () => {
    await publishCheck(42);

    expect(mocks.get).toHaveBeenCalledWith("/hub_client/publish_check", {
      params: { flow_id: 42 },
    });
  });

  it("posts a publish to /hub_client/publish", async () => {
    const input = { flow_id: 42, namespace_id: 2, message: "again", overwrite_version: 2 };

    await publishToHub(input);

    expect(mocks.post).toHaveBeenCalledWith("/hub_client/publish", input);
  });

  it("posts an open to /hub_client/open", async () => {
    const input = { flow_uuid: "u", version: 2, replace: true };

    await openFromHub(input);

    expect(mocks.post).toHaveBeenCalledWith("/hub_client/open", input);
  });
});

describe("refusalOf", () => {
  const failure = (detail: unknown) => ({ response: { status: 409, data: { detail } } });

  it.each([
    { kind: "stale_base", flow_uuid: "u", base: null, current: { version: 2 } },
    { kind: "refused", status: 403, detail: "no write access" },
    { kind: "other_hub", hub_url: "https://other" },
    { kind: "local_copy_exists", flow_path: "/f.yaml" },
  ])("reads $kind", (detail) => {
    expect(refusalOf(failure(detail))).toEqual(detail);
  });

  it("is null for a plain string detail, an unknown kind or no response", () => {
    expect(refusalOf(failure("save the flow first"))).toBeNull();
    expect(refusalOf(failure({ kind: "something_else" }))).toBeNull();
    expect(refusalOf(new Error("network"))).toBeNull();
  });
});
