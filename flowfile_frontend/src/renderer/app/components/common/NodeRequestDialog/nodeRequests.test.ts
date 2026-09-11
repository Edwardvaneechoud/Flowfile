import { describe, it, expect } from "vitest";

import { filterRequests, newRequestUrl } from "./nodeRequests";
import { NODE_REQUEST_ISSUE_URL } from "../../../lib/docsLinks";
import type { NodeRequest } from "../../../api/nodeRequests.api";

function request(
  number: number,
  title: string,
  upvotes = 0,
  kind: NodeRequest["kind"] = "general",
) {
  return { number, title, url: `https://x/${number}`, kind, tool_key: null, upvotes };
}

const REQUESTS = [
  request(3, "[Alteryx node] DateTime", 1, "alteryx"),
  request(5, "A pivot-longer node with several value columns", 4),
  request(8, "Date parsing with multiple formats", 4),
];

describe("filterRequests", () => {
  it("matches every word of the query against the title, ignoring case", () => {
    expect(filterRequests(REQUESTS, "date").map((r) => r.number)).toEqual([8, 3]);
    expect(filterRequests(REQUESTS, "DATE parsing").map((r) => r.number)).toEqual([8]);
    expect(filterRequests(REQUESTS, "pivot tile")).toEqual([]);
  });

  it("orders by upvotes, then by age, and an empty query lists everything", () => {
    expect(filterRequests(REQUESTS, "  ").map((r) => r.number)).toEqual([5, 8, 3]);
  });
});

describe("newRequestUrl", () => {
  it("opens the form as-is without a query", () => {
    expect(newRequestUrl("   ")).toBe(NODE_REQUEST_ISSUE_URL);
  });

  it("prefills the title from the search text", () => {
    const url = new URL(newRequestUrl(" pivot longer "));
    expect(url.searchParams.get("template")).toBe("node_request.yml");
    expect(url.searchParams.get("title")).toBe("pivot longer");
  });
});
