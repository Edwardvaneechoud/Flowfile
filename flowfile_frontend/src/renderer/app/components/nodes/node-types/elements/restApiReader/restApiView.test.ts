import { describe, expect, it } from "vitest";
import {
  columnGroupPaths,
  columnRows,
  detectedCursorPath,
  guessCursorParam,
  limitsSummary,
  nextRequestExample,
  defaultOpenPaths,
  responseLine,
  showSecret,
  testErrorText,
  toCurl,
  treeRows,
  type ResponseNode,
  type TraceEntry,
} from "./restApiView";
import { createNodeRestApiReader } from "./utils";

const defaults = () => createNodeRestApiReader(1, 1).rest_api_settings;

const entry = (update: Partial<TraceEntry> = {}): TraceEntry => ({
  page: 1,
  method: "GET",
  url: "https://api.example.com/items?api_key=REDACTED",
  headers: { Authorization: "Bearer REDACTED", "X-Team": "data" },
  body: null,
  status: 200,
  elapsed_ms: 312,
  bytes: 4300,
  records: 50,
  next_cursor: null,
  stopped: "single_request",
  ...update,
});

const tree: ResponseNode = {
  key: "",
  path: "",
  kind: "object",
  children: [
    {
      key: "meta",
      path: "meta",
      kind: "object",
      children: [{ key: "next", path: "meta.next", kind: "value", preview: '"c2"' }],
    },
    {
      key: "data",
      path: "data",
      kind: "list",
      length: 50,
      records: true,
      children: [
        { key: "id", path: "data.0.id", kind: "value", preview: "1" },
        {
          key: "items",
          path: "data.0.items",
          kind: "list",
          length: 2,
          records: true,
          children: [{ key: "sku", path: "data.0.items.0.sku", kind: "value", preview: '"a"' }],
        },
      ],
    },
    { key: "tags", path: "tags", kind: "list", length: 3, records: false, preview: '["a"]' },
    { key: "none", path: "none", kind: "list", length: 0, records: false, children: [] },
  ],
};

const everyPath = (node: ResponseNode): string[] => [
  node.path,
  ...(node.children ?? []).flatMap(everyPath),
];

describe("treeRows", () => {
  const rows = treeRows(tree, new Set(everyPath(tree)));

  it("lists every node with its depth and what it holds", () => {
    expect(rows.map((r) => [r.key, r.depth, r.detail])).toEqual([
      ["Whole response", 0, ""],
      ["meta", 1, ""],
      ["next", 2, '"c2"'],
      ["data", 1, "50 records"],
      ["id", 2, "1"],
      ["items", 2, "2 records"],
      ["sku", 3, '"a"'],
      ["tags", 1, "list of 3"],
      ["none", 1, "empty list"],
    ]);
  });

  it("offers a list of objects as records and a value outside any record as a token", () => {
    expect(rows.filter((r) => r.pick === "records").map((r) => r.path)).toEqual([
      "data",
      "data.0.items",
    ]);
    expect(rows.filter((r) => r.pick === "value").map((r) => r.path)).toEqual(["meta.next"]);
  });

  it("offers a response that is itself the list, and stops at the row limit", () => {
    const list: ResponseNode = { key: "", path: "", kind: "list", length: 2, records: true };
    expect(treeRows(list, new Set())).toEqual([
      {
        key: "Whole response",
        path: "",
        depth: 0,
        kind: "list",
        detail: "2 records",
        pick: "records",
        expandable: false,
        open: false,
      },
    ]);
    expect(treeRows(tree, new Set(everyPath(tree)), 3)).toHaveLength(3);
    expect(treeRows(null, new Set())).toEqual([]);
  });

  it("lists the children of a node only while it is open", () => {
    const folded = treeRows(tree, new Set([""]));
    expect(folded.map((r) => [r.key, r.expandable, r.open])).toEqual([
      ["Whole response", true, true],
      ["meta", true, false],
      ["data", true, false],
      ["tags", false, false],
      ["none", false, false],
    ]);
  });
});

describe("defaultOpenPaths", () => {
  it("opens the response and its objects, and leaves record lists folded", () => {
    expect([...defaultOpenPaths(tree)]).toEqual(["", "meta"]);
    expect(treeRows(tree, defaultOpenPaths(tree)).map((r) => r.key)).toEqual([
      "Whole response",
      "meta",
      "next",
      "data",
      "tags",
      "none",
    ]);
  });

  it("opens the way to the chosen token, but not into a record", () => {
    expect([...defaultOpenPaths(tree, "paging.cursors.after")]).toEqual([
      "",
      "meta",
      "paging",
      "paging.cursors",
    ]);
    expect([...defaultOpenPaths(tree, "data.-1.id")]).toEqual(["", "meta"]);
  });

  it("leaves a response that is itself the record list folded", () => {
    const list: ResponseNode = { key: "", path: "", kind: "list", length: 2, records: true };
    expect([...defaultOpenPaths(list)]).toEqual([]);
    expect([...defaultOpenPaths(null)]).toEqual([]);
  });
});

describe("the request as shown", () => {
  it("names the secret where the value was masked", () => {
    expect(showSecret("api_key=REDACTED", "acme_prod")).toBe("api_key=‹acme_prod›");
    expect(showSecret("Bearer REDACTED", "")).toBe("Bearer ‹secret›");
  });

  it("shows a failed test as the engine worded it", () => {
    const detail =
      "Failed to fetch sample from REST API: RestApiRequestError: GET https://x.test/a?key=REDACTED answered 401 Unauthorized: denied";
    expect(testErrorText(detail, "acme")).toBe(
      "GET https://x.test/a?key=‹acme› answered 401 Unauthorized: denied",
    );
    expect(testErrorText("Network Error", "")).toBe("Network Error");
  });

  it("summarises what came back", () => {
    expect(responseLine(entry())).toBe("200 · 312 ms · 4.2 KB · 50 records");
    expect(responseLine(entry({ bytes: 72, records: 1 }))).toBe("200 · 312 ms · 72 B · 1 record");
  });

  it("writes a curl command whose secret is a variable", () => {
    expect(toCurl(entry(), "acme-prod")).toBe(
      [
        'curl -X GET "https://api.example.com/items?api_key=$ACME_PROD"',
        '-H "Authorization: Bearer $ACME_PROD"',
        "-H 'X-Team: data'",
      ].join(" \\\n  "),
    );
  });

  it("sends a POST body as JSON and quotes it for the shell", () => {
    const curl = toCurl(
      entry({ method: "POST", url: "https://x.test/q", headers: {}, body: { q: "it's" } }),
      "",
    );
    expect(curl).toBe(
      [
        "curl -X POST 'https://x.test/q'",
        "-H 'Content-Type: application/json'",
        `-d '{"q":"it'\\''s"}'`,
      ].join(" \\\n  "),
    );
  });
});

describe("paging detection", () => {
  it("guesses the request parameter from the response field", () => {
    expect(guessCursorParam("meta.next_cursor")).toBe("cursor");
    expect(guessCursorParam("nextPageToken")).toBe("pageToken");
    expect(guessCursorParam("paging.after")).toBe("after");
    expect(guessCursorParam("next")).toBe("cursor");
    expect(guessCursorParam("data.-1._id")).toBe("_id");
  });

  it("shows what each next request carries for the token", () => {
    expect(nextRequestExample("cursor", false)).toEqual({
      code: "cursor=‹token›",
      place: "the URL",
    });
    expect(nextRequestExample("data_id", true)).toEqual({
      code: '"data_id": ‹token›',
      place: "the JSON body",
    });
    expect(nextRequestExample("page.after", true)?.code).toBe('"page": {"after": ‹token›}');
    expect(nextRequestExample("page.after", false)?.code).toBe("page.after=‹token›");
    expect(nextRequestExample("  ", false)).toBeNull();
  });

  it("offers a field of the response, never one of a record", () => {
    expect(detectedCursorPath(["data.-1._id", "meta.next"])).toBe("meta.next");
    expect(detectedCursorPath(["data.-1._id", "0.id"])).toBeNull();
    expect(detectedCursorPath([])).toBeNull();
  });
});

describe("limitsSummary", () => {
  it("names the limits a run has, page limits only while it pages", () => {
    const s = defaults();
    expect(limitsSummary(s)).toBe("30 s timeout · 3 retries");

    s.max_retries = 1;
    s.pagination = {
      ...s.pagination,
      pagination_type: "cursor",
      max_records: 200,
      page_delay_seconds: 0.5,
    };
    expect(limitsSummary(s)).toBe(
      "1000 pages · 200 records · 0.5 s between pages · 30 s timeout · 1 retry",
    );
  });
});

describe("columnRows", () => {
  const fields = [
    { name: "id", data_type: "Int64" },
    { name: "post.uri", data_type: "String" },
    { name: "post.author.handle", data_type: "String" },
    { name: "post.author.associated.chat.allowIncoming", data_type: "String" },
    { name: "reason.$type", data_type: "String" },
    { name: "post.likeCount", data_type: "Int64" },
  ];

  it("folds every group at first, in the order the columns come", () => {
    expect(columnRows(fields, new Set()).map((r) => [r.key, r.group, r.count])).toEqual([
      ["id", false, 1],
      ["post", true, 4],
      ["reason", true, 1],
    ]);
  });

  it("lists a group's children while it is open, a lone child group as one step", () => {
    const rows = columnRows(
      fields,
      new Set(["post", "post.author", "post.author.associated.chat"]),
    );
    expect(rows.map((r) => [r.key, r.depth, r.dataType])).toEqual([
      ["id", 0, "Int64"],
      ["post", 0, ""],
      ["uri", 1, "String"],
      ["author", 1, ""],
      ["handle", 2, "String"],
      ["associated.chat", 2, ""],
      ["allowIncoming", 3, "String"],
      ["likeCount", 1, "Int64"],
      ["reason", 0, ""],
    ]);
    expect(rows.find((r) => r.key === "allowIncoming")?.path).toBe(
      "post.author.associated.chat.allowIncoming",
    );
  });

  it("names every group path for unfolding them all", () => {
    expect([...columnGroupPaths(fields)].sort()).toEqual([
      "post",
      "post.author",
      "post.author.associated",
      "post.author.associated.chat",
      "reason",
    ]);
  });
});
