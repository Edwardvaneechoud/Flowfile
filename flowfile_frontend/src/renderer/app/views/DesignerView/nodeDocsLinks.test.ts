import { describe, it, expect } from "vitest";
import { NODE_DOC_PAGES, nodeDocsUrl, nodeDocsMenuOptions } from "./nodeDocsLinks";
import type { NodeTemplate } from "../../types";

const BASE = "https://edwardvaneechoud.github.io/Flowfile/users/visual-editor/";
const INDEX = `${BASE}nodes/index.html`;
const CUSTOM = `${BASE}creating-custom-nodes.html`;

function makeNode(overrides: Partial<NodeTemplate>): NodeTemplate {
  return {
    name: "Node",
    color: "#fff",
    item: "node",
    input: 1,
    output: 1,
    image: "x.svg",
    multi: false,
    node_group: "custom",
    prod_ready: true,
    drawer_title: "t",
    drawer_intro: "i",
    custom_node: false,
    ...overrides,
  } as NodeTemplate;
}

describe("nodeDocsUrl", () => {
  it("resolves a mapped built-in to its page and anchor", () => {
    expect(nodeDocsUrl(makeNode({ item: "read", node_group: "input" }))).toBe(
      `${BASE}nodes/input.html#read-data`,
    );
  });

  // The two nodes whose docs page disagrees with their palette group. A
  // "just derive the page from node_group" refactor breaks exactly these.
  it("sends window_functions to transform.html despite its aggregate group", () => {
    expect(nodeDocsUrl(makeNode({ item: "window_functions", node_group: "aggregate" }))).toBe(
      `${BASE}nodes/transform.html#window-functions`,
    );
  });

  it("sends wait_for to ml.html despite its combine group", () => {
    expect(nodeDocsUrl(makeNode({ item: "wait_for", node_group: "combine" }))).toBe(
      `${BASE}nodes/ml.html#wait-for`,
    );
  });

  it("sends custom nodes to the custom-node guide", () => {
    expect(nodeDocsUrl(makeNode({ item: "my_thing", custom_node: true }))).toBe(CUSTOM);
  });

  it("prefers the custom-node guide over a colliding built-in item name", () => {
    expect(nodeDocsUrl(makeNode({ item: "filter", custom_node: true }))).toBe(CUSTOM);
  });

  it("falls back to the node index for undocumented built-ins", () => {
    expect(nodeDocsUrl(makeNode({ item: "polars_lazy_frame", node_group: "special" }))).toBe(INDEX);
    expect(nodeDocsUrl(makeNode({ item: "external_source", node_group: "input" }))).toBe(INDEX);
  });

  it("falls back to the node index when the template is missing", () => {
    expect(nodeDocsUrl(undefined)).toBe(INDEX);
    expect(nodeDocsUrl(null)).toBe(INDEX);
  });

  it("does not read through the prototype chain", () => {
    expect(nodeDocsUrl(makeNode({ item: "constructor" }))).toBe(INDEX);
    expect(nodeDocsUrl(makeNode({ item: "toString" }))).toBe(INDEX);
  });
});

describe("NODE_DOC_PAGES", () => {
  const entries = Object.entries(NODE_DOC_PAGES);

  it("points every entry at a real reference page with a slug anchor", () => {
    for (const [item, target] of entries) {
      expect(target, `${item} -> ${target}`).toMatch(
        /^(input|transform|combine|aggregate|ml|output)\.html#[a-z0-9]+(-[a-z0-9]+)*$/,
      );
    }
  });

  it("has no two nodes pointing at the same section", () => {
    const targets = entries.map(([, target]) => target);
    expect(new Set(targets).size).toBe(targets.length);
  });

  // 46 node templates ship in flowfile_core/configs/node_store/nodes.py; the two
  // without a docs section are external_source (dev-only) and polars_lazy_frame.
  it("covers the 44 documented node types", () => {
    expect(entries).toHaveLength(44);
  });
});

describe("nodeDocsMenuOptions", () => {
  it("pins the label and the action string the handlers switch on", () => {
    expect(nodeDocsMenuOptions()).toEqual([{ label: "Read more", action: "docs" }]);
  });
});
