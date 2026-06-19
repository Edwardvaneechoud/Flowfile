import { describe, it, expect } from "vitest";
import { nodeDocsUrl } from "./nodeDocs";

describe("nodeDocsUrl", () => {
  it("returns the category docs page for documented node groups", () => {
    expect(nodeDocsUrl("input")).toBe(
      "https://edwardvaneechoud.github.io/Flowfile/users/visual-editor/nodes/input",
    );
    expect(nodeDocsUrl("transform")).toBe(
      "https://edwardvaneechoud.github.io/Flowfile/users/visual-editor/nodes/transform",
    );
    expect(nodeDocsUrl("combine")).toBe(
      "https://edwardvaneechoud.github.io/Flowfile/users/visual-editor/nodes/combine",
    );
    expect(nodeDocsUrl("aggregate")).toBe(
      "https://edwardvaneechoud.github.io/Flowfile/users/visual-editor/nodes/aggregate",
    );
    expect(nodeDocsUrl("ml")).toBe(
      "https://edwardvaneechoud.github.io/Flowfile/users/visual-editor/nodes/ml",
    );
    expect(nodeDocsUrl("output")).toBe(
      "https://edwardvaneechoud.github.io/Flowfile/users/visual-editor/nodes/output",
    );
  });

  it("returns an empty string for groups without a docs page", () => {
    expect(nodeDocsUrl("custom")).toBe("");
    expect(nodeDocsUrl("")).toBe("");
    expect(nodeDocsUrl("unknown")).toBe("");
  });
});
