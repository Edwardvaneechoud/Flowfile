import { describe, expect, it } from "vitest";
import {
  isPopoutHash,
  notebookWindowName,
  notebookWindowUrl,
  parseFlowQuery,
  windowTitle,
} from "./popoutWindow";

describe("popoutWindow", () => {
  it("recognises the pop-out route's hash only", () => {
    expect(isPopoutHash("#/notebook?flow=3")).toBe(true);
    expect(isPopoutHash("#/notebook")).toBe(true);
    expect(isPopoutHash("#/notebooks?flow=3")).toBe(false);
    expect(isPopoutHash("#/main/designer")).toBe(false);
    expect(isPopoutHash("")).toBe(false);
  });

  it("builds the window name and url for a flow", () => {
    expect(notebookWindowName(12)).toBe("flowfile-notebook-12");
    expect(notebookWindowUrl(12, { origin: "http://localhost:8080", pathname: "/" })).toBe(
      "http://localhost:8080/#/notebook?flow=12",
    );
    expect(notebookWindowUrl(12, { origin: "tauri://localhost", pathname: "/index.html" })).toBe(
      "tauri://localhost/index.html#/notebook?flow=12",
    );
  });

  it("reads a positive integer flow id from the query and -1 otherwise", () => {
    expect(parseFlowQuery("7")).toBe(7);
    expect(parseFlowQuery(["7", "8"])).toBe(7);
    expect(parseFlowQuery(7)).toBe(7);
    expect(parseFlowQuery("0")).toBe(-1);
    expect(parseFlowQuery("-3")).toBe(-1);
    expect(parseFlowQuery("7.5")).toBe(-1);
    expect(parseFlowQuery("abc")).toBe(-1);
    expect(parseFlowQuery(undefined)).toBe(-1);
    expect(parseFlowQuery(null)).toBe(-1);
  });

  it("titles the window after the flow", () => {
    expect(windowTitle("orders")).toBe("Notebook – orders");
    expect(windowTitle(null)).toBe("Notebook");
    expect(windowTitle("")).toBe("Notebook");
  });
});
