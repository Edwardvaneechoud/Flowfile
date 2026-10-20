import { describe, expect, it } from "vitest";
import {
  POPOUT_KINDS,
  POPOUT_ROUTES,
  flowLabel,
  isPopoutHash,
  isPopoutKind,
  isPopoutMessage,
  parseFlowQuery,
  popoutWindowHash,
  popoutWindowName,
  popoutWindowUrl,
  windowTitle,
} from "./popoutWindow";

describe("popoutWindow", () => {
  it("recognises every kind's route hash and nothing else", () => {
    for (const kind of POPOUT_KINDS) {
      expect(isPopoutHash(`#${POPOUT_ROUTES[kind]}`)).toBe(true);
      expect(isPopoutHash(`#${POPOUT_ROUTES[kind]}?flow=3`)).toBe(true);
    }
    expect(isPopoutHash("#/notebook?flow=3")).toBe(true);
    expect(isPopoutHash("#/popout/logs?flow=3")).toBe(true);
    expect(isPopoutHash("#/notebooks?flow=3")).toBe(false);
    expect(isPopoutHash("#/popout")).toBe(false);
    expect(isPopoutHash("#/popout/tables?flow=3")).toBe(false);
    expect(isPopoutHash("#/main/designer")).toBe(false);
    expect(isPopoutHash("")).toBe(false);
  });

  it("tells a kind from any other value", () => {
    expect(isPopoutKind("table")).toBe(true);
    expect(isPopoutKind("notebook")).toBe(true);
    expect(isPopoutKind("settings")).toBe(false);
    expect(isPopoutKind(3)).toBe(false);
    expect(isPopoutKind(undefined)).toBe(false);
  });

  it("builds the window name, route hash and url for a flow of each kind", () => {
    expect(popoutWindowName("notebook", 12)).toBe("flowfile-notebook-12");
    expect(popoutWindowName("logs", 12)).toBe("flowfile-logs-12");
    expect(popoutWindowHash("notebook", 12)).toBe("#/notebook?flow=12");
    expect(popoutWindowHash("table", 12, { node: 3 })).toBe("#/popout/table?flow=12&node=3");
    expect(
      popoutWindowUrl("#/notebook?flow=12", { origin: "http://localhost:8080", pathname: "/" }),
    ).toBe("http://localhost:8080/#/notebook?flow=12");
    expect(
      popoutWindowUrl("#/notebook?flow=12", { origin: "tauri://localhost", pathname: "/index.html" }),
    ).toBe("tauri://localhost/index.html#/notebook?flow=12");
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

  it("titles the window after its kind and the flow", () => {
    expect(windowTitle("notebook", "orders")).toBe("Notebook – orders");
    expect(windowTitle("notebook", null)).toBe("Notebook");
    expect(windowTitle("notebook", "")).toBe("Notebook");
    expect(windowTitle("table", "orders")).toBe("Data – orders");
    expect(windowTitle("logs", undefined)).toBe("Logs");
    expect(windowTitle("ai", "orders")).toBe("AI Assistant – orders");
  });

  it("accepts a selection message and nothing else", () => {
    expect(isPopoutMessage({ type: "selection", previewNodeId: 3, previewToken: 2 })).toBe(true);
    expect(isPopoutMessage({ type: "selection", previewNodeId: null, previewToken: 0 })).toBe(true);
    expect(isPopoutMessage({ type: "preview", previewNodeId: 3, previewToken: 1 })).toBe(false);
    expect(isPopoutMessage({ type: "selection", previewNodeId: "3", previewToken: 1 })).toBe(false);
    expect(isPopoutMessage({ type: "selection", previewNodeId: 3, previewToken: "1" })).toBe(false);
    expect(isPopoutMessage({ type: "selection", previewNodeId: 3 })).toBe(false);
    expect(isPopoutMessage(null)).toBe(false);
  });

  it("labels the flow as the designer's tab bar does", () => {
    expect(flowLabel({ name: "orders.flowfile", display_name: "Orders" })).toBe("Orders");
    expect(flowLabel({ name: "orders", display_name: null })).toBe("orders");
    expect(flowLabel({ name: "Untitled flow 2026-10-08 04:19:57" })).toBe(
      "Untitled flow 2026-10-08 04:19:57",
    );
  });
});
