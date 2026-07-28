import { describe, it, expect } from "vitest";
import { catalogSaveErrorMessage, extractSaveErrorMessage } from "./saveError";

describe("extractSaveErrorMessage", () => {
  it("returns the core's string detail (e.g. a validation error)", () => {
    const error = {
      response: {
        data: {
          detail:
            "table_name: Invalid SQL identifier: 'tesst-2'. Only letters, numbers, and underscores are allowed.",
        },
      },
    };
    expect(extractSaveErrorMessage(error)).toBe(
      "table_name: Invalid SQL identifier: 'tesst-2'. Only letters, numbers, and underscores are allowed.",
    );
  });

  it("joins FastAPI's structured detail array", () => {
    const error = {
      response: {
        data: {
          detail: [
            { loc: ["body", "table_name"], msg: "field required" },
            { loc: ["body", "schema_name"], msg: "invalid value" },
          ],
        },
      },
    };
    expect(extractSaveErrorMessage(error)).toBe("field required; invalid value");
  });

  it("falls back when there is no usable detail (e.g. a network error)", () => {
    expect(extractSaveErrorMessage(new Error("Network Error"))).toBe(
      "Failed to save settings. Please check the node configuration.",
    );
    expect(extractSaveErrorMessage({ response: { data: { detail: "   " } } })).toBe(
      "Failed to save settings. Please check the node configuration.",
    );
  });
});

describe("catalogSaveErrorMessage structured details", () => {
  it("returns the message field of an object detail (stale_write 409)", () => {
    const err = {
      response: {
        status: 409,
        data: {
          detail: {
            error: "stale_write",
            resource_type: "dashboard",
            message: "This dashboard was modified in another session. Reload it or overwrite.",
          },
        },
      },
    };
    expect(catalogSaveErrorMessage(err, "fallback")).toBe(
      "This dashboard was modified in another session. Reload it or overwrite.",
    );
  });

  it("still falls back for object details without a message", () => {
    const err = { response: { status: 409, data: { detail: { error: "x" } } }, message: "boom" };
    expect(catalogSaveErrorMessage(err, "fallback")).toBe("boom");
  });
});
