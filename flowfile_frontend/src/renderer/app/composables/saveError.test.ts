import { describe, it, expect } from "vitest";
import { extractSaveErrorMessage } from "./saveError";

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
