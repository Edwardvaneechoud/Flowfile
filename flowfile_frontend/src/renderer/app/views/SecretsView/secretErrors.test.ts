import { describe, expect, it } from "vitest";
import { DELETE_FAILED, deleteErrorMessage } from "./secretErrors";

describe("deleteErrorMessage", () => {
  it("shows the server's sentence when the delete was refused with one", () => {
    const said =
      "This secret is the sign-in of the catalog polaris; change the sign-in on the catalog or remove the " +
      "catalog from Flowfile instead.";
    expect(deleteErrorMessage({ response: { status: 400, data: { detail: said } } })).toBe(said);
  });

  it("keeps the page's own text for a failure that says nothing", () => {
    expect(DELETE_FAILED).toBe("Failed to delete secret. Please try again.");
    expect(deleteErrorMessage(new Error("Network Error"))).toBe(DELETE_FAILED);
    expect(deleteErrorMessage({ response: { status: 500, data: "Internal Server Error" } })).toBe(
      DELETE_FAILED,
    );
    expect(deleteErrorMessage({ response: { status: 422, data: { detail: [] } } })).toBe(
      DELETE_FAILED,
    );
    expect(deleteErrorMessage({ response: { status: 400, data: { detail: "  " } } })).toBe(
      DELETE_FAILED,
    );
    expect(deleteErrorMessage(undefined)).toBe(DELETE_FAILED);
  });
});
