import { describe, it, expect } from "vitest";
import { NO_AUTOFILL } from "./noAutofill";

describe("NO_AUTOFILL", () => {
  it("carries the opt-out each password manager actually reads", () => {
    // Not interchangeable: every manager reads its own attribute.
    expect(NO_AUTOFILL).toMatchObject({
      autocomplete: "off",
      "data-lpignore": "true",
      "data-1p-ignore": "true",
      "data-bwignore": "true",
      "data-form-type": "other",
    });
  });

  it("does not rely on autocomplete alone", () => {
    // Chrome ignores autocomplete="off" for address-shaped fields.
    expect(Object.keys(NO_AUTOFILL).length).toBeGreaterThan(1);
  });
});
