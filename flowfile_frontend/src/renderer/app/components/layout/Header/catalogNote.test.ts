import { beforeEach, describe, expect, it } from "vitest";
import { noteAtOpen, resetNotesShown } from "./catalogNote";

const NOTE = "Saved as your draft on the hub.";

describe("noteAtOpen", () => {
  beforeEach(resetNotesShown);

  it("shows the catalog's own note once when its flow opens", () => {
    expect(noteAtOpen({ flow_id: 3, save_note: NOTE })).toBe(NOTE);
    expect(noteAtOpen({ flow_id: 3, save_note: NOTE })).toBeNull();
    expect(noteAtOpen({ flow_id: 4, save_note: NOTE })).toBe(NOTE);
  });

  it("shows nothing for a flow the catalog says nothing of", () => {
    expect(noteAtOpen({ flow_id: 3, save_note: null })).toBeNull();
    expect(noteAtOpen({ flow_id: 3 })).toBeNull();
    expect(noteAtOpen({ flow_id: 3, save_note: NOTE })).toBe(NOTE);
  });
});
