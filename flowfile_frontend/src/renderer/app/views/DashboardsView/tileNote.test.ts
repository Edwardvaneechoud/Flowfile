import { describe, expect, it } from "vitest";

import type { DashboardTile, DashboardTileType } from "../../types";
import { noteMenuLabel, takesNote, tileNote, withNote } from "./tileNote";

const tile = (type: DashboardTileType, note_md?: string | null): DashboardTile => ({
  id: "t1",
  type,
  viz_id: 1,
  chart_index: 0,
  note_md,
  x: 0,
  y: 0,
  w: 6,
  h: 4,
});

describe("takesNote", () => {
  it("is true for tiles that show data or a control", () => {
    expect(["viz", "kpi", "table", "filter"].every((t) => takesNote(t as DashboardTileType))).toBe(
      true,
    );
  });

  it("is false for text blocks and separators", () => {
    expect(takesNote("text")).toBe(false);
    expect(takesNote("separator")).toBe(false);
  });
});

describe("tileNote", () => {
  it("is the note when there is one", () => {
    expect(tileNote(tile("viz", "Net of refunds"))).toBe("Net of refunds");
  });

  it("is null for a missing, null or blank note", () => {
    expect(tileNote(tile("viz"))).toBeNull();
    expect(tileNote(tile("viz", null))).toBeNull();
    expect(tileNote(tile("kpi", "  \n "))).toBeNull();
  });

  it("is null on a tile type that takes no note", () => {
    expect(tileNote(tile("text", "a stray note"))).toBeNull();
  });
});

describe("noteMenuLabel", () => {
  it("offers to add a note, or to edit the one there is", () => {
    expect(noteMenuLabel(tile("table"))).toBe("Add note…");
    expect(noteMenuLabel(tile("table", "Rows since 2024"))).toBe("Edit note…");
  });
});

describe("withNote", () => {
  it("sets the draft as the note", () => {
    expect(withNote(tile("viz"), "**Net** of refunds")?.note_md).toBe("**Net** of refunds");
  });

  it("removes the note for a blank draft", () => {
    const next = withNote(tile("viz", "Old"), "   ");
    expect(next).not.toBeNull();
    expect(next?.note_md).toBeNull();
  });

  it("keeps leading indentation and drops trailing whitespace", () => {
    expect(withNote(tile("viz"), "    code\n\n")?.note_md).toBe("    code");
  });

  it("is null when nothing changes", () => {
    expect(withNote(tile("viz", "Same"), "Same")).toBeNull();
    expect(withNote(tile("viz", "Same"), "Same\n")).toBeNull();
    expect(withNote(tile("viz"), "")).toBeNull();
    expect(withNote(tile("viz", null), "  ")).toBeNull();
  });

  it("keeps the rest of the tile", () => {
    const before = { ...tile("kpi"), title: "Revenue", bg_color: "#fef3c7" };
    expect(withNote(before, "Note")).toEqual({ ...before, note_md: "Note" });
  });
});
