import { describe, expect, it } from "vitest";
import { isProxy, reactive } from "vue";
import { toPlainJson, toPlainShared } from "./structuredClone";

describe("toPlainShared", () => {
  const value = {
    tiles: [
      { id: "a", image: { data_url: "data:image/png;base64,AAAA", alt: undefined }, x: 1 },
      { id: "b", image: null, flags: [true, undefined, 2] },
    ],
    name: "Board",
    missing: undefined,
  };

  it("copies like a JSON round trip: same content, undefined keys dropped", () => {
    expect(toPlainShared(value)).toEqual(toPlainJson(value));
    expect(JSON.stringify(toPlainShared(value))).toBe(JSON.stringify(value));
    expect("missing" in toPlainShared(value)).toBe(false);
  });

  it("strips reactive proxies and shares no object with the source", () => {
    const source = reactive(structuredClone(value));
    const copy = toPlainShared(source);
    expect(isProxy(copy)).toBe(false);
    expect(isProxy(copy.tiles[0])).toBe(false);
    copy.tiles[0].x = 99;
    (copy.tiles[0].image as { alt?: string }).alt = "changed";
    expect(source.tiles[0].x).toBe(1);
    expect(source.tiles[0].image?.alt).toBeUndefined();
  });
});
