// Unit tests for the store's one-time geometry sanitizer: instantiating the
// store must drop only current-version panel entries pinned at the collapse
// fingerprint (width <= 150 && height <= 100) and leave everything else intact.
// Pinia + a fake localStorage are set up per-test. The fake stores data as
// enumerable own-properties so `Object.keys(localStorage)` (used by the sweep)
// behaves like a real Storage — a Map-backed fake would hide the keys.

import { setActivePinia, createPinia } from "pinia";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { useItemStore } from "./stateStore";

function fakeLocalStorage(): Storage {
  const ls = {} as Record<string, string> & Storage;
  Object.defineProperties(ls, {
    getItem: {
      value: (k: string) => (Object.prototype.hasOwnProperty.call(ls, k) ? ls[k] : null),
      enumerable: false,
    },
    setItem: {
      value: (k: string, v: string) => {
        ls[k] = String(v);
      },
      enumerable: false,
    },
    removeItem: {
      value: (k: string) => {
        delete ls[k];
      },
      enumerable: false,
    },
    clear: {
      value: () => {
        for (const k of Object.keys(ls)) delete ls[k];
      },
      enumerable: false,
    },
    key: { value: (i: number) => Object.keys(ls)[i] ?? null, enumerable: false },
    length: { get: () => Object.keys(ls).length, enumerable: false },
  });
  return ls;
}

const key = (id: string) => `overlayPositionAndSize.v3_${id}`;

const layout = (width: number, height: number) =>
  JSON.stringify({
    width,
    height,
    left: 20,
    top: 8,
    stickynessPosition: "right",
    fullWidth: false,
    fullHeight: false,
    zIndex: 100,
    fullScreen: false,
    clicked: false,
  });

beforeEach(() => {
  vi.stubGlobal("localStorage", fakeLocalStorage());
  setActivePinia(createPinia());
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("purgeCorruptedGeometry (via store init)", () => {
  it("drops an entry pinned at the exact collapse floor (150x100)", () => {
    localStorage.setItem(key("rightDrawer"), layout(150, 100));
    useItemStore();
    expect(localStorage.getItem(key("rightDrawer"))).toBeNull();
  });

  it("drops entries at or below the fingerprint (width<=150 && height<=100)", () => {
    localStorage.setItem(key("a"), layout(120, 80));
    localStorage.setItem(key("b"), layout(150, 100));
    localStorage.setItem(key("c"), layout(90, 90));
    useItemStore();
    expect(localStorage.getItem(key("a"))).toBeNull();
    expect(localStorage.getItem(key("b"))).toBeNull();
    expect(localStorage.getItem(key("c"))).toBeNull();
  });

  it("keeps a healthy saved layout", () => {
    localStorage.setItem(key("rightDrawer"), layout(600, 400));
    useItemStore();
    expect(localStorage.getItem(key("rightDrawer"))).toBe(layout(600, 400));
  });

  it("keeps entries just outside the fingerprint on either axis", () => {
    // width over the floor — a real (if narrow) panel, not a collapse.
    localStorage.setItem(key("wide"), layout(151, 100));
    // height over the floor — unreachable by the collapse bug.
    localStorage.setItem(key("tall"), layout(150, 101));
    useItemStore();
    expect(localStorage.getItem(key("wide"))).toBe(layout(151, 100));
    expect(localStorage.getItem(key("tall"))).toBe(layout(150, 101));
  });

  it("removes only the corrupted entry from a mixed set", () => {
    localStorage.setItem(key("bad"), layout(150, 100));
    localStorage.setItem(key("good"), layout(500, 300));
    useItemStore();
    expect(localStorage.getItem(key("bad"))).toBeNull();
    expect(localStorage.getItem(key("good"))).toBe(layout(500, 300));
  });

  it("drops an entry with unparseable JSON", () => {
    localStorage.setItem(key("broken"), "{ not json");
    useItemStore();
    expect(localStorage.getItem(key("broken"))).toBeNull();
  });

  it("ignores unrelated localStorage keys", () => {
    localStorage.setItem("someOtherKey", "value");
    localStorage.setItem("layoutControlsPositionV2", '{"x":10,"y":20}');
    useItemStore();
    expect(localStorage.getItem("someOtherKey")).toBe("value");
    expect(localStorage.getItem("layoutControlsPositionV2")).toBe('{"x":10,"y":20}');
  });
});
