// Unit tests for the intent store: v3→v4 migration at panel registration
// (corrupted records heal to defaults, healthy ones convert), gesture-only
// persistence, and reset. Pinia + a fake localStorage are set up per-test.
// The fake stores data as enumerable own-properties so `Object.keys(...)`
// (used by the legacy-key purge) behaves like a real Storage.

import { createPinia, setActivePinia } from "pinia";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { defaultIntent, type PanelConfig } from "./layoutGeometry";
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

const v3Key = (id: string) => `overlayPositionAndSize.v3_${id}`;
const v4Key = (id: string) => `overlayPositionAndSize.v4_${id}`;

const v3Layout = (width: number, height: number, patch: Record<string, unknown> = {}) =>
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
    ...patch,
  });

const rightDrawerCfg: PanelConfig = {
  defaultDock: "right",
  h: { behaviour: "fixed", defaultSize: 600, defaultOffset: 0 },
  v: { behaviour: "scale", defaultSize: 700, defaultOffset: 8 },
};

beforeEach(() => {
  vi.stubGlobal("localStorage", fakeLocalStorage());
  setActivePinia(createPinia());
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("v3 migration at panel registration", () => {
  it("heals the exact collapse fingerprint (150x100) to defaults", () => {
    localStorage.setItem(v3Key("rightDrawer"), v3Layout(150, 100, { left: 464, top: 0 }));
    const store = useItemStore();
    const intent = store.registerPanel("rightDrawer", rightDrawerCfg);
    expect(intent).toEqual(defaultIntent(rightDrawerCfg));
    expect(localStorage.getItem(v3Key("rightDrawer"))).toBeNull();
    expect(localStorage.getItem(v4Key("rightDrawer"))).toBeNull();
  });

  it("heals one-axis crushes the old sweep missed", () => {
    localStorage.setItem(v3Key("rightDrawer"), v3Layout(600, 100));
    const store = useItemStore();
    expect(store.registerPanel("rightDrawer", rightDrawerCfg)).toEqual(
      defaultIntent(rightDrawerCfg),
    );
    expect(localStorage.getItem(v3Key("rightDrawer"))).toBeNull();
  });

  it("converts a healthy v3 record to a v4 intent record", () => {
    localStorage.setItem(v3Key("rightDrawer"), v3Layout(640, 800));
    const store = useItemStore();
    const intent = store.registerPanel("rightDrawer", rightDrawerCfg);
    expect(intent).toEqual({
      dock: "right",
      h: { size: 640, offset: null, gap: null },
      v: { size: null, offset: null, gap: null },
      fullScreen: false,
    });
    expect(localStorage.getItem(v3Key("rightDrawer"))).toBeNull();
    expect(JSON.parse(localStorage.getItem(v4Key("rightDrawer"))!)).toEqual(intent);
  });

  it("drops unparseable v3 JSON and falls back to defaults", () => {
    localStorage.setItem(v3Key("broken"), "{ not json");
    const store = useItemStore();
    expect(store.registerPanel("broken", rightDrawerCfg)).toEqual(defaultIntent(rightDrawerCfg));
    expect(localStorage.getItem(v3Key("broken"))).toBeNull();
  });
});

describe("v4 load", () => {
  it("loads a healthy v4 record", () => {
    const saved = {
      dock: "right",
      h: { size: 500, offset: null, gap: null },
      v: { size: null, offset: 8, gap: 250 },
      fullScreen: false,
    };
    localStorage.setItem(v4Key("rightDrawer"), JSON.stringify(saved));
    const store = useItemStore();
    expect(store.registerPanel("rightDrawer", rightDrawerCfg)).toEqual(saved);
  });

  it("drops a degenerate v4 record and heals to defaults", () => {
    localStorage.setItem(
      v4Key("rightDrawer"),
      JSON.stringify({ dock: "right", h: { size: 20 }, v: {} }),
    );
    const store = useItemStore();
    expect(store.registerPanel("rightDrawer", rightDrawerCfg)).toEqual(
      defaultIntent(rightDrawerCfg),
    );
    expect(localStorage.getItem(v4Key("rightDrawer"))).toBeNull();
  });

  it("keeps the in-session intent when a panel remounts", () => {
    const store = useItemStore();
    store.registerPanel("rightDrawer", rightDrawerCfg);
    const resized = {
      ...defaultIntent(rightDrawerCfg),
      h: { size: 480, offset: null, gap: null },
    };
    store.commitIntent("rightDrawer", resized);
    // Drawer close/open remounts the component; the session intent must win.
    expect(store.registerPanel("rightDrawer", rightDrawerCfg)).toEqual(resized);
  });
});

describe("persistence discipline", () => {
  it("commitIntent writes the v4 record", () => {
    const store = useItemStore();
    store.registerPanel("rightDrawer", rightDrawerCfg);
    const intent = {
      ...defaultIntent(rightDrawerCfg),
      h: { size: 480, offset: null, gap: null },
    };
    store.commitIntent("rightDrawer", intent);
    expect(JSON.parse(localStorage.getItem(v4Key("rightDrawer"))!)).toEqual(intent);
  });

  it("registering a panel with no stored record writes nothing", () => {
    const store = useItemStore();
    store.registerPanel("rightDrawer", rightDrawerCfg);
    expect(localStorage.getItem(v4Key("rightDrawer"))).toBeNull();
  });

  it("resetLayout removes stored records and restores defaults", () => {
    const store = useItemStore();
    store.registerPanel("rightDrawer", rightDrawerCfg);
    store.commitIntent("rightDrawer", {
      ...defaultIntent(rightDrawerCfg),
      h: { size: 480, offset: null, gap: null },
    });
    store.resetLayout();
    expect(localStorage.getItem(v4Key("rightDrawer"))).toBeNull();
    expect(store.items["rightDrawer"]).toEqual(defaultIntent(rightDrawerCfg));
  });
});

describe("legacy key handling at store init", () => {
  it("purges pre-v3 keys and group keys, keeps v3 and v4 and unrelated keys", () => {
    localStorage.setItem("overlayPositionAndSize_old", "{}");
    localStorage.setItem("overlayPositionAndSize.v2_x", "{}");
    localStorage.setItem("overlayGroups.v3", "{}");
    localStorage.setItem(v3Key("rightDrawer"), v3Layout(640, 800));
    localStorage.setItem(v4Key("bottomDock"), "{}");
    localStorage.setItem("someOtherKey", "value");
    localStorage.setItem("layoutControlsPositionV2", '{"x":10,"y":20}');
    useItemStore();
    expect(localStorage.getItem("overlayPositionAndSize_old")).toBeNull();
    expect(localStorage.getItem("overlayPositionAndSize.v2_x")).toBeNull();
    expect(localStorage.getItem("overlayGroups.v3")).toBeNull();
    expect(localStorage.getItem(v3Key("rightDrawer"))).not.toBeNull();
    expect(localStorage.getItem(v4Key("bottomDock"))).not.toBeNull();
    expect(localStorage.getItem("someOtherKey")).toBe("value");
    expect(localStorage.getItem("layoutControlsPositionV2")).toBe('{"x":10,"y":20}');
  });
});

describe("z-order", () => {
  it("bringToFront raises the panel above its peers, fullscreen pins the top", () => {
    const store = useItemStore();
    store.registerPanel("a", rightDrawerCfg);
    store.registerPanel("b", rightDrawerCfg);
    store.bringToFront("a");
    expect(store.zIndexFor("a")).toBeGreaterThan(store.zIndexFor("b"));
    store.setFullScreen("b", true);
    expect(store.zIndexFor("b")).toBeGreaterThan(store.zIndexFor("a"));
  });
});
