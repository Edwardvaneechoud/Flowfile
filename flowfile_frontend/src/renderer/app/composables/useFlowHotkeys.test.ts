// @vitest-environment happy-dom
import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  createFlowHotkeysHandler,
  historyShortcutFor,
  isEditableKeydownTarget,
} from "./useFlowHotkeys";
import type { FlowHotkeyActions } from "./useFlowHotkeys";

const makeActions = (flowId = 1): { actions: FlowHotkeyActions; spies: Record<string, ReturnType<typeof vi.fn>> } => {
  const spies = {
    selectAll: vi.fn(),
    newFlow: vi.fn(),
    save: vi.fn(),
    run: vi.fn(),
    toggleCodeGenerator: vi.fn(),
    openFlowSettings: vi.fn(),
    openFile: vi.fn(),
    toggleAiDrawer: vi.fn(),
  };
  return { actions: { flowId: () => flowId, ...spies }, spies };
};

const makeEvent = (key: string, target: EventTarget | null = document.body) => {
  const preventDefault = vi.fn();
  return {
    event: { key, metaKey: true, ctrlKey: false, target, preventDefault } as unknown as KeyboardEvent,
    preventDefault,
  };
};

const editableTargets = (): Record<string, Element> => {
  document.body.innerHTML = `
    <input id="input" />
    <div class="cm-editor"><div id="cm" class="cm-content" contenteditable="true"></div></div>
    <button id="button"></button>
  `;
  return {
    input: document.getElementById("input")!,
    cm: document.getElementById("cm")!,
    button: document.getElementById("button")!,
  };
};

describe("isEditableKeydownTarget", () => {
  it("detects inputs and CodeMirror, not plain buttons", () => {
    const els = editableTargets();
    expect(isEditableKeydownTarget(els.input)).toBe(true);
    expect(isEditableKeydownTarget(els.cm)).toBe(true);
    expect(isEditableKeydownTarget(els.button)).toBe(false);
    expect(isEditableKeydownTarget(null)).toBe(false);
  });
});

describe("createFlowHotkeysHandler guard policy", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
  });

  it("ignores keys without a modifier", () => {
    const { actions, spies } = makeActions();
    const handler = createFlowHotkeysHandler(actions);
    handler({ key: "s", metaKey: false, ctrlKey: false, target: null, preventDefault: vi.fn() } as unknown as KeyboardEvent);
    expect(spies.save).not.toHaveBeenCalled();
  });

  it.each([
    ["s", "save"],
    ["e", "run"],
    ["g", "toggleCodeGenerator"],
    [",", "openFlowSettings"],
    ["n", "newFlow"],
  ] as const)("app-global Cmd+%s fires even while typing in an editor", (key, spy) => {
    const els = editableTargets();
    const { actions, spies } = makeActions();
    const handler = createFlowHotkeysHandler(actions);
    const { event, preventDefault } = makeEvent(key, els.cm);
    handler(event);
    expect(spies[spy]).toHaveBeenCalledTimes(1);
    expect(preventDefault).toHaveBeenCalled();
  });

  it.each([
    ["a", "selectAll"],
    ["o", "openFile"],
    ["k", "toggleAiDrawer"],
  ] as const)("canvas-scoped Cmd+%s is suppressed while typing", (key, spy) => {
    const els = editableTargets();
    const { actions, spies } = makeActions();
    const handler = createFlowHotkeysHandler(actions);
    for (const target of [els.input, els.cm]) {
      const { event, preventDefault } = makeEvent(key, target);
      handler(event);
      expect(spies[spy]).not.toHaveBeenCalled();
      expect(preventDefault).not.toHaveBeenCalled();
    }
    const { event } = makeEvent(key, document.body);
    handler(event);
    expect(spies[spy]).toHaveBeenCalledTimes(1);
  });

  it.each([
    ["s", "save"],
    ["e", "run"],
    ["g", "toggleCodeGenerator"],
    [",", "openFlowSettings"],
    ["k", "toggleAiDrawer"],
  ] as const)("Cmd+%s requires an open flow", (key, spy) => {
    const { actions, spies } = makeActions(-1);
    const handler = createFlowHotkeysHandler(actions);
    const { event, preventDefault } = makeEvent(key);
    handler(event);
    expect(spies[spy]).not.toHaveBeenCalled();
    expect(preventDefault).not.toHaveBeenCalled();
  });

  it("Cmd+N works without an open flow", () => {
    const { actions, spies } = makeActions(-1);
    createFlowHotkeysHandler(actions)(makeEvent("n").event);
    expect(spies.newFlow).toHaveBeenCalledTimes(1);
  });

  it("normalizes Caps Lock (uppercase keys)", () => {
    const { actions, spies } = makeActions();
    createFlowHotkeysHandler(actions)(makeEvent("S").event);
    expect(spies.save).toHaveBeenCalledTimes(1);
  });

  it("does not handle Cmd+C / Cmd+V (owned by the ClipboardEvent listeners)", () => {
    const { actions, spies } = makeActions();
    const handler = createFlowHotkeysHandler(actions);
    const c = makeEvent("c");
    const v = makeEvent("v");
    handler(c.event);
    handler(v.event);
    expect(c.preventDefault).not.toHaveBeenCalled();
    expect(v.preventDefault).not.toHaveBeenCalled();
    for (const spy of Object.values(spies)) expect(spy).not.toHaveBeenCalled();
  });
});

describe("historyShortcutFor", () => {
  const press = (
    key: string,
    target: EventTarget | null,
    { mac = true, shift = false, alt = false, handled = false, ctrl = false } = {},
  ) =>
    historyShortcutFor(
      {
        key,
        metaKey: mac && !ctrl,
        ctrlKey: !mac || ctrl,
        shiftKey: shift,
        altKey: alt,
        defaultPrevented: handled,
        target,
      } as unknown as KeyboardEvent,
      mac,
    );

  const mount = (html: string, id: string): Element => {
    document.body.innerHTML = html;
    return document.getElementById(id)!;
  };

  it("maps Cmd/Ctrl+Z, Shift+Cmd/Ctrl+Z and Ctrl+Y", () => {
    const body = document.body;
    expect(press("z", body)).toBe("undo");
    expect(press("Z", body, { shift: true })).toBe("redo");
    expect(press("z", body, { mac: false })).toBe("undo");
    expect(press("z", body, { mac: false, shift: true })).toBe("redo");
    expect(press("y", body, { mac: false })).toBe("redo");
  });

  it("needs the platform modifier and no Alt", () => {
    expect(press("z", document.body, { ctrl: true })).toBeNull();
    expect(press("z", document.body, { alt: true })).toBeNull();
    expect(press("x", document.body)).toBeNull();
  });

  it("leaves an already-handled event alone", () => {
    expect(press("z", document.body, { handled: true })).toBeNull();
  });

  it.each([
    ["an input", `<input id="t" />`],
    ["a textarea", `<textarea id="t"></textarea>`],
    ["a select", `<select id="t"><option>a</option></select>`],
    ["a combobox", `<div role="combobox"><span id="t"></span></div>`],
    ["a listbox", `<ul role="listbox"><li id="t"></li></ul>`],
    ["CodeMirror", `<div class="cm-editor"><div id="t"></div></div>`],
    [
      "a dialog",
      `<div class="el-overlay"><div class="el-dialog"><button id="t"></button></div></div>`,
    ],
    ["a message box", `<div class="el-message-box"><button id="t"></button></div>`],
    ["a popper", `<div class="el-popper"><div id="t"></div></div>`],
    ["the context menu", `<div class="context-menu"><div id="t"></div></div>`],
    ["a modal", `<div aria-modal="true"><button id="t"></button></div>`],
  ])("does not fire inside %s", (_, html) => {
    expect(press("z", mount(html, "t"))).toBeNull();
  });

  it("fires on a non-editable spot of the settings drawer (it closes the drawer, then undoes)", () => {
    const target = mount(
      `<div class="node-settings-drawer nokey"><div id="t">Filter settings</div><input /></div>`,
      "t",
    );
    expect(press("z", target)).toBe("undo");
  });

  it("does not fire for a text field inside a shadow root (GraphicWalker)", () => {
    const host = mount(`<div class="node-settings-drawer"><div id="t"></div></div>`, "t");
    const input = host.attachShadow({ mode: "open" }).appendChild(document.createElement("input"));
    const event = {
      key: "z",
      metaKey: true,
      ctrlKey: false,
      shiftKey: false,
      altKey: false,
      defaultPrevented: false,
      target: host,
      composedPath: () => [input, host.shadowRoot, host, document.body],
    } as unknown as KeyboardEvent;
    expect(historyShortcutFor(event, true)).toBeNull();
  });
});
