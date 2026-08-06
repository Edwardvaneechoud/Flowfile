// Unit tests for the node-designer store: DesignerState-backed state transitions,
// dirty tracking, and the per-node localStorage draft lifecycle (debounced write,
// restore banner, discard, legacy sessionStorage + v1-draft migration).
//
// Runs in the node env: mock Storage objects are installed on globalThis,
// which is where the store resolves localStorage/sessionStorage from.

import { setActivePinia, createPinia } from "pinia";
import { nextTick } from "vue";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  DRAFT_DEBOUNCE_MS,
  DRAFT_KEY_PREFIX,
  DRAFT_VERSION,
  NEW_NODE_DRAFT_KEY,
  useNodeDesignerStore,
} from "./node-designer-store";
import { STORAGE_KEY, defaultProcessCode } from "../pages/nodeDesigner/constants";
import { newDesignerState } from "../pages/nodeDesigner/designerState";

const NEW_DRAFT_KEY = DRAFT_KEY_PREFIX + NEW_NODE_DRAFT_KEY;

class MemoryStorage {
  data = new Map<string, string>();
  getItem(key: string): string | null {
    return this.data.has(key) ? (this.data.get(key) as string) : null;
  }
  setItem(key: string, value: string): void {
    this.data.set(key, value);
  }
  removeItem(key: string): void {
    this.data.delete(key);
  }
}

let localStorageMock: MemoryStorage;
let sessionStorageMock: MemoryStorage;

const g = globalThis as { localStorage?: unknown; sessionStorage?: unknown };

beforeEach(() => {
  setActivePinia(createPinia());
  vi.useFakeTimers();
  localStorageMock = new MemoryStorage();
  sessionStorageMock = new MemoryStorage();
  g.localStorage = localStorageMock;
  g.sessionStorage = sessionStorageMock;
});

afterEach(() => {
  vi.useRealTimers();
  delete g.localStorage;
  delete g.sessionStorage;
});

const flushDraft = async () => {
  await nextTick();
  vi.advanceTimersByTime(DRAFT_DEBOUNCE_MS);
};

const v2Draft = (state: ReturnType<typeof newDesignerState>) =>
  JSON.stringify({ version: DRAFT_VERSION, codeOnly: false, codeText: "", designerState: state });

describe("state transitions", () => {
  it("starts with a seeded Settings section and adds more", () => {
    const store = useNodeDesignerStore();
    expect(store.sections).toHaveLength(1);
    expect(store.sections[0].name).toBe("settings");
    store.addSection();
    expect(store.sections).toHaveLength(2);
    expect(store.sections[1].name).toBe("section_2");
    expect(store.selectedSectionIndex).toBe(1);
    expect(store.selectedComponentIndex).toBeNull();
  });

  it("ensureSection recreates a section when all were removed", () => {
    const store = useNodeDesignerStore();
    store.removeSection(0);
    expect(store.sections).toHaveLength(0);
    const index = store.ensureSection();
    expect(index).toBe(0);
    expect(store.sections[0].name).toBe("settings");
    expect(store.ensureSection()).toBe(0);
    expect(store.sections).toHaveLength(1);
  });

  it("addComponent builds a default control and selects it", () => {
    const store = useNodeDesignerStore();
    store.addComponent(0, "TextInput");
    expect(store.sections[0].components).toHaveLength(1);
    expect(store.sections[0].components[0].component_type).toBe("TextInput");
    expect(store.selectedComponent?.name).toBe("text_input_1");
  });

  it("addComponentToSection accepts a prebuilt ComponentState", () => {
    const store = useNodeDesignerStore();
    store.addComponentToSection(0, {
      component_type: "TextInput",
      name: "greeting",
      label: "Greeting",
      default: "",
      placeholder: null,
    });
    expect(store.selectedComponent?.name).toBe("greeting");
  });

  it("clears selection when the selected component is removed", () => {
    const store = useNodeDesignerStore();
    store.addComponent(0, "TextInput");
    store.removeComponent(0, 0);
    expect(store.selectedComponent).toBeNull();
    expect(store.sections[0].components).toHaveLength(0);
  });

  it("moveComponent reorders within a section", () => {
    const store = useNodeDesignerStore();
    store.addComponent(0, "TextInput");
    store.addComponent(0, "NumericInput");
    const firstName = store.sections[0].components[0].name;
    store.moveComponent(0, 0, 1);
    expect(store.sections[0].components[1].name).toBe(firstName);
  });

  it("moveSection reorders groups and refuses out-of-range targets", () => {
    const store = useNodeDesignerStore();
    store.addSection();
    store.addSection();
    expect(store.sections.map((s) => s.name)).toEqual(["settings", "section_2", "section_3"]);

    store.moveSection(2, 0);
    expect(store.sections.map((s) => s.name)).toEqual(["section_3", "settings", "section_2"]);

    store.moveSection(0, 2);
    expect(store.sections.map((s) => s.name)).toEqual(["settings", "section_2", "section_3"]);

    store.moveSection(0, -1);
    store.moveSection(0, 3);
    store.moveSection(1, 1);
    expect(store.sections.map((s) => s.name)).toEqual(["settings", "section_2", "section_3"]);
  });

  it("moveSection keeps the selection pointing at the same group", () => {
    const store = useNodeDesignerStore();
    store.addSection();
    store.addSection();

    // Selection follows the group that moved.
    store.selectSection(0);
    store.moveSection(0, 2);
    expect(store.selectedSectionIndex).toBe(2);
    expect(store.sections[store.selectedSectionIndex as number].name).toBe("settings");

    // ...and survives a different group moving past it.
    store.selectSection(0);
    const selectedName = store.sections[0].name;
    store.moveSection(2, 0);
    expect(store.sections[store.selectedSectionIndex as number].name).toBe(selectedName);
  });

  it("moveComponentToSection moves a control across groups and selects it there", () => {
    const store = useNodeDesignerStore();
    store.addSection();
    store.addComponent(0, "TextInput");
    const moved = store.sections[0].components[0].name;

    store.moveComponentToSection(0, 0, 1, 0);

    expect(store.sections[0].components).toHaveLength(0);
    expect(store.sections[1].components.map((c) => c.name)).toEqual([moved]);
    expect(store.selectedSectionIndex).toBe(1);
    expect(store.selectedComponentIndex).toBe(0);
  });

  it("moveComponentToSection renames on collision instead of shadowing", () => {
    const store = useNodeDesignerStore();
    store.addSection();
    store.addComponent(0, "TextInput");
    store.addComponent(1, "TextInput");
    // Both groups seeded the same default name, so the move must rename.
    expect(store.sections[0].components[0].name).toBe("text_input_1");
    expect(store.sections[1].components[0].name).toBe("text_input_1");

    store.moveComponentToSection(0, 0, 1, 1);

    expect(store.sections[1].components.map((c) => c.name)).toEqual([
      "text_input_1",
      "text_input_1_2",
    ]);
  });

  it("moveComponentToSection retargets visible_when across the move and the rename", () => {
    const store = useNodeDesignerStore();
    store.addSection();
    store.addComponent(0, "ToggleSwitch");
    store.setSectionVisibleWhen(1, { field: "settings.toggle_switch_1", equals: true });

    store.moveComponentToSection(0, 0, 1, 0);

    expect(store.sections[1].visible_when?.field).toBe("section_2.toggle_switch_1");
    expect(store.sections[1].visible_when?.equals).toBe(true);
  });

  it("moveComponentToSection carries the preview value to the new key", () => {
    const store = useNodeDesignerStore();
    store.addSection();
    store.addComponent(0, "TextInput");
    store.previewValues.settings.text_input_1 = "typed by the user";

    store.moveComponentToSection(0, 0, 1, 0);

    expect(store.previewValues.section_2.text_input_1).toBe("typed by the user");
    expect(store.previewValues.settings.text_input_1).toBeUndefined();
  });

  it("moveComponentToSection within one group falls back to moveComponent", () => {
    const store = useNodeDesignerStore();
    store.addComponent(0, "TextInput");
    store.addComponent(0, "NumericInput");
    const first = store.sections[0].components[0].name;

    store.moveComponentToSection(0, 0, 0, 1);

    expect(store.sections[0].components[1].name).toBe(first);
  });

  it("moveComponentToSection lands a control at the end when dropped on its own header", () => {
    const store = useNodeDesignerStore();
    store.addComponent(0, "TextInput");
    store.addComponent(0, "NumericInput");
    store.addComponent(0, "ToggleSwitch");
    const first = store.sections[0].components[0].name;

    // Header drops pass toIndex = components.length, one past the last valid index.
    store.moveComponentToSection(0, 0, 0, store.sections[0].components.length);

    expect(store.sections[0].components.map((c) => c.name).at(-1)).toBe(first);
  });

  it("sanitizes section names to python identifiers", () => {
    const store = useNodeDesignerStore();
    store.addSection();
    store.sections[1].name = "1 My-Section!";
    store.sanitizeSectionName(1);
    expect(store.sections[1].name).toBe("_1_my_section");
  });

  it("seeds sections with a vertical layout and toggles description/layout", () => {
    const store = useNodeDesignerStore();
    expect(store.sections[0].layout).toBe("vertical");
    store.setSectionLayout(0, "horizontal");
    expect(store.sections[0].layout).toBe("horizontal");
    store.updateSectionDescription(0, "Row of controls");
    expect(store.sections[0].description).toBe("Row of controls");
    store.updateSectionDescription(0, "");
    expect(store.sections[0].description).toBeNull();
    expect(store.frontendSchema.settings.layout).toBe("horizontal");
  });

  it("sets and clears a section visible_when rule and reflects it in the schema", () => {
    const store = useNodeDesignerStore();
    expect(store.sections[0].visible_when).toBeUndefined();
    store.setSectionVisibleWhen(0, { field: "opts.show_advanced", equals: true });
    expect(store.sections[0].visible_when).toEqual({ field: "opts.show_advanced", equals: true });
    expect(store.frontendSchema.settings.visible_when).toEqual({
      field: "opts.show_advanced",
      equals: true,
    });
    store.setSectionVisibleWhen(0, null);
    expect(store.sections[0].visible_when).toBeNull();
    expect(store.frontendSchema.settings.visible_when).toBeUndefined();
  });

  it("retargets visible_when references when a section is renamed", () => {
    const store = useNodeDesignerStore();
    store.addComponent(0, "ToggleSwitch");
    store.addSection();
    store.setSectionVisibleWhen(1, { field: "settings.toggle_switch_1", equals: true });
    store.retargetVisibleWhenForSection("settings", "main");
    expect(store.sections[1].visible_when?.field).toBe("main.toggle_switch_1");
  });

  it("retargets visible_when references when a toggle is renamed", () => {
    const store = useNodeDesignerStore();
    store.addComponent(0, "ToggleSwitch");
    store.addSection();
    store.setSectionVisibleWhen(1, { field: "settings.toggle_switch_1", equals: false });
    store.retargetVisibleWhenForComponent("settings", "toggle_switch_1", "flag");
    expect(store.sections[1].visible_when).toEqual({ field: "settings.flag", equals: false });
  });

  it("leaves unrelated visible_when references untouched on rename", () => {
    const store = useNodeDesignerStore();
    store.addSection();
    store.setSectionVisibleWhen(1, { field: "other.toggle", equals: true });
    store.retargetVisibleWhenForSection("settings", "main");
    store.retargetVisibleWhenForComponent("settings", "x", "y");
    expect(store.sections[1].visible_when?.field).toBe("other.toggle");
  });

  it("resetState restores the defaults", () => {
    const store = useNodeDesignerStore();
    store.nodeMetadata.node_name = "Something";
    store.addComponent(0, "TextInput");
    store.processCode = "def process(self): pass";
    store.resetState();
    expect(store.nodeMetadata.node_name).toBe("");
    expect(store.sections).toHaveLength(1);
    expect(store.sections[0].name).toBe("settings");
    expect(store.sections[0].components).toHaveLength(0);
    expect(store.selectedSectionIndex).toBeNull();
  });

  it("canSave requires a name and category", () => {
    const store = useNodeDesignerStore();
    expect(store.canSave).toBe(false);
    store.nodeMetadata.node_name = "My Node";
    expect(store.canSave).toBe(true);
    store.nodeMetadata.node_category = "  ";
    expect(store.canSave).toBe(false);
  });

  it("nodeMetadata.node_name drives the class names", () => {
    const store = useNodeDesignerStore();
    store.nodeMetadata.node_name = "My Cool Node";
    expect(store.designerState.class_name).toBe("MyCoolNode");
    expect(store.designerState.settings_class_name).toBe("MyCoolNodeSettings");
  });

  it("requires_kernel accessor maps onto environment.kind", () => {
    const store = useNodeDesignerStore();
    expect(store.environment.kind).toBe("local");
    store.nodeMetadata.requires_kernel = true;
    expect(store.environment.kind).toBe("kernel");
    store.nodeMetadata.requires_kernel = false;
    expect(store.environment.kind).toBe("local");
  });

  it("frontendSchema mirrors sections and controls", () => {
    const store = useNodeDesignerStore();
    store.addComponent(0, "ToggleSwitch");
    const schema = store.frontendSchema;
    expect(schema.settings.component_type).toBe("Section");
    const compKey = Object.keys(schema.settings.components)[0];
    expect(schema.settings.components[compKey].component_type).toBe("ToggleSwitch");
  });
});

describe("isDirty", () => {
  it("is clean on a fresh store and dirty after a change", () => {
    const store = useNodeDesignerStore();
    expect(store.isDirty).toBe(false);
    store.nodeMetadata.node_name = "My Node";
    expect(store.isDirty).toBe(true);
  });

  it("markSaved snapshots the current state", () => {
    const store = useNodeDesignerStore();
    store.nodeMetadata.node_name = "My Node";
    store.markSaved();
    expect(store.isDirty).toBe(false);
    store.processCode = "changed";
    expect(store.isDirty).toBe(true);
  });

  it("newNode resets to a clean slate", () => {
    const store = useNodeDesignerStore();
    store.nodeMetadata.node_name = "My Node";
    store.newNode();
    expect(store.isDirty).toBe(false);
    expect(store.nodeMetadata.node_name).toBe("");
    expect(store.sourceFile).toBeNull();
  });

  it("does not leak output_names mutations into the defaults", () => {
    const store = useNodeDesignerStore();
    store.nodeMetadata.output_names.push("extra");
    store.newNode();
    expect(store.nodeMetadata.output_names).toEqual(["main"]);
    expect(store.isDirty).toBe(false);
  });
});

describe("draft persistence", () => {
  it("writes a debounced v2 draft under the __new__ key when dirty", async () => {
    const store = useNodeDesignerStore();
    store.nodeMetadata.node_name = "Draft Node";
    await nextTick();
    expect(localStorageMock.getItem(NEW_DRAFT_KEY)).toBeNull();
    vi.advanceTimersByTime(DRAFT_DEBOUNCE_MS - 100);
    expect(localStorageMock.getItem(NEW_DRAFT_KEY)).toBeNull();
    vi.advanceTimersByTime(100);
    const raw = localStorageMock.getItem(NEW_DRAFT_KEY);
    expect(raw).not.toBeNull();
    const parsed = JSON.parse(raw as string);
    expect(parsed.version).toBe(DRAFT_VERSION);
    expect(parsed.designerState.node_name).toBe("Draft Node");
  });

  it("keys drafts by source file when one is set", async () => {
    const store = useNodeDesignerStore();
    store.sourceFile = "my_node.py";
    store.nodeMetadata.node_name = "Named";
    await flushDraft();
    expect(localStorageMock.getItem(DRAFT_KEY_PREFIX + "my_node.py")).not.toBeNull();
    expect(localStorageMock.getItem(NEW_DRAFT_KEY)).toBeNull();
  });

  it("removes the draft again once state matches the saved snapshot", async () => {
    const store = useNodeDesignerStore();
    store.nodeMetadata.node_name = "Draft Node";
    await flushDraft();
    expect(localStorageMock.getItem(NEW_DRAFT_KEY)).not.toBeNull();
    store.markSaved();
    expect(localStorageMock.getItem(NEW_DRAFT_KEY)).toBeNull();
    await flushDraft();
    expect(localStorageMock.getItem(NEW_DRAFT_KEY)).toBeNull();
  });
});

describe("draft restore", () => {
  const seedDraft = (name: string) => {
    const state = newDesignerState();
    state.node_name = name;
    state.sections = [
      {
        name: "opts",
        title: "Options",
        description: null,
        hidden: false,
        layout: "vertical",
        components: [],
      },
    ];
    state.process_code = "return inputs[0]";
    localStorageMock.setItem(NEW_DRAFT_KEY, v2Draft(state));
  };

  it("flags a differing draft on initialize and restores it on demand", () => {
    seedDraft("Recovered");
    const store = useNodeDesignerStore();
    store.initialize();
    expect(store.restoreAvailable).toBe(true);
    expect(store.nodeMetadata.node_name).toBe("");

    store.restore();
    expect(store.restoreAvailable).toBe(false);
    expect(store.nodeMetadata.node_name).toBe("Recovered");
    expect(store.sections).toHaveLength(1);
    expect(store.sections[0].name).toBe("opts");
    expect(store.isDirty).toBe(true);
  });

  it("discardDraft removes the stored draft and hides the banner", () => {
    seedDraft("Recovered");
    const store = useNodeDesignerStore();
    store.initialize();
    expect(store.restoreAvailable).toBe(true);

    store.discardDraft();
    expect(store.restoreAvailable).toBe(false);
    expect(localStorageMock.getItem(NEW_DRAFT_KEY)).toBeNull();
    expect(store.nodeMetadata.node_name).toBe("");
  });

  it("does not offer restore when the draft equals the current state", () => {
    const store = useNodeDesignerStore();
    store.nodeMetadata.node_name = "Same";
    localStorageMock.setItem(NEW_DRAFT_KEY, v2Draft(store.designerState));
    store.initialize();
    expect(store.restoreAvailable).toBe(false);
  });

  it("drops an unparseable draft", () => {
    localStorageMock.setItem(NEW_DRAFT_KEY, "{not json");
    const store = useNodeDesignerStore();
    store.initialize();
    expect(store.restoreAvailable).toBe(false);
    expect(localStorageMock.getItem(NEW_DRAFT_KEY)).toBeNull();
  });

  it("migrates a legacy v1-shape draft into DesignerState", () => {
    localStorageMock.setItem(
      NEW_DRAFT_KEY,
      JSON.stringify({
        nodeMetadata: { node_name: "Legacy", node_category: "Custom", output_names: ["main"] },
        sections: [
          {
            name: "opts",
            title: "Options",
            components: [
              { component_type: "TextInput", field_name: "greeting", label: "Greeting" },
            ],
          },
        ],
        processCode: "return inputs[0]",
      }),
    );
    const store = useNodeDesignerStore();
    store.initialize();
    expect(store.restoreAvailable).toBe(true);
    store.restore();
    expect(store.nodeMetadata.node_name).toBe("Legacy");
    expect(store.sections[0].components[0].name).toBe("greeting");
    expect(store.sections[0].components[0].component_type).toBe("TextInput");
  });
});

describe("legacy sessionStorage migration", () => {
  const legacyPayload = JSON.stringify({
    nodeMetadata: { node_name: "Legacy" },
    sections: [],
    processCode: "def process(self): pass",
  });

  it("moves the old sessionStorage state into the __new__ draft once", () => {
    sessionStorageMock.setItem(STORAGE_KEY, legacyPayload);
    const store = useNodeDesignerStore();
    store.initialize();

    expect(sessionStorageMock.getItem(STORAGE_KEY)).toBeNull();
    expect(localStorageMock.getItem(NEW_DRAFT_KEY)).toBe(legacyPayload);
    // migrated payload is a v1 draft -> restore offered
    expect(store.restoreAvailable).toBe(true);
  });

  it("does not overwrite an existing localStorage draft", () => {
    sessionStorageMock.setItem(STORAGE_KEY, legacyPayload);
    const existing = v2Draft(newDesignerState());
    localStorageMock.setItem(NEW_DRAFT_KEY, existing);
    const store = useNodeDesignerStore();
    store.initialize();

    expect(sessionStorageMock.getItem(STORAGE_KEY)).toBeNull();
    expect(localStorageMock.getItem(NEW_DRAFT_KEY)).toBe(existing);
  });
});

describe("code-only mode", () => {
  it("routes processCode through the full-file text buffer", () => {
    const store = useNodeDesignerStore();
    store.loadCodeOnly("# whole file\nclass X: ...", [
      { code: "OUT_OF_SUBSET", message: "nope", severity: "error" },
    ]);
    expect(store.codeOnly).toBe(true);
    expect(store.processCode).toBe("# whole file\nclass X: ...");
    expect(store.parseIssues).toHaveLength(1);
    store.processCode = "# edited";
    expect(store.codeText).toBe("# edited");
  });
});

describe("without storage available", () => {
  it("initialize and mutations do not throw", async () => {
    delete g.localStorage;
    delete g.sessionStorage;
    const store = useNodeDesignerStore();
    expect(() => store.initialize()).not.toThrow();
    store.nodeMetadata.node_name = "No storage";
    await flushDraft();
    expect(store.isDirty).toBe(true);
  });
});

describe("multi-output signature & scaffold", () => {
  const SINGLE_SIG = "def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:";
  const MULTI_SIG = "def process(self, *inputs: pl.LazyFrame) -> dict[str, pl.LazyFrame]:";

  it("derives the return type live from the output count", () => {
    const store = useNodeDesignerStore();
    expect(store.processSignature).toBe(SINGLE_SIG);
    store.nodeMetadata.number_of_outputs = 2;
    expect(store.processSignature).toBe(MULTI_SIG);
    store.nodeMetadata.number_of_outputs = 1;
    expect(store.processSignature).toBe(SINGLE_SIG);
  });

  it("scaffolds a dict body keyed by output_names when pristine", async () => {
    const store = useNodeDesignerStore();
    store.nodeMetadata.number_of_outputs = 2;
    await nextTick();
    const code = store.designerState.process_code;
    expect(code).toContain(MULTI_SIG);
    expect(code).toContain("return {");
    expect(code).toContain('"main": ...,  # replace ... with the output');
    expect(code).toContain('"output_1": ...,  # replace ... with the output');
  });

  it("recomposes an edited body with the derived signature", async () => {
    const store = useNodeDesignerStore();
    store.nodeMetadata.number_of_outputs = 2;
    await nextTick();
    store.processBody = '    return {"main": inputs[0], "output_1": inputs[0]}';
    expect(store.designerState.process_code.split("\n")[0]).toBe(MULTI_SIG);
  });

  it("never clobbers a body the user has edited", async () => {
    const store = useNodeDesignerStore();
    store.processBody = "    return inputs[0].head(5)";
    store.nodeMetadata.number_of_outputs = 2;
    await nextTick();
    const code = store.designerState.process_code;
    expect(code).toContain("head(5)");
    expect(code).toContain(MULTI_SIG);
    expect(code).not.toContain("replace ... with the output");
  });

  it("keeps output_names length in sync with number_of_outputs", async () => {
    const store = useNodeDesignerStore();
    store.nodeMetadata.number_of_outputs = 3;
    await nextTick();
    expect(store.nodeMetadata.output_names).toEqual(["main", "output_1", "output_2"]);
    store.nodeMetadata.number_of_outputs = 1;
    await nextTick();
    expect(store.nodeMetadata.output_names).toEqual(["main"]);
  });

  it("normalizes a stale signature on load without opening dirty", async () => {
    const state = newDesignerState();
    state.node_name = "Multi";
    state.node_category = "Custom";
    state.number_of_outputs = 2;
    state.output_names = ["main", "second"];
    state.process_code = `${SINGLE_SIG}\n    return {"main": inputs[0], "second": inputs[0]}`;
    const store = useNodeDesignerStore();
    store.loadDesignerState(state);
    store.markSaved();
    expect(store.designerState.process_code).toContain(MULTI_SIG);
    expect(store.isDirty).toBe(false);
    await nextTick();
    expect(store.isDirty).toBe(false);
  });

  it("leaves process_code untouched in code-only mode", async () => {
    const store = useNodeDesignerStore();
    store.loadCodeOnly("def process(self, *inputs):\n    return inputs[0]");
    const before = store.designerState.process_code;
    store.nodeMetadata.number_of_outputs = 2;
    await nextTick();
    expect(store.codeOnly).toBe(true);
    expect(store.designerState.process_code).toBe(before);
  });
});

describe("predict_output_schema hook editor", () => {
  const SINGLE_SIG =
    "def predict_output_schema(self, *inputs: pl.LazyFrame) -> pl.LazyFrame | None:";
  const MULTI_SIG =
    "def predict_output_schema(self, *inputs: pl.LazyFrame) -> dict[str, pl.LazyFrame] | None:";

  it("is disabled by default and scaffolds on enable", () => {
    const store = useNodeDesignerStore();
    expect(store.predictSchemaEnabled).toBe(false);
    expect(store.designerState.predict_schema_code).toBe("");

    store.predictSchemaEnabled = true;
    const code = store.designerState.predict_schema_code;
    expect(code.split("\n")[0]).toBe(SINGLE_SIG);
    expect(code).toContain("return self.process(*inputs)");
  });

  it("tolerates states and drafts saved before the field existed", () => {
    const state = newDesignerState();
    state.node_name = "Legacy";
    delete (state as Partial<typeof state>).predict_schema_code;
    const store = useNodeDesignerStore();
    store.loadDesignerState(state);
    expect(store.predictSchemaEnabled).toBe(false);
    expect(store.predictSchemaBody).toBe("");
    expect(store.designerState.predict_schema_code).toBe("");
  });

  it("clears the stored def on disable and on an emptied body", () => {
    const store = useNodeDesignerStore();
    store.predictSchemaEnabled = true;
    store.predictSchemaEnabled = false;
    expect(store.designerState.predict_schema_code).toBe("");

    store.predictSchemaEnabled = true;
    store.predictSchemaBody = "   ";
    expect(store.designerState.predict_schema_code).toBe("");
    expect(store.predictSchemaEnabled).toBe(false);
  });

  it("recomposes an edited body under the derived signature", () => {
    const store = useNodeDesignerStore();
    store.predictSchemaBody = "    return None";
    const code = store.designerState.predict_schema_code;
    expect(code.split("\n")[0]).toBe(SINGLE_SIG);
    expect(code).toContain("return None");
    expect(store.predictSchemaBody).toBe("    return None");
  });

  it("re-derives the signature when the output count changes", async () => {
    const store = useNodeDesignerStore();
    store.predictSchemaEnabled = true;
    store.nodeMetadata.number_of_outputs = 2;
    await nextTick();
    expect(store.predictSchemaSignature).toBe(MULTI_SIG);
    expect(store.designerState.predict_schema_code.split("\n")[0]).toBe(MULTI_SIG);
    expect(store.designerState.predict_schema_code).toContain("return self.process(*inputs)");
  });

  it("round-trips a loaded state's hook body", () => {
    const state = newDesignerState();
    state.node_name = "Hooked";
    state.predict_schema_code = `${SINGLE_SIG}\n    return pl.Schema({"a": pl.Int64()})`;
    const store = useNodeDesignerStore();
    store.loadDesignerState(state);
    expect(store.predictSchemaEnabled).toBe(true);
    expect(store.predictSchemaBody).toBe('    return pl.Schema({"a": pl.Int64()})');
  });
});

describe("requires_data_for_prediction flag", () => {
  it("defaults to false on a fresh state", () => {
    expect(newDesignerState().requires_data_for_prediction).toBe(false);
    const store = useNodeDesignerStore();
    expect(store.requiresDataForPrediction).toBe(false);
  });

  it("tolerates states and drafts saved before the field existed", () => {
    const state = newDesignerState();
    state.node_name = "Legacy";
    delete (state as Partial<typeof state>).requires_data_for_prediction;
    const store = useNodeDesignerStore();
    store.loadDesignerState(state);
    store.markSaved();
    expect(store.requiresDataForPrediction).toBe(false);
    expect(store.isDirty).toBe(false);
  });

  it("restores a pre-field draft with the flag defaulted to false", () => {
    const state = newDesignerState();
    state.node_name = "Recovered";
    delete (state as Partial<typeof state>).requires_data_for_prediction;
    localStorageMock.setItem(NEW_DRAFT_KEY, v2Draft(state));
    const store = useNodeDesignerStore();
    store.initialize();
    store.restore();
    expect(store.requiresDataForPrediction).toBe(false);
  });

  it("predictionRequiresRun is only on for flag-true hookless states", () => {
    const store = useNodeDesignerStore();
    expect(store.predictionRequiresRun).toBe(false);
    store.requiresDataForPrediction = true;
    expect(store.predictionRequiresRun).toBe(true);
    store.predictSchemaEnabled = true;
    expect(store.predictionRequiresRun).toBe(false);
    store.requiresDataForPrediction = false;
    store.predictSchemaEnabled = false;
    expect(store.predictionRequiresRun).toBe(false);
  });

  it("flipping the flag marks the state dirty and survives a draft round-trip", async () => {
    const store = useNodeDesignerStore();
    store.nodeMetadata.node_name = "Flagged";
    store.markSaved();
    store.requiresDataForPrediction = true;
    expect(store.isDirty).toBe(true);
    await flushDraft();
    const raw = localStorageMock.getItem(NEW_DRAFT_KEY);
    expect(raw).not.toBeNull();
    expect(JSON.parse(raw as string).designerState.requires_data_for_prediction).toBe(true);
  });
});

export { defaultProcessCode };
