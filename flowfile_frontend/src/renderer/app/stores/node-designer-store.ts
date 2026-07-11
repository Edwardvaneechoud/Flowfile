// Node Designer state, migrated to DesignerState as the internal model.
// sections are SectionState[]; the public action surface (addSection,
// addComponentToSection, ...) is preserved. nodeMetadata stays as a live
// accessor view over DesignerState identity/io fields so existing panels keep
// their v-model bindings. Per-node localStorage drafts serialize the full
// DesignerState (draft version bumped — old-shape drafts are migrated once, then
// discarded if incompatible).
import { defineStore } from "pinia";
import { computed, reactive, ref, watch } from "vue";
import { STORAGE_KEY, defaultProcessCode } from "../pages/nodeDesigner/constants";
import type {
  ComponentState,
  ComponentType,
  DesignerState,
  EnvironmentState,
  ParseIssue,
  SectionLayout,
  SectionState,
} from "../pages/nodeDesigner/designerState";
import { newDesignerState } from "../pages/nodeDesigner/designerState";
import {
  defaultComponentState,
  designerStateToFrontendSchema,
  legacyDraftToDesignerState,
  toPascalCase,
  toSnakeCase,
} from "../pages/nodeDesigner/mappers";
import type { DryRunResponse } from "../api/nodeDesigner";

export const DRAFT_KEY_PREFIX = "nodeDesigner.draft.";
export const NEW_NODE_DRAFT_KEY = "__new__";
export const DRAFT_DEBOUNCE_MS = 500;
export const DRAFT_VERSION = 2;

interface StorageLike {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
}

const resolveLocalStorage = (): StorageLike | null => {
  try {
    return (globalThis as { localStorage?: StorageLike }).localStorage ?? null;
  } catch {
    return null;
  }
};

const resolveSessionStorage = (): StorageLike | null => {
  try {
    return (globalThis as { sessionStorage?: StorageLike }).sessionStorage ?? null;
  } catch {
    return null;
  }
};

const seededState = (): DesignerState => {
  const state = newDesignerState();
  state.process_code = defaultProcessCode;
  return state;
};

export const useNodeDesignerStore = defineStore("node-designer", () => {
  const designerState = ref<DesignerState>(seededState());
  const codeOnly = ref(false);
  // Full-file text in code-only mode; falls back to process_code otherwise.
  const codeText = ref("");
  const parseIssues = ref<ParseIssue[]>([]);
  const sourceFile = ref<string | null>(null);
  const fileHash = ref<string | null>(null);

  const selectedSectionIndex = ref<number | null>(null);
  const selectedComponentIndex = ref<number | null>(null);

  // Live values held by the interactive Form-tab preview; double as dry-run settings.
  const previewValues = ref<Record<string, Record<string, unknown>>>({});
  const dryRun = ref<{ running: boolean; result: DryRunResponse | null; error: string | null }>({
    running: false,
    result: null,
    error: null,
  });

  const restoreAvailable = ref(false);
  let pendingDraft: DesignerState | null = null;

  // --- nodeMetadata accessor view over DesignerState (keeps v-model bindings working) ---
  const nodeMetadata = reactive({
    get node_name() {
      return designerState.value.node_name;
    },
    set node_name(v: string) {
      designerState.value.node_name = v;
      designerState.value.class_name = toPascalCase(v || "MyCustomNode") || "MyCustomNode";
      designerState.value.settings_class_name = `${designerState.value.class_name}Settings`;
    },
    get node_category() {
      return designerState.value.node_category;
    },
    set node_category(v: string) {
      designerState.value.node_category = v;
    },
    get title() {
      return designerState.value.title;
    },
    set title(v: string) {
      designerState.value.title = v;
    },
    get intro() {
      return designerState.value.intro;
    },
    set intro(v: string) {
      designerState.value.intro = v;
    },
    get author() {
      return designerState.value.author;
    },
    set author(v: string) {
      designerState.value.author = v;
    },
    get version() {
      return designerState.value.version;
    },
    set version(v: string) {
      designerState.value.version = v;
    },
    get tags() {
      return designerState.value.tags;
    },
    set tags(v: string[]) {
      designerState.value.tags = v;
    },
    get number_of_inputs() {
      return designerState.value.number_of_inputs;
    },
    set number_of_inputs(v: number) {
      designerState.value.number_of_inputs = v;
    },
    get number_of_outputs() {
      return designerState.value.number_of_outputs;
    },
    set number_of_outputs(v: number) {
      designerState.value.number_of_outputs = v;
    },
    get node_icon() {
      return designerState.value.node_icon;
    },
    set node_icon(v: string) {
      designerState.value.node_icon = v;
    },
    get output_names() {
      return designerState.value.output_names;
    },
    set output_names(v: string[]) {
      designerState.value.output_names = v;
    },
    // Legacy compat: MetadataPanel/others toggle these; map onto environment.
    get requires_kernel() {
      return designerState.value.environment.kind === "kernel";
    },
    set requires_kernel(v: boolean) {
      designerState.value.environment.kind = v ? "kernel" : "local";
    },
    get kernel_id() {
      return designerState.value.environment.default_kernel_id ?? null;
    },
    set kernel_id(v: string | null) {
      designerState.value.environment.default_kernel_id = v;
    },
  });

  // sections stays a direct reference to designerState.sections so components that
  // do store.sections[i].title = ... keep mutating the source of truth.
  const sections = computed({
    get: () => designerState.value.sections,
    set: (v: SectionState[]) => {
      designerState.value.sections = v;
    },
  });

  const environment = computed<EnvironmentState>({
    get: () => designerState.value.environment,
    set: (v: EnvironmentState) => {
      designerState.value.environment = v;
    },
  });

  // processCode: in code-only mode this is the whole file; otherwise the process def.
  const processCode = computed({
    get: () => (codeOnly.value ? codeText.value : designerState.value.process_code),
    set: (v: string) => {
      if (codeOnly.value) codeText.value = v;
      else designerState.value.process_code = v;
    },
  });

  const DEFAULT_PROCESS_SIGNATURE = "def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:";

  // The stored process_code is the FULL def block (backend contract). The Code
  // tab edits only the body under a read-only signature header, so users never
  // have to type (or accidentally delete) the def line themselves.
  const splitProcessCode = (code: string): { signature: string; body: string } => {
    const lines = code.split("\n");
    const defIndex = lines.findIndex((line) => line.trimStart().startsWith("def process"));
    if (defIndex === -1) return { signature: DEFAULT_PROCESS_SIGNATURE, body: code };
    let end = defIndex;
    while (end < lines.length && !lines[end].trimEnd().endsWith(":")) end += 1;
    if (end >= lines.length) return { signature: DEFAULT_PROCESS_SIGNATURE, body: code };
    return {
      signature: lines.slice(0, end + 1).join("\n"),
      body: lines.slice(end + 1).join("\n"),
    };
  };

  // The process() return type depends on the output count: a single frame for one
  // output, a dict keyed by output name for many. Derive the signature from
  // number_of_outputs so the read-only header is always correct and live, and so
  // the saved .py (which ships process_code verbatim) never carries a stale hint.
  const RETURN_SINGLE = "pl.LazyFrame";
  const RETURN_MULTI = "dict[str, pl.LazyFrame]";
  const derivedSignature = () =>
    `def process(self, *inputs: pl.LazyFrame) -> ${
      designerState.value.number_of_outputs > 1 ? RETURN_MULTI : RETURN_SINGLE
    }:`;

  const normalizeProcessSignature = () => {
    const { body } = splitProcessCode(designerState.value.process_code);
    designerState.value.process_code = `${derivedSignature()}\n${body}`;
  };

  // Keep output_names length matched to number_of_outputs (the spinner sets the
  // count directly; the name editor only appends/removes). Preserves existing
  // names; appended names follow MetadataPanel's `output_${index}` scheme.
  const reconcileOutputNames = (n: number) => {
    if (n < 1) return;
    const names = designerState.value.output_names;
    if (names.length > n) {
      designerState.value.output_names = names.slice(0, n);
    } else if (names.length < n) {
      const next = names.slice();
      while (next.length < n) next.push(`output_${next.length}`);
      designerState.value.output_names = next;
    }
  };

  const singleOutputBody = () => splitProcessCode(defaultProcessCode).body;
  const knownPristineBodies = () => [splitProcessCode(defaultProcessCode).body.trim()];
  const multiOutputEntry = (name: string) =>
    `        ${JSON.stringify(name)}: ...,  # replace ... with the output`;
  const multiOutputBody = (names: string[]) =>
    ["    return {", ...names.map(multiOutputEntry), "    }"].join("\n");

  const processSignature = computed(() => derivedSignature());

  const processBody = computed({
    get: () => splitProcessCode(designerState.value.process_code).body,
    set: (v: string) => {
      designerState.value.process_code = `${derivedSignature()}\n${v}`;
    },
  });

  // Coherence: when the output count changes, resize output_names to match and
  // refresh the stored signature line (covers an edited body that won't be
  // rescaffolded below). Created before the scaffold watcher so names are settled
  // before it reads them.
  watch(
    () => designerState.value.number_of_outputs,
    (n) => {
      if (codeOnly.value) return;
      reconcileOutputNames(n);
      normalizeProcessSignature();
    },
  );

  // While the body is still a known template/scaffold, keep it in step with the
  // declared outputs: a runnable dict keyed by output_names for multi-output, or
  // the single-output default otherwise. A body the user has edited is left alone.
  let lastScaffoldBody = "";
  watch(
    () =>
      [designerState.value.number_of_outputs, designerState.value.output_names.slice()] as const,
    ([count]) => {
      if (codeOnly.value) return;
      const body = processBody.value.trim();
      const pristine =
        body === "" || knownPristineBodies().includes(body) || body === lastScaffoldBody.trim();
      if (!pristine) return;
      if (count > 1) {
        const scaffold = multiOutputBody(designerState.value.output_names);
        lastScaffoldBody = scaffold;
        processBody.value = scaffold;
      } else {
        lastScaffoldBody = "";
        processBody.value = singleOutputBody();
      }
    },
  );

  const frontendSchema = computed(() => designerStateToFrontendSchema(designerState.value));

  const serialize = (): string =>
    JSON.stringify({
      version: DRAFT_VERSION,
      codeOnly: codeOnly.value,
      codeText: codeText.value,
      designerState: designerState.value,
    });

  const savedSnapshot = ref(serialize());
  const isDirty = computed(() => serialize() !== savedSnapshot.value);
  const canSave = computed(
    () =>
      designerState.value.node_name.trim() !== "" &&
      designerState.value.node_category.trim() !== "",
  );

  const selectedComponent = computed<ComponentState | null>(() => {
    if (selectedSectionIndex.value !== null && selectedComponentIndex.value !== null) {
      return (
        sections.value[selectedSectionIndex.value]?.components[selectedComponentIndex.value] ?? null
      );
    }
    return null;
  });

  // --- drafts ---

  const draftKey = () => DRAFT_KEY_PREFIX + (sourceFile.value || NEW_NODE_DRAFT_KEY);

  function writeDraft() {
    const storage = resolveLocalStorage();
    if (!storage) return;
    try {
      if (isDirty.value) storage.setItem(draftKey(), serialize());
      else storage.removeItem(draftKey());
    } catch {
      // quota exceeded / storage disabled — best effort
    }
  }

  let draftTimer: ReturnType<typeof setTimeout> | null = null;
  watch(
    [designerState, codeOnly, codeText, sourceFile],
    () => {
      if (draftTimer) clearTimeout(draftTimer);
      draftTimer = setTimeout(writeDraft, DRAFT_DEBOUNCE_MS);
    },
    { deep: true },
  );

  // Drafts saved before the publishing-metadata fields existed lack these keys;
  // fill defaults so they never leak undefined into save payloads.
  const normalizeDraftState = (state: DesignerState): DesignerState => ({
    ...state,
    author: state.author ?? "",
    version: state.version ?? "",
    tags: state.tags ?? [],
  });

  function parseDraft(raw: string): DesignerState | null {
    let parsed: unknown;
    try {
      parsed = JSON.parse(raw);
    } catch {
      return null;
    }
    const obj = parsed as Record<string, unknown>;
    if (obj && obj.version === DRAFT_VERSION && obj.designerState) {
      return normalizeDraftState(obj.designerState as DesignerState);
    }
    // Legacy v1 draft: { nodeMetadata, sections, processCode }.
    if (obj && obj.nodeMetadata && Array.isArray(obj.sections)) {
      try {
        return legacyDraftToDesignerState(obj as never);
      } catch {
        return null;
      }
    }
    return null;
  }

  function migrateLegacySessionState() {
    const session = resolveSessionStorage();
    const local = resolveLocalStorage();
    if (!session || !local) return;
    try {
      const legacy = session.getItem(STORAGE_KEY);
      if (legacy !== null) {
        const key = DRAFT_KEY_PREFIX + NEW_NODE_DRAFT_KEY;
        if (local.getItem(key) === null) local.setItem(key, legacy);
        session.removeItem(STORAGE_KEY);
      }
    } catch {
      // best effort
    }
  }

  function checkForDraft() {
    restoreAvailable.value = false;
    pendingDraft = null;
    const storage = resolveLocalStorage();
    if (!storage) return;
    let raw: string | null;
    try {
      raw = storage.getItem(draftKey());
    } catch {
      return;
    }
    if (raw === null) return;
    const draftState = parseDraft(raw);
    if (draftState === null) {
      try {
        storage.removeItem(draftKey());
      } catch {
        // best effort
      }
      return;
    }
    if (JSON.stringify(draftState) === JSON.stringify(designerState.value)) return;
    pendingDraft = draftState;
    restoreAvailable.value = true;
  }

  function initialize() {
    migrateLegacySessionState();
    checkForDraft();
  }

  function restore() {
    if (!pendingDraft) return;
    designerState.value = pendingDraft;
    codeOnly.value = false;
    pendingDraft = null;
    restoreAvailable.value = false;
    resetPreviewValues();
  }

  function discardDraft() {
    const storage = resolveLocalStorage();
    try {
      storage?.removeItem(draftKey());
    } catch {
      // best effort
    }
    pendingDraft = null;
    restoreAvailable.value = false;
  }

  // --- state loading / lifecycle ---

  function loadDesignerState(state: DesignerState) {
    designerState.value = state;
    // Fold count/name coherence and the derived signature into the loaded state
    // BEFORE markSaved snapshots it, so a stale-but-valid file never opens dirty.
    reconcileOutputNames(state.number_of_outputs);
    normalizeProcessSignature();
    codeOnly.value = false;
    codeText.value = "";
    resetPreviewValues();
    selectedSectionIndex.value = null;
    selectedComponentIndex.value = null;
  }

  function loadCodeOnly(code: string, issues: ParseIssue[] = []) {
    codeOnly.value = true;
    codeText.value = code;
    parseIssues.value = issues;
    selectedSectionIndex.value = null;
    selectedComponentIndex.value = null;
  }

  function resetPreviewValues() {
    const next: Record<string, Record<string, unknown>> = {};
    for (const section of designerState.value.sections) {
      next[section.name] = {};
      for (const comp of section.components) {
        next[section.name][comp.name] = defaultPreviewValue(comp);
      }
    }
    // Seed from example_settings if present so reopened nodes are reproducible.
    const seed = designerState.value.example_settings;
    if (seed) {
      for (const [sectionKey, values] of Object.entries(seed)) {
        if (!next[sectionKey]) next[sectionKey] = {};
        for (const [field, value] of Object.entries(values)) {
          next[sectionKey][field] = value;
        }
      }
    }
    previewValues.value = next;
  }

  function defaultPreviewValue(comp: ComponentState): unknown {
    switch (comp.component_type) {
      case "TextInput":
        return comp.default ?? "";
      case "NumericInput":
      case "SliderInput":
        return comp.default ?? null;
      case "ToggleSwitch":
        return comp.default;
      case "MultiSelect":
        return comp.default ?? [];
      case "ColumnSelector":
        return comp.multiple ? [] : null;
      case "ColumnActionInput":
        return { rows: [], group_by_columns: [], order_by_column: null };
      case "SingleSelect":
        return comp.default ?? null;
      case "SecretSelector":
      default:
        return null;
    }
  }

  // Deep-merges the current preview values back onto the schema after a section/
  // component is added or removed so untouched values survive edits.
  function syncPreviewValues() {
    const prev = previewValues.value;
    const next: Record<string, Record<string, unknown>> = {};
    for (const section of designerState.value.sections) {
      next[section.name] = {};
      for (const comp of section.components) {
        const existing = prev[section.name]?.[comp.name];
        next[section.name][comp.name] =
          existing !== undefined ? existing : defaultPreviewValue(comp);
      }
    }
    previewValues.value = next;
  }

  function setPreviewValues(values: Record<string, Record<string, unknown>>) {
    previewValues.value = values;
  }

  function resetState() {
    designerState.value = seededState();
    codeOnly.value = false;
    codeText.value = "";
    parseIssues.value = [];
    selectedSectionIndex.value = null;
    selectedComponentIndex.value = null;
    resetPreviewValues();
  }

  function newNode() {
    discardDraft();
    resetState();
    sourceFile.value = null;
    fileHash.value = null;
    savedSnapshot.value = serialize();
    discardDraft();
  }

  function markSaved(fileName?: string, hash?: string | null) {
    discardDraft();
    if (fileName !== undefined) sourceFile.value = fileName;
    if (hash !== undefined) fileHash.value = hash;
    savedSnapshot.value = serialize();
    discardDraft();
  }

  // --- section / component actions (public surface preserved) ---

  function addSection() {
    if (sections.value.length === 0) {
      sections.value.push({
        name: "settings",
        title: "Settings",
        description: null,
        hidden: false,
        layout: "vertical",
        components: [],
      });
    } else {
      let n = sections.value.length + 1;
      const taken = new Set(sections.value.map((s) => s.name));
      while (taken.has(`section_${n}`)) n += 1;
      sections.value.push({
        name: `section_${n}`,
        title: `Section ${n}`,
        description: null,
        hidden: false,
        layout: "vertical",
        components: [],
      });
    }
    syncPreviewValues();
    selectedSectionIndex.value = sections.value.length - 1;
    selectedComponentIndex.value = null;
  }

  function ensureSection(): number {
    if (sections.value.length === 0) addSection();
    return sections.value.length - 1;
  }

  function removeSection(index: number) {
    sections.value.splice(index, 1);
    if (selectedSectionIndex.value === index) {
      selectedSectionIndex.value = null;
      selectedComponentIndex.value = null;
    }
    syncPreviewValues();
  }

  function selectSection(index: number) {
    selectedSectionIndex.value = index;
    selectedComponentIndex.value = null;
  }

  function sanitizeSectionName(index: number) {
    let name = sections.value[index].name;
    name = name.replace(/[\s-]+/g, "_").replace(/[^a-zA-Z0-9_]/g, "");
    if (/^[0-9]/.test(name)) name = "_" + name;
    sections.value[index].name = name.toLowerCase();
    syncPreviewValues();
  }

  function updateSectionTitle(index: number, title: string) {
    sections.value[index].title = title;
  }

  function updateSectionDescription(index: number, description: string) {
    sections.value[index].description = description || null;
  }

  function setSectionLayout(index: number, layout: SectionLayout) {
    sections.value[index].layout = layout;
  }

  function selectComponent(sectionIndex: number, compIndex: number) {
    selectedSectionIndex.value = sectionIndex;
    selectedComponentIndex.value = compIndex;
  }

  function removeComponent(sectionIndex: number, compIndex: number) {
    sections.value[sectionIndex].components.splice(compIndex, 1);
    if (selectedSectionIndex.value === sectionIndex && selectedComponentIndex.value === compIndex) {
      selectedComponentIndex.value = null;
    }
    syncPreviewValues();
  }

  // Legacy signature kept: callers pass an already-built ComponentState.
  function addComponentToSection(sectionIndex: number, component: ComponentState) {
    sections.value[sectionIndex].components.push(component);
    syncPreviewValues();
    selectedSectionIndex.value = sectionIndex;
    selectedComponentIndex.value = sections.value[sectionIndex].components.length - 1;
  }

  // Build a default component of `type` with a unique name, append it, select it.
  function addComponent(sectionIndex: number, type: ComponentType) {
    const section = sections.value[sectionIndex];
    const taken = new Set(section.components.map((c) => c.name));
    const base = toSnakeCase(type);
    let n = section.components.length + 1;
    let name = `${base}_${n}`;
    while (taken.has(name)) {
      n += 1;
      name = `${base}_${n}`;
    }
    addComponentToSection(sectionIndex, defaultComponentState(type, name));
  }

  function moveComponent(sectionIndex: number, from: number, to: number) {
    const comps = sections.value[sectionIndex].components;
    if (to < 0 || to >= comps.length) return;
    const [moved] = comps.splice(from, 1);
    comps.splice(to, 0, moved);
    if (selectedSectionIndex.value === sectionIndex && selectedComponentIndex.value === from) {
      selectedComponentIndex.value = to;
    }
  }

  return {
    // state
    designerState,
    nodeMetadata,
    sections,
    environment,
    processCode,
    processSignature,
    processBody,
    codeOnly,
    codeText,
    parseIssues,
    sourceFile,
    fileHash,
    selectedSectionIndex,
    selectedComponentIndex,
    previewValues,
    dryRun,
    restoreAvailable,
    // getters
    isDirty,
    canSave,
    selectedComponent,
    frontendSchema,
    // lifecycle
    initialize,
    restore,
    discardDraft,
    resetState,
    newNode,
    markSaved,
    loadDesignerState,
    loadCodeOnly,
    resetPreviewValues,
    syncPreviewValues,
    setPreviewValues,
    // section/component actions
    addSection,
    ensureSection,
    removeSection,
    selectSection,
    sanitizeSectionName,
    updateSectionTitle,
    updateSectionDescription,
    setSectionLayout,
    selectComponent,
    removeComponent,
    addComponentToSection,
    addComponent,
    moveComponent,
  };
});

// Re-exported so callers keep a single import for the default process template.
export { defaultProcessCode };
