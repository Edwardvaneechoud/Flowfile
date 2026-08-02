// Shared Test-run state for the Node Designer. Held as a module singleton so the
// Test tab and the Code-tab test dock (both mount a TestPanel) share one sample
// dataset, one save toggle, and one dry-run result — el-tabs keeps both panes
// mounted, so per-component state would diverge.
import { computed, effectScope, ref, watch } from "vue";
import { useNodeDesignerStore } from "@/stores/node-designer-store";
import type { FileColumn } from "../../../components/nodes/baseNode/nodeInterfaces";
import {
  columnDataToTable,
  emptyTable,
  sampleColumnsToFileColumns,
  tableToColumnData,
  tableToRawData,
  type SampleTable,
} from "../sampleData";
import { dryRunCustomNode, type DryRunBody } from "../../../api/nodeDesigner";
import type { ExampleInput } from "../designerState";
import { KernelApi } from "@/api/kernel.api";
import type { KernelInfo } from "@/types";
import { useKernelMatch } from "@/composables/useKernelMatch";
import { pickAutoSelect, sortKernelsByMatch } from "@/components/kernel/kernelMatch";
import type { UpgradePhase } from "@/components/kernel/kernelActions";

type DryRunTest = ReturnType<typeof create>;

let singleton: DryRunTest | null = null;

const MATCH_DEBOUNCE_MS = 500;

function create() {
  const store = useNodeDesignerStore();

  // One SampleTable per input port, seeded from example_inputs when present.
  const tables = ref<SampleTable[]>([]);
  const activeInput = ref(0);
  const saveWithNode = ref(store.designerState.example_inputs !== null);

  const inputCount = computed(() => Math.max(0, store.designerState.number_of_inputs));
  const hasParameters = computed(() => store.sections.some((s) => s.components.length > 0));

  // Kernel-environment nodes run the test in a Docker kernel (which has their pip
  // deps), not the worker. The chosen kernel is session-local to the designer —
  // seeded from the node's legacy kernel_id, never written back onto the candidate.
  const isKernelEnv = computed(() => store.environment.kind === "kernel");
  const kernels = ref<KernelInfo[]>([]);
  const kernelsLoading = ref(false);
  const dockerError = ref(false);
  const kernelsAvailable = computed(() => !dockerError.value);
  const selectedKernelId = ref<string | null>(store.environment.default_kernel_id ?? null);

  // Live-designer dependency matching: ranks kernels against the pip deps the
  // author is editing right now (not a saved snapshot of the node).
  const { matches, suggestion, matchLoading, matchUnavailable, matchFor, refreshMatch } =
    useKernelMatch();
  const upgradePhase = ref<UpgradePhase | null>(null);
  const deps = computed(() => (isKernelEnv.value ? store.environment.dependencies : []));
  const hasDeps = computed(() => deps.value.length > 0);
  const nodeDisplayName = computed(() => store.designerState.node_name.trim() || "custom-node");
  const sortedKernels = computed(() => sortKernelsByMatch(kernels.value, matches.value));
  const selectedMatch = computed(() => matchFor(selectedKernelId.value));
  const hasFullMatch = computed(() => matches.value.some((m) => m.level === "full"));

  async function loadKernels() {
    kernelsLoading.value = true;
    try {
      kernels.value = await KernelApi.getAll();
      dockerError.value = false;
      // Drop a stale selection so ensureSelection can re-pick a valid kernel.
      if (selectedKernelId.value && !kernels.value.some((k) => k.id === selectedKernelId.value)) {
        selectedKernelId.value = null;
      }
    } catch {
      kernels.value = [];
      dockerError.value = true;
    } finally {
      kernelsLoading.value = false;
    }
  }

  // Prefer a full dependency match when nothing is selected; first kernel otherwise.
  function ensureSelection() {
    if (selectedKernelId.value || !kernels.value.length) return;
    selectedKernelId.value = pickAutoSelect(matches.value, kernels.value) ?? kernels.value[0].id;
  }

  async function refreshKernels() {
    await Promise.all([loadKernels(), refreshMatch(deps.value, nodeDisplayName.value)]);
    ensureSelection();
  }

  // Column-driven controls (ColumnSelector, IncomingColumns selects) preview against
  // the first sample input's columns, mirroring the Form-tab live preview.
  const paramColumnTypes = computed<FileColumn[]>(() =>
    sampleColumnsToFileColumns(tables.value[0]?.columns ?? []),
  );
  const paramColumns = computed<string[]>(() => paramColumnTypes.value.map((c) => c.name));

  function seedTables() {
    const count = inputCount.value;
    const examples = store.designerState.example_inputs;
    const next: SampleTable[] = [];
    for (let i = 0; i < count; i++) {
      const example = examples?.[i];
      next.push(example ? columnDataToTable(example.data) : emptyTable());
    }
    tables.value = next;
    if (activeInput.value >= count) activeInput.value = 0;
  }

  function snapshotSettings(): Record<string, Record<string, unknown>> {
    return JSON.parse(JSON.stringify(store.previewValues));
  }

  function persistExampleInputs() {
    if (!saveWithNode.value) {
      store.designerState.example_inputs = null;
      return;
    }
    const examples: ExampleInput[] = tables.value.map((t) => ({ data: tableToColumnData(t) }));
    store.designerState.example_inputs = examples;
    store.designerState.example_settings = snapshotSettings();
  }

  function setTable(index: number, table: SampleTable) {
    tables.value[index] = table;
    persistExampleInputs();
  }

  async function run() {
    // Typed RawData (not bare {col: values}) so the grid's dtypes survive the wire —
    // JSON can't distinguish 21 from 21.0, so a float column would otherwise arrive mixed.
    const sampleInputs = tables.value.map((t) => tableToRawData(t));
    const body: DryRunBody = {
      settings_values: snapshotSettings(),
      sample_inputs: sampleInputs.length ? sampleInputs : null,
      row_limit: 100,
      timeout_seconds: 30,
    };
    if (store.codeOnly) body.code = store.codeText;
    else body.designer_state = store.designerState;
    if (isKernelEnv.value) body.kernel_id = selectedKernelId.value;

    store.dryRun.running = true;
    store.dryRun.error = null;
    try {
      const result = await dryRunCustomNode(body);
      store.dryRun.result = result;
      if (saveWithNode.value) persistExampleInputs();
    } catch (err: unknown) {
      const e = err as { response?: { data?: { detail?: string } }; message?: string };
      store.dryRun.error = e.response?.data?.detail || e.message || "Dry run failed";
      store.dryRun.result = null;
    } finally {
      store.dryRun.running = false;
      // The run may have started/warmed the kernel, so refresh the list — otherwise
      // the picker keeps showing the stale state from the initial load.
      if (isKernelEnv.value) void refreshKernels();
    }
  }

  // Detached scope so these watchers live for the app, not the first mounting component.
  const scope = effectScope(true);
  scope.run(() => {
    seedTables();
    // Re-seed when a different node is loaded (designerState is reassigned) or the
    // input-port count changes.
    watch(
      () => store.designerState,
      () => {
        saveWithNode.value = store.designerState.example_inputs !== null;
        seedTables();
        if (isKernelEnv.value) {
          const seed = store.environment.default_kernel_id;
          if (seed) selectedKernelId.value = seed;
          void refreshKernels();
        }
      },
    );
    watch(inputCount, seedTables);
    watch(saveWithNode, persistExampleInputs);
    // Load kernels + match once a node needs one; re-seed the selection from the node's id.
    watch(
      isKernelEnv,
      (needsKernel) => {
        if (needsKernel) {
          const seed = store.environment.default_kernel_id;
          if (seed) selectedKernelId.value = seed;
          void refreshKernels();
        }
      },
      { immediate: true },
    );
    // Re-match while the author edits the dependency chips; debounced so
    // mid-typing keystrokes don't spam the endpoint. Stale replies are dropped
    // by useKernelMatch's own sequence guard.
    let matchDebounce: ReturnType<typeof setTimeout> | null = null;
    watch(
      () => deps.value.join("\n"),
      () => {
        if (matchDebounce) clearTimeout(matchDebounce);
        matchDebounce = setTimeout(() => {
          void refreshMatch(deps.value, nodeDisplayName.value);
        }, MATCH_DEBOUNCE_MS);
      },
    );
  });

  return {
    tables,
    activeInput,
    saveWithNode,
    inputCount,
    hasParameters,
    paramColumns,
    paramColumnTypes,
    setTable,
    run,
    isKernelEnv,
    kernels,
    kernelsLoading,
    kernelsAvailable,
    selectedKernelId,
    refreshKernels,
    deps,
    hasDeps,
    nodeDisplayName,
    suggestion,
    matchLoading,
    matchUnavailable,
    matchFor,
    sortedKernels,
    selectedMatch,
    hasFullMatch,
    upgradePhase,
  };
}

export function useDryRunTest(): DryRunTest {
  if (!singleton) singleton = create();
  return singleton;
}
