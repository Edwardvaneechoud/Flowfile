// Shared Test-run state for the Node Designer. Held as a module singleton so the
// Test tab and the Code-tab test dock (both mount a TestPanel) share one sample
// dataset, one save toggle, and one dry-run result — el-tabs keeps both panes
// mounted, so per-component state would diverge.
import { computed, effectScope, ref, watch } from "vue";
import { useNodeDesignerStore } from "@/stores/node-designer-store";
import type { FileColumn } from "../../../components/nodes/baseNode/nodeInterfaces";
import { columnDataToTable, emptyTable, tableToColumnData, type SampleTable } from "../sampleData";
import { dryRunCustomNode, type DryRunBody } from "../../../api/nodeDesigner";
import type { ExampleInput } from "../designerState";
import { KernelApi } from "@/api/kernel.api";
import type { KernelInfo } from "@/types";

type DryRunTest = ReturnType<typeof create>;

let singleton: DryRunTest | null = null;

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

  async function loadKernels() {
    kernelsLoading.value = true;
    try {
      kernels.value = await KernelApi.getAll();
      dockerError.value = false;
      // Keep a valid selection: drop a stale id, default to the first kernel.
      if (selectedKernelId.value && !kernels.value.some((k) => k.id === selectedKernelId.value)) {
        selectedKernelId.value = null;
      }
      if (!selectedKernelId.value && kernels.value.length) {
        selectedKernelId.value = kernels.value[0].id;
      }
    } catch {
      kernels.value = [];
      dockerError.value = true;
    } finally {
      kernelsLoading.value = false;
    }
  }

  // Column-driven controls (ColumnSelector, IncomingColumns selects) preview against
  // the first sample input's columns, mirroring the Form-tab live preview.
  const paramColumns = computed<string[]>(() => tables.value[0]?.columns.map((c) => c.name) ?? []);
  const paramColumnTypes = computed<FileColumn[]>(() =>
    paramColumns.value.map((name) => ({ name, data_type: "String" }) as FileColumn),
  );

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
    const sampleInputs = tables.value.map((t) => tableToColumnData(t));
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
      },
    );
    watch(inputCount, seedTables);
    watch(saveWithNode, persistExampleInputs);
    // Load kernels once a node needs one; re-seed the selection from the node's id.
    watch(
      isKernelEnv,
      (needsKernel) => {
        if (needsKernel) {
          const seed = store.environment.default_kernel_id;
          if (seed) selectedKernelId.value = seed;
          if (!kernels.value.length) loadKernels();
        }
      },
      { immediate: true },
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
    loadKernels,
  };
}

export function useDryRunTest(): DryRunTest {
  if (!singleton) singleton = create();
  return singleton;
}
