// Cross-store wiring for the tutorial: synthesizes events no single component
// seam can emit reliably, and seeds context from live app state. Kept out of
// tutorial-store.ts so the store stays importable in the node test env.
import { watch } from "vue";

import { useTutorialStore } from "../../stores/tutorial-store";
import { useFlowStore } from "../../stores/flow-store";
import { useEditorStore } from "../../stores/editor-store";
import { useNodeStore } from "../../stores/node-store";
import type { TutorialContext } from "./tutorial-engine";

export function useTutorialBridge() {
  const tutorialStore = useTutorialStore();
  const flowStore = useFlowStore();
  const editorStore = useEditorStore();
  const nodeStore = useNodeStore();

  tutorialStore.registerSeedProvider(() => seedFromApp(flowStore, editorStore, nodeStore));

  // Covers every flow-open path (recents, catalog, dialogs) in one place.
  watch(
    () => flowStore.flowId,
    (id, old) => {
      if (id && id > 0 && (!old || old <= 0)) {
        tutorialStore.notify({ type: "flow-opened", flowId: id });
      }
    },
  );

  // Falling edge covers every drawer-close path (close, toggle, hideAllPanels).
  watch(
    () => editorStore.drawerOpen,
    (open, was) => {
      if (was && !open) tutorialStore.notify({ type: "node-settings-closed" });
    },
  );

  // Rising edge covers the header button and the Ctrl+G shortcut.
  watch(
    () => editorStore.showCodeGenerator,
    (open, was) => {
      if (open && !was) tutorialStore.notify({ type: "code-generator-opened" });
    },
  );

  // Safety net for run completion: the polling emit misses cancel, poll
  // errors, and runs finishing while off the designer. checkRunStatus emits
  // first on the normal path, so the runCompleted guard prevents doubles.
  watch(
    () => editorStore.isRunning,
    (running, was) => {
      if (!was || running) return;
      const ctx = tutorialStore.context;
      if (ctx.runStarted && !ctx.runCompleted) {
        tutorialStore.notify({ type: "flow-run-completed", success: false });
      }
    },
  );
}

function seedFromApp(
  flowStore: ReturnType<typeof useFlowStore>,
  editorStore: ReturnType<typeof useEditorStore>,
  nodeStore: ReturnType<typeof useNodeStore>,
): Partial<TutorialContext> {
  const seed: Partial<TutorialContext> = {};
  if (flowStore.flowId && flowStore.flowId > 0) seed.flowId = flowStore.flowId;
  if (editorStore.showCodeGenerator) seed.codeGeneratorOpened = true;
  if (editorStore.drawerOpen && nodeStore.nodeId > 0) {
    seed.openSettingsNodeId = nodeStore.nodeId;
    seed.settingsEverOpenedFor = [nodeStore.nodeId];
  }

  const vueFlow = flowStore.vueFlowInstance;
  if (!vueFlow) return seed;

  const nodesByItem: Record<string, number[]> = {};
  for (const node of vueFlow.getNodes?.value ?? []) {
    // Hidden nodes (inside collapsed groups) can't be clicked or highlighted.
    if (node.hidden) continue;
    const item = node.data?.nodeTemplate?.item;
    const id = Number(node.data?.id ?? node.id);
    if (typeof item === "string" && Number.isFinite(id)) {
      (nodesByItem[item] ??= []).push(id);
    }
  }
  seed.nodesByItem = nodesByItem;
  seed.edges = (vueFlow.getEdges?.value ?? [])
    .map((edge: { source: string; target: string }) => ({
      sourceId: parseInt(edge.source, 10),
      targetId: parseInt(edge.target, 10),
    }))
    .filter(
      (edge: { sourceId: number; targetId: number }) =>
        Number.isFinite(edge.sourceId) && Number.isFinite(edge.targetId),
    );
  return seed;
}
