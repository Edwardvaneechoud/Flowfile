import { ref, watch, type Ref } from "vue";
import { ElMessage } from "element-plus";
import type { NodeBase } from "../types/node.types";
import { useNodeStore } from "../stores/node-store";
import { useEditorStore } from "../stores/editor-store";
import { extractSaveErrorMessage } from "./saveError";

export { extractSaveErrorMessage };

/**
 * Options for configuring the useNodeSettings composable
 */
export interface UseNodeSettingsOptions<T extends NodeBase> {
  /**
   * The reactive reference to the node data
   */
  nodeRef: Ref<T | null>;

  /**
   * Optional callback called before saving settings.
   * Can be used to transform or validate data before save.
   * Return false to cancel the save operation.
   */
  onBeforeSave?: () => boolean | void | Promise<boolean | void>;

  /**
   * Optional callback called after settings are successfully saved.
   * Useful for validation, refreshing data, or other post-save operations.
   */
  onAfterSave?: () => void | Promise<void>;

  /**
   * Whether to automatically set is_setup to true before saving.
   * Defaults to true.
   */
  autoSetIsSetup?: boolean;
}

/**
 * Return type for the useNodeSettings composable
 */
export interface UseNodeSettingsReturn {
  /**
   * Whether a save operation is currently in progress
   */
  isSaving: Ref<boolean>;

  /**
   * Save settings to the backend without closing the drawer.
   * Returns a promise that resolves when the save is complete.
   * Useful for operations like "Load from Schema" that need
   * the backend to have the latest settings.
   */
  saveSettings: () => Promise<boolean>;

  /**
   * Push node data - standard method called when drawer closes.
   * This is the method that should be exposed via defineExpose.
   */
  /** Resolves false when the save was refused, so a caller can skip its success state. */
  pushNodeData: () => Promise<boolean>;

  /**
   * Handle updates from genericNodeSettings component.
   * Automatically syncs NodeBase properties.
   */
  handleGenericSettingsUpdate: (updatedNode: NodeBase) => void;
}

/**
 * Composable for standardized node settings management.
 *
 * This composable provides a consistent pattern for all nodes to:
 * - Save settings to the backend
 * - Handle pre-save and post-save callbacks
 * - Support saving without closing the drawer (for operations like "Load from Schema")
 * - Track saving state for loading indicators
 *
 * @example
 * ```typescript
 * const nodeFilter = ref<NodeFilter | null>(null);
 *
 * const {
 *   isSaving,
 *   saveSettings,
 *   pushNodeData,
 *   handleGenericSettingsUpdate
 * } = useNodeSettings({
 *   nodeRef: nodeFilter,
 *   onAfterSave: async () => {
 *     await refreshPreview();
 *   }
 * });
 *
 * // In template:
 * <generic-node-settings
 *   v-model="nodeFilter"
 *   @update:model-value="handleGenericSettingsUpdate"
 *   @request-save="saveSettings"
 * >
 *
 * // Expose for drawer lifecycle
 * defineExpose({
 *   loadNodeData,
 *   pushNodeData,
 *   saveSettings, // Optional: expose for programmatic saves
 * });
 * ```
 */
export function useNodeSettings<T extends NodeBase>(
  options: UseNodeSettingsOptions<T>,
): UseNodeSettingsReturn {
  const { nodeRef, onBeforeSave, onAfterSave, autoSetIsSetup = true } = options;

  const nodeStore = useNodeStore();
  const isSaving = ref(false);

  // The settings as last loaded or saved; reassigning nodeRef is a load, in-place changes are edits.
  const snapshot = (): string | null => (nodeRef.value ? JSON.stringify(nodeRef.value) : null);
  let cleanSnapshot: string | null = null;
  watch(
    nodeRef,
    () => {
      cleanSnapshot = snapshot();
    },
    { immediate: true },
  );

  /**
   * Save settings to the backend.
   * Can be called without closing the drawer.
   * Resolves true when nothing is loaded yet (or the load failed): there is nothing to save,
   * so Run, a node switch or minimize must not be blocked by it.
   */
  const saveSettings = async (): Promise<boolean> => {
    if (!nodeRef.value) {
      console.warn("useNodeSettings: nothing to save - nodeRef is null");
      return true;
    }

    if (onBeforeSave) {
      const shouldContinue = await onBeforeSave();
      if (shouldContinue === false) {
        return false;
      }
    }

    isSaving.value = true;

    try {
      if (autoSetIsSetup && nodeRef.value.is_setup !== undefined) {
        nodeRef.value.is_setup = true;
      }

      await nodeStore.updateSettings(nodeRef);
      cleanSnapshot = snapshot();
      // Covers saves outside a leave, such as the request-save on the output-schema tab.
      useEditorStore().disarmRefusedSave();

      if (onAfterSave) {
        await onAfterSave();
      }

      return true;
    } catch (error) {
      console.error("useNodeSettings: Error saving settings:", error);
      ElMessage.error({ message: extractSaveErrorMessage(error), showClose: true, duration: 6000 });
      return false;
    } finally {
      isSaving.value = false;
    }
  };

  /**
   * Push node data - called when drawer closes.
   * This wraps saveSettings for the standard drawer lifecycle.
   * A refused save of settings unchanged since they were loaded resolves true: the server already
   * holds them, so a fresh, not-yet-valid node (e.g. a Read with no file) never blocks leaving it.
   */
  const pushNodeData = async (): Promise<boolean> =>
    (await saveSettings()) || (cleanSnapshot !== null && snapshot() === cleanSnapshot);

  /**
   * Handle updates from genericNodeSettings component.
   * Syncs NodeBase properties from the settings component back to the node.
   */
  const handleGenericSettingsUpdate = (updatedNode: NodeBase): void => {
    if (!nodeRef.value) return;

    nodeRef.value.cache_results = updatedNode.cache_results;
    nodeRef.value.description = updatedNode.description;
    nodeRef.value.output_field_config = updatedNode.output_field_config;

    // Position is managed by the store during save, but sync if provided
    if (updatedNode.pos_x !== undefined) {
      nodeRef.value.pos_x = updatedNode.pos_x;
    }
    if (updatedNode.pos_y !== undefined) {
      nodeRef.value.pos_y = updatedNode.pos_y;
    }

    for (const key in updatedNode) {
      if (
        key in nodeRef.value &&
        key !== "cache_results" &&
        key !== "description" &&
        key !== "output_field_config" &&
        key !== "pos_x" &&
        key !== "pos_y"
      ) {
        (nodeRef.value as any)[key] = (updatedNode as any)[key];
      }
    }
  };

  return {
    isSaving,
    saveSettings,
    pushNodeData,
    handleGenericSettingsUpdate,
  };
}

/**
 * Simplified version of useNodeSettings for nodes that don't need
 * custom callbacks. Provides the same interface with sensible defaults.
 *
 * @example
 * ```typescript
 * const nodeSort = ref<NodeSort | null>(null);
 * const { saveSettings, pushNodeData } = useSimpleNodeSettings(nodeSort);
 * ```
 */
export function useSimpleNodeSettings<T extends NodeBase>(nodeRef: Ref<T | null>) {
  return useNodeSettings({ nodeRef });
}
