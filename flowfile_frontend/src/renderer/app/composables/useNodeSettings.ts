import { ref, type Ref } from "vue";
import type { NodeBase } from "../types/node.types";
import { useNodeStore } from "../stores/node-store";

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
   * Optional callback to set up validation function for the node.
   * Called after save to register the validation function with the store.
   */
  getValidationFunc?: () => (() => void) | undefined;

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
  pushNodeData: () => Promise<void>;

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
 *     await validateNode();
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
  const { nodeRef, onBeforeSave, onAfterSave, getValidationFunc, autoSetIsSetup = true } = options;

  const nodeStore = useNodeStore();
  const isSaving = ref(false);

  /**
   * Save settings to the backend.
   * Can be called without closing the drawer.
   */
  const saveSettings = async (): Promise<boolean> => {
    if (!nodeRef.value) {
      console.warn("useNodeSettings: Cannot save - nodeRef is null");
      return false;
    }

    // Run before save callback
    if (onBeforeSave) {
      const shouldContinue = await onBeforeSave();
      if (shouldContinue === false) {
        return false;
      }
    }

    isSaving.value = true;

    try {
      // Auto-set is_setup flag if enabled
      if (autoSetIsSetup && nodeRef.value.is_setup !== undefined) {
        nodeRef.value.is_setup = true;
      }

      // Save to backend
      await nodeStore.updateSettings(nodeRef);

      // Run after save callback
      if (onAfterSave) {
        await onAfterSave();
      }

      // Register validation function if provided
      if (getValidationFunc) {
        const validateFunc = getValidationFunc();
        if (validateFunc && nodeRef.value) {
          nodeStore.setNodeValidateFunc(nodeRef.value.node_id, validateFunc);
        }
      }

      return true;
    } catch (error) {
      console.error("useNodeSettings: Error saving settings:", error);
      return false;
    } finally {
      isSaving.value = false;
    }
  };

  /**
   * Push node data - called when drawer closes.
   * This wraps saveSettings for the standard drawer lifecycle.
   */
  const pushNodeData = async (): Promise<void> => {
    await saveSettings();
  };

  /**
   * Handle updates from genericNodeSettings component.
   * Syncs NodeBase properties from the settings component back to the node.
   */
  const handleGenericSettingsUpdate = (updatedNode: NodeBase): void => {
    if (!nodeRef.value) return;

    // Sync NodeBase properties
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

    // Copy any additional properties that exist on both objects
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
