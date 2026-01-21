// composables/useNodeSettings.ts
// Standardized composable for managing node settings with lifecycle hooks
import { type Ref } from "vue";
import { useNodeStore } from "../stores/column-store";

/**
 * Options for the useNodeSettings composable
 */
export interface UseNodeSettingsOptions<T> {
  /**
   * Reactive reference to the node data object
   */
  nodeData: Ref<T | null>;

  /**
   * Set to true for custom/user-defined nodes that use a different API endpoint
   * @default false
   */
  isUserDefined?: boolean;

  /**
   * Hook called before saving settings to the backend.
   * Use this for data transformation, cleanup, or validation.
   * If this function throws an error, the save will be aborted.
   */
  beforeSave?: () => Promise<void> | void;

  /**
   * Hook called after settings have been successfully saved to the backend.
   * Use this for validation, cache invalidation, or UI state updates.
   */
  afterSave?: () => Promise<void> | void;
}

/**
 * Return type for the useNodeSettings composable
 */
export interface UseNodeSettingsReturn {
  /**
   * Save settings to the backend without closing the drawer.
   * Useful when one tab's settings depend on another tab's saved state.
   * Does NOT trigger beforeSave/afterSave hooks - use pushNodeData for that.
   */
  saveSettings: () => Promise<void>;

  /**
   * Save settings with full lifecycle hooks (beforeSave -> save -> afterSave).
   * This is called automatically when the drawer closes.
   * Expose this method via defineExpose() for the drawer to call.
   */
  pushNodeData: () => Promise<void>;
}

/**
 * Composable for standardized node settings management.
 *
 * Provides a consistent pattern for all node components to:
 * - Save settings to the backend
 * - Handle pre-save transformations (beforeSave hook)
 * - Handle post-save operations (afterSave hook)
 * - Support both standard and user-defined node types
 *
 * @example
 * ```typescript
 * // Simple usage - just save settings
 * const { saveSettings, pushNodeData } = useNodeSettings({
 *   nodeData: nodeFilter,
 * });
 *
 * // With hooks for custom logic
 * const { saveSettings, pushNodeData } = useNodeSettings({
 *   nodeData: nodeFilter,
 *   beforeSave: () => {
 *     // Transform data before saving
 *     if (isAdvancedMode.value) {
 *       nodeFilter.value.mode = 'advanced';
 *     }
 *   },
 *   afterSave: async () => {
 *     // Validate after saving
 *     await instantValidate();
 *   },
 * });
 *
 * // For user-defined/custom nodes
 * const { saveSettings, pushNodeData } = useNodeSettings({
 *   nodeData: nodeUserDefined,
 *   isUserDefined: true,
 * });
 *
 * // Expose for drawer integration
 * defineExpose({ loadNodeData, pushNodeData, saveSettings });
 * ```
 */
export function useNodeSettings<T>(options: UseNodeSettingsOptions<T>): UseNodeSettingsReturn {
  const { nodeData, isUserDefined = false, beforeSave, afterSave } = options;
  const nodeStore = useNodeStore();

  /**
   * Save settings to the backend without triggering lifecycle hooks.
   * Use this for intermediate saves (e.g., when switching tabs within the drawer).
   */
  const saveSettings = async (): Promise<void> => {
    if (!nodeData.value) {
      console.warn("Cannot save settings: nodeData is null");
      return;
    }

    try {
      if (isUserDefined) {
        await nodeStore.updateUserDefinedSettings(nodeData);
      } else {
        await nodeStore.updateSettings(nodeData);
      }
    } catch (error) {
      console.error("Error saving node settings:", error);
      throw error;
    }
  };

  /**
   * Full save operation with lifecycle hooks.
   * Called by the drawer when closing or switching nodes.
   */
  const pushNodeData = async (): Promise<void> => {
    if (!nodeData.value) {
      console.warn("Cannot push node data: nodeData is null");
      return;
    }

    try {
      // Execute beforeSave hook if provided
      if (beforeSave) {
        await beforeSave();
      }

      // Save to backend
      await saveSettings();

      // Execute afterSave hook if provided
      if (afterSave) {
        await afterSave();
      }
    } catch (error) {
      console.error("Error in pushNodeData:", error);
      throw error;
    }
  };

  return {
    saveSettings,
    pushNodeData,
  };
}

export default useNodeSettings;
