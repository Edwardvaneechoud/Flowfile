import type { NodeBase } from "../components/nodes/baseNode/nodeInput";

/**
 * Composable for handling generic node settings updates.
 * Automatically syncs NodeBase properties (cache_results, description, output_field_config)
 * from genericNodeSettings back to the node.
 *
 * @example
 * ```typescript
 * const { handleGenericSettingsUpdate } = useGenericNodeSettings(nodePolarsCode);
 *
 * // In template:
 * <generic-node-settings
 *   :model-value="nodePolarsCode"
 *   @update:modelValue="handleGenericSettingsUpdate"
 * >
 * ```
 */
export function useGenericNodeSettings<T extends NodeBase>(nodeRef: { value: T | null }) {
  const handleGenericSettingsUpdate = (updatedNode: T) => {
    if (!nodeRef.value) return;

    // Automatically sync all NodeBase properties
    // These are common to all node types
    nodeRef.value.cache_results = updatedNode.cache_results;
    nodeRef.value.description = updatedNode.description;
    nodeRef.value.output_field_config = updatedNode.output_field_config;
    nodeRef.value.pos_x = updatedNode.pos_x;
    nodeRef.value.pos_y = updatedNode.pos_y;

    // If there are additional properties on the updated node that exist on the original,
    // copy them as well (this handles node-specific properties if they were changed)
    // Note: This is a shallow copy for additional properties
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
    handleGenericSettingsUpdate,
  };
}
