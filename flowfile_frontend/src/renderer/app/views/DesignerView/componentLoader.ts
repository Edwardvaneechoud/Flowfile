import { DefineComponent, markRaw } from "vue";
import { NodeTemplate } from "../../types";
import GenericNode from "../../nodes/GenericNode.vue"; // Import GenericNode directly

const componentCache: Map<string, Promise<DefineComponent>> = new Map();

/**
 * ALWAYS returns the GenericNode component
 * The nodeOrItem parameter is kept for backward compatibility but not used
 */
export function getComponent(_nodeOrItem: NodeTemplate | string): Promise<DefineComponent> {
  // Always return the same GenericNode component for ALL nodes
  const cacheKey = 'generic-node';
  
  if (componentCache.has(cacheKey)) {
    return componentCache.get(cacheKey)!;
  }
  
  // Just return the GenericNode component wrapped in a Promise
  const componentPromise = Promise.resolve(markRaw(GenericNode as any));
  
  componentCache.set(cacheKey, componentPromise);
  return componentPromise;
}

/**
 * Gets a component using the raw name (without case conversion)
 * This is kept for backward compatibility but also returns GenericNode
 */
export function getComponentRaw(name: string): Promise<DefineComponent> {
  // Also return GenericNode for raw components
  return getComponent(name);
}

/**
 * Clears the component cache
 * Useful for development or when components might have changed
 */
export function clearComponentCache(): void {
  componentCache.clear();
}

/**
 * Preload multiple components
 * Since we're using GenericNode for everything, this just ensures it's loaded
 */
export async function preloadComponents(_items: string[]): Promise<void> {
  // Just load the GenericNode once
  await getComponent('generic');
}