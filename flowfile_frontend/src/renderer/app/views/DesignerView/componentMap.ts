import { DefineComponent, markRaw } from "vue";

const componentMap: Map<string, DefineComponent> = new Map();

export async function preloadComponent(name: string, path: string) {
  const { default: component } = await import(path);
  componentMap.set(name, markRaw(component));
}

export function getComponent(name: string): DefineComponent | undefined {
  return componentMap.get(name);
}
