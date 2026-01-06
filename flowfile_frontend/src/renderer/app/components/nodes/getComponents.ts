import { DefineComponent, markRaw } from "vue";

const componentCache: Record<string, Promise<DefineComponent>> = {};

export function getComponent(name: string): Promise<DefineComponent> {
  if (!componentCache[name]) {
    componentCache[name] = import(`./elements/manualInput/${name}.vue`).then((module) => {
      const component = markRaw(module.default);
      return component;
    });
  }
  return componentCache[name];
}

export function getComponentRaw(name: string): Promise<DefineComponent> {
  if (!componentCache[name]) {
    componentCache[name] = import(`./elements/manualInput/${name}.vue`).then((module) => {
      const component = markRaw(module.default);
      return component;
    });
  }
  return componentCache[name];
}
