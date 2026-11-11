// Mounts a component with Vue itself: @vue/test-utils is not a dependency.
import { createApp, defineComponent, h, type Component } from "vue";
import { createPinia, setActivePinia, type Pinia } from "pinia";

// Element Plus is registered app-wide in the real app; here each tag renders its slots.
const ELEMENT_TAGS = [
  "el-popover",
  "el-tooltip",
  "el-select",
  "el-option",
  "el-option-group",
  "el-input",
  "el-button",
  "el-card",
  "el-collapse",
  "el-collapse-item",
  "el-alert",
  "el-tag",
  "el-dropdown",
  "el-dropdown-menu",
  "el-dropdown-item",
  "el-icon",
  "el-timeline",
  "el-timeline-item",
];

const passthrough = (tag: string) =>
  defineComponent({
    name: tag,
    inheritAttrs: false,
    setup(_, { slots }) {
      return () =>
        h(
          "div",
          { "data-stub": tag },
          Object.values(slots).flatMap((slot) => slot?.() ?? []),
        );
    },
  });

export interface Mounted {
  el: HTMLElement;
  pinia: Pinia;
  text: () => string;
  unmount: () => void;
}

/** Mount `component` on a fresh pinia; `setup` runs after the pinia is active and before the mount. */
export function mount(
  component: Component,
  props: Record<string, unknown> = {},
  setup?: (pinia: Pinia) => void,
): Mounted {
  const pinia = createPinia();
  setActivePinia(pinia);
  setup?.(pinia);
  const app = createApp(component, props);
  app.use(pinia);
  for (const tag of ELEMENT_TAGS) app.component(tag, passthrough(tag));
  const el = document.createElement("div");
  document.body.appendChild(el);
  app.mount(el);
  return {
    el,
    pinia,
    text: () => (el.textContent ?? "").replace(/\s+/g, " ").trim(),
    unmount: () => {
      app.unmount();
      el.remove();
    },
  };
}
