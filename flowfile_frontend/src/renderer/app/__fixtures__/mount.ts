// Mounts a component with Vue itself: @vue/test-utils is not a dependency.
import {
  createApp,
  defineComponent,
  h,
  nextTick,
  type Component,
  type FunctionalComponent,
} from "vue";
import { createPinia, setActivePinia, type Pinia } from "pinia";
import { onTestFinished } from "vitest";

/** A stand-in for localStorage or sessionStorage, for `vi.stubGlobal`. */
export const fakeStorage = () => {
  const map = new Map<string, string>();
  return {
    getItem: (key: string) => map.get(key) ?? null,
    setItem: (key: string, value: string) => void map.set(key, value),
    removeItem: (key: string) => void map.delete(key),
    clear: () => map.clear(),
    values: () => [...map.values()],
  };
};

/** A global component's stub: a plain `tag` with the attributes and default slot it was given. */
export const element = (tag: string): FunctionalComponent => {
  const stub: FunctionalComponent = (_, { attrs, slots }) => h(tag, attrs, slots.default?.());
  stub.inheritAttrs = false;
  return stub;
};

/** Lets pending promises and the renders they cause finish. */
export const settle = async () => {
  for (let i = 0; i < 10; i++) {
    await Promise.resolve();
    await nextTick();
  }
};

/** An element's text as a person reads it. */
export const text = (el: Element | null) => el?.textContent?.replace(/\s+/g, " ").trim() ?? "";

/** Mounts a component in a new element on the page and settles it; the end of the test takes both away. */
export const mountIn = async (
  component: Component,
  options: {
    props?: Record<string, unknown>;
    pinia?: Pinia;
    stubs?: Record<string, FunctionalComponent>;
  } = {},
): Promise<HTMLElement> => {
  const host = document.createElement("div");
  document.body.appendChild(host);
  const app = createApp(component, options.props);
  if (options.pinia) app.use(options.pinia);
  for (const [name, stub] of Object.entries(options.stubs ?? {})) app.component(name, stub);
  onTestFinished(() => {
    app.unmount();
    host.remove();
  });
  app.mount(host);
  await settle();
  return host;
};

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
