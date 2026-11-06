import { createApp, h, nextTick, type Component, type FunctionalComponent } from "vue";
import type { Pinia } from "pinia";
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
