/// <reference types="vite/client" />

declare module '*.vue' {
  import type { DefineComponent } from 'vue'
  const component: DefineComponent<{}, {}, any>
  export default component
}

/** Flowfile version injected at build time from pyproject.toml */
declare const __FLOWFILE_VERSION__: string
