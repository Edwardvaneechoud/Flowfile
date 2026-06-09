/// <reference types="vite/client" />

// Injected by Vite `define` (vite.config.ts) — the WASM editor package version.
declare const __APP_VERSION__: string

declare module '*.vue' {
  import type { DefineComponent } from 'vue'
  const component: DefineComponent<{}, {}, any>
  export default component
}
