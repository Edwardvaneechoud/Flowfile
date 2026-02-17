import type { App, Plugin } from 'vue'
import { createPinia } from 'pinia'
import FlowfileEditor from './FlowfileEditor.vue'

export interface FlowfilePluginOptions {
  /** Provide an existing Pinia instance if the host app doesn't have one */
  pinia?: ReturnType<typeof createPinia>
}

export const FlowfileEditorPlugin: Plugin = {
  install(app: App, options?: FlowfilePluginOptions) {
    // Check if Pinia is already installed
    const hasPinia = app.config.globalProperties.$pinia !== undefined

    if (!hasPinia) {
      const pinia = options?.pinia ?? createPinia()
      app.use(pinia)
    }

    // Register the component globally
    app.component('FlowfileEditor', FlowfileEditor)
  }
}
