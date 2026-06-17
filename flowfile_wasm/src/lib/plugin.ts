import type { App, Plugin } from 'vue'
import { createPinia } from 'pinia'
import FlowfileEditor from './FlowfileEditor.vue'

export interface FlowfilePluginOptions {
  /** Provide an existing Pinia instance if the host app doesn't have one */
  pinia?: ReturnType<typeof createPinia>
}

export const FlowfileEditorPlugin: Plugin = {
  install(app: App, options?: FlowfilePluginOptions) {
    const hasPinia = app.config.globalProperties.$pinia !== undefined

    if (!hasPinia) {
      const pinia = options?.pinia ?? createPinia()
      app.use(pinia)
    }

    app.component('FlowfileEditor', FlowfileEditor)
  }
}
