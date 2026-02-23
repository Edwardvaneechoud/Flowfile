// Main component
export { default as FlowfileEditor } from './FlowfileEditor.vue'

// Plugin for app.use() registration
export { FlowfileEditorPlugin } from './plugin'
export type { FlowfilePluginOptions } from './plugin'

// Public API types
export type {
  FlowfileEditorProps,
  FlowfileEditorAPI,
  PyodideConfig,
  InputDataMap,
  InputDataItem,
  ThemeConfig,
  ToolbarConfig,
  NodeCategoryConfig,
  OutputData,
  EditorError
} from './types'

// Re-export core types consumers may need
export type {
  FlowfileData,
  FlowfileNode,
  FlowNode,
  FlowEdge,
  NodeResult,
  DataPreview,
  ColumnSchema,
  NodeSettings,
  NodeType
} from '../types'
