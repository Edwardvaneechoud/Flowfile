// DEPRECATED: Import from '@/types' or '../../../types' instead
// This file is kept for backward compatibility during migration

import { ref } from 'vue'

// Re-export all types from the new location
export * from '../../../types/node.types'

// Re-export RunInformation from flow.types as it was originally defined here
export type { RunInformation, RunInformationDictionary } from '../../../types/flow.types'

// Keep the nodeData ref for backward compatibility (it was defined here)
import type { NodeData } from '../../../types/node.types'
export const nodeData = ref<NodeData | null>(null)
