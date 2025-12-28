// DEPRECATED: Import from '@/types' or '../../../types' instead
// This file is kept for backward compatibility during migration

import { ref } from 'vue'

// Re-export all types from the new location
export * from '../../../types/node.types'

// Keep the nodeSelect ref for backward compatibility (it was defined here)
import type { NodeSelect } from '../../../types/node.types'
export const nodeSelect = ref<NodeSelect | null>(null)
