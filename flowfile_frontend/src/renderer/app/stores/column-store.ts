// Column Store - Simple column tracking (kept for backward compatibility)
// DEPRECATED: The monolithic useNodeStore has been split into focused stores:
// - useFlowStore: Flow ID and VueFlow instance management
// - useNodeStore: Node data, validation, and descriptions
// - useResultsStore: Run results and caching
// - useEditorStore: Drawer, editor UI, log viewer, code generator
//
// For backward compatibility, useNodeStore is re-exported here, but new code
// should import from the specific store modules.

import { defineStore } from 'pinia';

export const useColumnStore = defineStore('column', {
  state: () => {
    return {
      localColumns: [] as string[],
    };
  },
  actions: {
    setColumns(columns: string[]) {
      this.localColumns = columns;
    },
    addColumn(column: string) {
      this.localColumns.push(column);
    },
  },
});

// Re-export the new stores for backward compatibility
export { useFlowStore } from './flow-store';
export { useNodeStore } from './node-store';
export { useResultsStore } from './results-store';
export { useEditorStore } from './editor-store';

// Re-export helper function for backward compatibility
import { NodeApi as NodeApiService } from '../services/api';
export { NodeApi } from '../services/api';
export const getDownstreamNodeIds = (flowId: number, nodeId: number) =>
  NodeApiService.getDownstreamNodeIds(flowId, nodeId);
