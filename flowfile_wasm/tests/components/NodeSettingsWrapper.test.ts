/**
 * The teaching surfaces are double-gated: the host's teachingMode prop offers
 * the capability, the user's Learning mode setting turns it on. The per-node
 * explainer must appear only when both say yes.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import NodeSettingsWrapper from '../../src/components/nodes/NodeSettingsWrapper.vue'
import { useLearningStore } from '../../src/stores/learning-store'
import type { NodeBase } from '../../src/types'

vi.mock('../../src/stores/flow-store', () => ({
  useFlowStore: () => ({
    nodes: new Map(),
    edges: [],
    validateNodeReference: vi.fn(() => ({ valid: true, error: null })),
    updateNodeReference: vi.fn()
  })
}))

const settings: NodeBase = {
  node_id: 1,
  is_setup: true,
  cache_results: true,
  pos_x: 0,
  pos_y: 0,
  description: ''
}

function mountWrapper(teachingMode: boolean) {
  return mount(NodeSettingsWrapper, {
    props: { nodeId: 1, settings, teachingMode },
    global: { stubs: { NodeExplainer: { template: '<div class="explainer-stub" />' } } }
  })
}

describe('NodeSettingsWrapper teaching gate', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    localStorage.clear()
  })

  it('hides the explainer until the user opts into Learning mode', () => {
    expect(mountWrapper(true).find('.explainer-stub').exists()).toBe(false)
  })

  it('shows the explainer once Learning mode is on', () => {
    useLearningStore().toggle()
    expect(mountWrapper(true).find('.explainer-stub').exists()).toBe(true)
  })

  it('never shows the explainer when the host switched teaching off', () => {
    useLearningStore().toggle()
    expect(mountWrapper(false).find('.explainer-stub').exists()).toBe(false)
  })
})
