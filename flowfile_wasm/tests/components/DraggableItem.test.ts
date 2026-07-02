/**
 * DraggableItem component tests.
 *
 * Guards the initial-offset resolution: a docked panel given initialTop=0 (e.g.
 * docking flush to the container top when the app toolbar is hidden) must stay
 * at top 0 and not be coerced to the 100 "unset" default — while an unset
 * offset must still fall back to 100.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import { nextTick } from 'vue'
import DraggableItem from '../../src/components/common/DraggableItem/DraggableItem.vue'
import { useItemStore } from '../../src/components/common/DraggableItem/stateStore'

// onMounted registers state, then applyStickyPosition runs on nextTick; give it
// a couple of ticks to settle.
const settle = async () => {
  await nextTick()
  await nextTick()
}

describe('DraggableItem component', () => {
  beforeEach(() => {
    localStorage.clear()
    setActivePinia(createPinia())
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('docks at top:0 when initialTop is 0 (does not coerce a real 0 to the 100 default)', async () => {
    const store = useItemStore()
    mount(DraggableItem, {
      props: {
        id: 'panel-zero-top',
        showRight: true,
        initialPosition: 'right',
        initialWidth: 450,
        initialHeight: 600,
        initialTop: 0,
        heightBehaviour: 'scale',
      },
      slots: { default: '<div>body</div>' },
    })
    await settle()

    expect(store.items['panel-zero-top'].top).toBe(0)
  })

  it('falls back to the 100 default when initialTop is unset', async () => {
    const store = useItemStore()
    mount(DraggableItem, {
      props: {
        id: 'panel-free',
        initialPosition: 'free',
        initialWidth: 300,
        initialHeight: 200,
      },
      slots: { default: '<div>body</div>' },
    })
    await settle()

    expect(store.items['panel-free'].top).toBe(100)
  })
})
