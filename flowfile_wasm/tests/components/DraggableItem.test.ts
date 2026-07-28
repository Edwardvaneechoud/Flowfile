/**
 * DraggableItem component tests.
 *
 * Guards the initial-offset resolution across the intent-store rework: a docked
 * panel given initialTop=0 (e.g. docking flush to the container top when the
 * app toolbar is hidden) must render at top 0 and not be coerced to the
 * free-panel 100 default — while a free panel with no initialTop must still
 * fall back to 100.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import { nextTick } from 'vue'
import DraggableItem from '../../src/components/common/DraggableItem/DraggableItem.vue'
import { useItemStore } from '../../src/components/common/DraggableItem/stateStore'

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

  it('renders at top:0 when initialTop is 0 (does not coerce a real 0 to the 100 default)', async () => {
    const store = useItemStore()
    store.containerBounds = { width: 1200, height: 800 }
    const wrapper = mount(DraggableItem, {
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

    expect(store.panelConfigs['panel-zero-top'].v.defaultOffset).toBe(0)
    expect((wrapper.element as HTMLElement).style.top).toBe('0px')
  })

  it('falls back to the 100 default when initialTop is unset on a free panel', async () => {
    const store = useItemStore()
    store.containerBounds = { width: 1200, height: 800 }
    const wrapper = mount(DraggableItem, {
      props: {
        id: 'panel-free',
        initialPosition: 'free',
        initialWidth: 300,
        initialHeight: 200,
      },
      slots: { default: '<div>body</div>' },
    })
    await settle()

    expect(store.panelConfigs['panel-free'].v.defaultOffset).toBe(100)
    expect((wrapper.element as HTMLElement).style.top).toBe('100px')
  })
})
