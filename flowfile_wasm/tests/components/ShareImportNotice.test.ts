/**
 * ShareImportNotice Component Tests
 *
 * One banner, two jobs: the dismissable placeholder warning, and the trust
 * prompt whose button is the only way a shared flow's code gets to run.
 */

import { describe, it, expect } from 'vitest'
import { mount } from '@vue/test-utils'
import ShareImportNotice from '../../src/components/ShareImportNotice.vue'

const nodes = [{ nodeId: 2, label: 'Polars Code', reason: 'runs Python its sender wrote' }]

describe('ShareImportNotice', () => {
  it('asks for trust with one button and cannot be dismissed', async () => {
    const wrapper = mount(ShareImportNotice, {
      props: {
        variant: 'warning',
        title: 'This shared flow contains custom code',
        placeholders: nodes,
        actionLabel: 'Trust and run',
        closable: false
      }
    })

    expect(wrapper.find('.share-notice__close').exists()).toBe(false)
    expect(wrapper.find('.share-notice__footer').exists()).toBe(false)
    expect(wrapper.text()).toContain('Polars Code (#2)')

    await wrapper.find('.share-notice__action').trigger('click')
    expect(wrapper.emitted('action')).toHaveLength(1)

    await wrapper.find('.share-notice__node').trigger('click')
    expect(wrapper.emitted('focus-node')).toEqual([[2]])
  })

  it('does not offer the action while it is disabled', async () => {
    const wrapper = mount(ShareImportNotice, {
      props: { variant: 'warning', title: 'x', actionLabel: 'Loading the runtime…', actionDisabled: true, closable: false }
    })

    await wrapper.find('.share-notice__action').trigger('click')
    expect(wrapper.emitted('action')).toBeUndefined()
  })

  it('keeps the placeholder warning dismissable, with its install hint and no action', async () => {
    const wrapper = mount(ShareImportNotice, {
      props: { variant: 'warning', title: "1 node isn't supported in the browser version", placeholders: nodes }
    })

    expect(wrapper.find('.share-notice__action').exists()).toBe(false)
    expect(wrapper.find('.share-notice__footer').exists()).toBe(true)

    await wrapper.find('.share-notice__close').trigger('click')
    expect(wrapper.emitted('close')).toHaveLength(1)
  })
})
