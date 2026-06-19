/**
 * NodeInfoModal Component Tests
 *
 * The popup is shared by every node: available nodes show their info + a docs link,
 * locked (full-app) nodes additionally show the install CTA. These tests lock that fork.
 */

import { describe, it, expect, afterEach } from 'vitest'
import { mount, type VueWrapper } from '@vue/test-utils'
import NodeInfoModal from '../../src/components/NodeInfoModal.vue'

describe('NodeInfoModal', () => {
  let wrapper: VueWrapper | null = null

  afterEach(() => {
    wrapper?.unmount()
    wrapper = null
    document.body.innerHTML = ''
  })

  it('shows the install CTA and docs link for a locked (full-app) node', () => {
    wrapper = mount(NodeInfoModal, {
      props: {
        name: 'Read from Database',
        intro: '',
        docsUrl: 'https://example.com/docs/input#database-reader',
        available: false,
        position: { x: 100, y: 100 }
      }
    })

    expect(document.body.querySelector('a[href="https://flowfile.io/install/"]')).not.toBeNull()
    expect(
      document.body.querySelector('a[href="https://example.com/docs/input#database-reader"]')
    ).not.toBeNull()
    expect(document.body.textContent).toContain('full Flowfile installation only')
  })

  it('shows the docs link and intro but no install CTA for an available node', () => {
    wrapper = mount(NodeInfoModal, {
      props: {
        name: 'Filter',
        intro: 'Filter rows based on conditions.',
        docsUrl: 'https://example.com/docs/transform#filter-data',
        available: true,
        position: { x: 100, y: 100 }
      }
    })

    expect(document.body.querySelector('a[href="https://flowfile.io/install/"]')).toBeNull()
    expect(
      document.body.querySelector('a[href="https://example.com/docs/transform#filter-data"]')
    ).not.toBeNull()
    expect(document.body.textContent).toContain('Filter rows based on conditions.')
  })

  it('emits close when the close button is clicked', () => {
    wrapper = mount(NodeInfoModal, {
      props: { name: 'Filter', intro: '', docsUrl: '', available: true, position: { x: 100, y: 100 } }
    })

    document.body.querySelector<HTMLButtonElement>('.node-info-close')!.click()
    expect(wrapper.emitted('close')).toBeTruthy()
  })

  it('closes on a pointerdown outside the popup', () => {
    wrapper = mount(NodeInfoModal, {
      props: { name: 'Filter', intro: '', docsUrl: '', available: true, position: { x: 100, y: 100 } }
    })

    document.body.dispatchEvent(new Event('pointerdown', { bubbles: true }))
    expect(wrapper.emitted('close')).toBeTruthy()
  })

  it('stays open on a pointerdown inside the popup', () => {
    wrapper = mount(NodeInfoModal, {
      props: { name: 'Filter', intro: '', docsUrl: '', available: true, position: { x: 100, y: 100 } }
    })

    document.body.querySelector('.node-info-card')!.dispatchEvent(new Event('pointerdown', { bubbles: true }))
    expect(wrapper.emitted('close')).toBeFalsy()
  })
})
