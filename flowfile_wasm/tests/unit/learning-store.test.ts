import { describe, it, expect, beforeEach } from 'vitest'
import { setActivePinia, createPinia } from 'pinia'
import { useLearningStore } from '../../src/stores/learning-store'

describe('learning mode', () => {
  beforeEach(() => {
    localStorage.clear()
    setActivePinia(createPinia())
  })

  it('is off until someone asks for it', () => {
    expect(useLearningStore().enabled).toBe(false)
  })

  it('remembers the choice across sessions', () => {
    useLearningStore().toggle()
    expect(localStorage.getItem('flowfile-learning-mode')).toBe('1')

    // A fresh pinia stands in for a reload.
    setActivePinia(createPinia())
    expect(useLearningStore().enabled).toBe(true)
  })

  it('turns back off again', () => {
    const store = useLearningStore()
    store.toggle()
    store.toggle()
    expect(store.enabled).toBe(false)
    expect(localStorage.getItem('flowfile-learning-mode')).toBe('0')
  })
})
