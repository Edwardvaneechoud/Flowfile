/**
 * Loads a catalog dataset into a Graphic Walker payload (fields + rows) for the
 * Visuals feature, ensuring Pyodide is ready first. This is the WASM client-side
 * replacement for flowfile_frontend's worker-backed source-fetch composables:
 * GraphicWalker aggregates the materialised rows in-browser (capped at
 * GW_MAX_ROWS by the Python side), so no `computation` callback is needed.
 *
 * Results are de-duplicated per dataset name via a module-level promise cache so
 * a dashboard with many tiles on the same table only parses it once.
 */

import { ref, shallowRef, watch, type Ref } from 'vue'
import { useFlowStore } from '../stores/flow-store'
import { usePyodideStore } from '../stores/pyodide-store'
import type { IRow, IMutField } from '../components/nodes/exploreData/interfaces'

export interface VisualData {
  fields: IMutField[]
  data: IRow[]
  rowInfo: Record<string, unknown> | null
}

const cache = new Map<string, Promise<VisualData>>()

/** Drop cached dataset payloads (e.g. after a catalog table is re-uploaded). */
export function clearVisualDataCache(name?: string) {
  if (name) cache.delete(name)
  else cache.clear()
}

async function fetchVisualData(name: string): Promise<VisualData> {
  const pyodide = usePyodideStore()
  const flow = useFlowStore()
  if (!pyodide.isReady) await pyodide.initialize()
  const res = await flow.loadDatasetForVisual(name)
  if (!res.success) throw new Error(res.error || 'Failed to load dataset')
  return {
    fields: (res.fields ?? []) as unknown as IMutField[],
    data: (res.data ?? []) as unknown as IRow[],
    rowInfo: (res.rowInfo ?? null) as Record<string, unknown> | null,
  }
}

function loadCached(name: string): Promise<VisualData> {
  let p = cache.get(name)
  if (!p) {
    p = fetchVisualData(name)
    cache.set(name, p)
    // Drop on failure so a later retry re-fetches instead of re-throwing.
    p.catch(() => cache.delete(name))
  }
  return p
}

type NameSource = Ref<string | null | undefined> | (() => string | null | undefined)

export function useVisualData(source: NameSource) {
  const getName = typeof source === 'function' ? source : () => source.value
  // shallowRef: data can hold up to 100k rows — deep reactivity would be costly
  // and pointless (GraphicWalker reads a toRaw snapshot).
  const fields = shallowRef<IMutField[]>([])
  const data = shallowRef<IRow[]>([])
  const rowInfo = ref<Record<string, unknown> | null>(null)
  const loading = ref(false)
  const error = ref<string | null>(null)

  async function load() {
    const name = getName()
    error.value = null
    if (!name) {
      fields.value = []
      data.value = []
      rowInfo.value = null
      return
    }
    loading.value = true
    try {
      const vd = await loadCached(name)
      if (getName() !== name) return // dataset switched mid-flight
      fields.value = vd.fields
      data.value = vd.data
      rowInfo.value = vd.rowInfo
    } catch (e) {
      if (getName() !== name) return
      error.value = e instanceof Error ? e.message : String(e)
      fields.value = []
      data.value = []
      rowInfo.value = null
    } finally {
      if (getName() === name) loading.value = false
    }
  }

  watch(getName, load, { immediate: true })

  return { fields, data, rowInfo, loading, error, reload: load }
}
