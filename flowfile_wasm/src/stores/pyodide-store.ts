import { defineStore } from 'pinia'
import { ref, shallowRef } from 'vue'

// The Python execution engine is a real package under src/pyodide/engine/. Vite
// inlines each module's source as a string (?raw); at runtime we write them into
// Pyodide's virtual filesystem and `from engine import *` (see setupExecutionEngine).
const engineFiles = import.meta.glob('../pyodide/engine/*.py', {
  as: 'raw',
  eager: true,
}) as Record<string, string>

declare global {
  interface Window {
    loadPyodide: (config?: any) => Promise<any>
  }
}

export const usePyodideStore = defineStore('pyodide', () => {
  const pyodide = shallowRef<any>(null)
  const isReady = ref(false)
  const isLoading = ref(false)
  const loadingStatus = ref('')
  const error = ref<string | null>(null)

  async function initialize() {
    if (isReady.value || isLoading.value) {
      return
    }

    isLoading.value = true
    error.value = null

    try {
      loadingStatus.value = 'Loading Pyodide...'

      // Load Pyodide from CDN - using v0.27.7 which is the last version with Polars support
      const script = document.createElement('script')
      script.src = 'https://cdn.jsdelivr.net/pyodide/v0.27.7/full/pyodide.js'
      document.head.appendChild(script)

      await new Promise<void>((resolve, reject) => {
        script.onload = () => resolve()
        script.onerror = () => reject(new Error('Failed to load Pyodide script'))
      })

      loadingStatus.value = 'Initializing Python runtime...'
      pyodide.value = await window.loadPyodide({
        indexURL: 'https://cdn.jsdelivr.net/pyodide/v0.27.7/full/'
      })

      loadingStatus.value = 'Installing packages...'
      // Load polars and pydantic - numpy is avoided by using native Polars rows() method
      await pyodide.value.loadPackage(['polars', 'pydantic'])

      loadingStatus.value = 'Setting up execution engine...'
      await setupExecutionEngine()

      isReady.value = true
      loadingStatus.value = 'Ready'
    } catch (err) {
      error.value = err instanceof Error ? err.message : 'Failed to initialize Pyodide'
      console.error('Pyodide initialization error:', err)
    } finally {
      isLoading.value = false
    }
  }

  async function setupExecutionEngine() {
    // Materialize the engine package into Pyodide's virtual FS, then import it.
    const FS = pyodide.value.FS
    const root = '/flowfile_engine'
    for (const dir of [root, `${root}/engine`]) {
      try {
        FS.mkdir(dir)
      } catch {
        // already exists (engine re-init)
      }
    }
    for (const [path, source] of Object.entries(engineFiles)) {
      const name = path.slice(path.lastIndexOf('/') + 1)
      FS.writeFile(`${root}/engine/${name}`, source, { encoding: 'utf8' })
    }
    await pyodide.value.runPythonAsync(`
import sys
if ${JSON.stringify(root)} not in sys.path:
    sys.path.insert(0, ${JSON.stringify(root)})
import engine

# The flow-store bridge was written against the original single-module engine,
# where the whole namespace — public functions, internal stores (e.g. _lazyframes)
# and imported modules (e.g. gc) — lived in Pyodide's globals. Re-expose that flat
# namespace from the package so those bare references keep resolving.
for _mod in [m for n, m in sys.modules.items() if n == "engine" or n.startswith("engine.")]:
    for _key, _val in vars(_mod).items():
        if not _key.startswith("__"):
            globals()[_key] = _val
del _mod, _key, _val
`)
  }

  async function runPython(code: string): Promise<any> {
    if (!isReady.value) {
      throw new Error('Pyodide is not ready')
    }
    return await pyodide.value.runPythonAsync(code)
  }

  async function runPythonWithResult(code: string): Promise<any> {
    if (!isReady.value) {
      throw new Error('Pyodide is not ready')
    }

    const rawResult = await pyodide.value.runPythonAsync(code)
    if (!rawResult?.toJs) {
      return rawResult
    }

    const jsResult = rawResult.toJs({ dict_converter: Object.fromEntries })

    function deepConvert(obj: any): any {
      if (obj instanceof Map) {
        const result: Record<string, any> = {}
        obj.forEach((value: any, key: string) => {
          result[key] = deepConvert(value)
        })
        return result
      }
      if (Array.isArray(obj)) {
        return obj.map(deepConvert)
      }
      if (obj && typeof obj === 'object' && obj.constructor === Object) {
        const result: Record<string, any> = {}
        for (const [key, value] of Object.entries(obj)) {
          result[key] = deepConvert(value)
        }
        return result
      }
      return obj
    }

    return deepConvert(jsResult)
  }

  function setGlobal(name: string, value: unknown): void {
    if (!isReady.value) {
      throw new Error('Pyodide is not ready')
    }
    pyodide.value.globals.set(name, value)
  }

  function deleteGlobal(name: string): void {
    if (!isReady.value) {
      throw new Error('Pyodide is not ready')
    }
    pyodide.value.globals.delete(name)
  }

  return {
    pyodide,
    isReady,
    isLoading,
    loadingStatus,
    error,
    initialize,
    runPython,
    runPythonWithResult,
    setGlobal,
    deleteGlobal
  }
})