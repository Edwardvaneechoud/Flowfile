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
  /** name → 'loading' | 'ready' for lazily micropip-installed packages. */
  const packageStatus = ref<Record<string, 'loading' | 'ready'>>({})
  const installedPyPackages = new Set<string>()
  const inFlightInstalls = new Map<string, Promise<void>>()
  let micropipLoaded = false

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

  /**
   * Lazily micropip-install pure-Python packages (pinned, e.g. 'openpyxl==3.1.5').
   * Memoized per package; nothing here runs at boot — first use of an Excel
   * node pays the download. Needs network access to pypi.org / files.pythonhosted.org.
   */
  async function ensurePyPackages(packages: string[]): Promise<void> {
    if (!isReady.value) {
      throw new Error('Pyodide is not ready')
    }
    // Per-package in-flight dedup: concurrent callers (e.g. the sheet picker
    // and node execution) await the same install instead of double-installing
    const waits = packages
      .map(p => inFlightInstalls.get(p))
      .filter((p): p is Promise<void> => p !== undefined)
    if (waits.length) await Promise.all(waits)

    const missing = packages.filter(p => !installedPyPackages.has(p))
    if (missing.length === 0) return

    const install = (async () => {
      try {
        if (!micropipLoaded) {
          await pyodide.value.loadPackage('micropip')
          micropipLoaded = true
        }
        for (const pkg of missing) packageStatus.value = { ...packageStatus.value, [pkg]: 'loading' }
        const micropip = pyodide.value.pyimport('micropip')
        try {
          await micropip.install(missing)
        } finally {
          micropip.destroy()
        }
        for (const pkg of missing) {
          installedPyPackages.add(pkg)
          packageStatus.value = { ...packageStatus.value, [pkg]: 'ready' }
        }
      } catch (err) {
        for (const pkg of missing) {
          if (packageStatus.value[pkg] === 'loading') {
            const { [pkg]: _removed, ...rest } = packageStatus.value
            packageStatus.value = rest
          }
        }
        throw new Error(
          `Could not download ${missing.join(', ')} from PyPI — check your network connection, ` +
          `or your page's Content-Security-Policy (pypi.org, files.pythonhosted.org) if embedding. (${err})`
        )
      }
    })()
    for (const pkg of missing) inFlightInstalls.set(pkg, install)
    try {
      await install
    } finally {
      for (const pkg of missing) {
        if (inFlightInstalls.get(pkg) === install) inFlightInstalls.delete(pkg)
      }
    }
  }

  /**
   * Run Python returning raw bytes. The expression must evaluate to Python
   * `bytes` (or None) — bytes don't survive toJs()/deepConvert, so this copies
   * them out via the buffer protocol and releases the proxy.
   */
  async function runPythonGetBytes(code: string): Promise<Uint8Array | null> {
    if (!isReady.value) {
      throw new Error('Pyodide is not ready')
    }
    const proxy = await pyodide.value.runPythonAsync(code)
    if (proxy == null) return null
    if (!proxy.getBuffer) {
      // Plain JS value (shouldn't happen for bytes) — best effort
      return proxy instanceof Uint8Array ? proxy : null
    }
    // Always destroy the proxy — a leak here pins the full byte payload in
    // the wasm heap
    try {
      const buf = proxy.getBuffer('u8')
      try {
        return new Uint8Array(buf.data)
      } finally {
        buf.release()
      }
    } finally {
      proxy.destroy()
    }
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
    packageStatus,
    initialize,
    runPython,
    runPythonWithResult,
    runPythonGetBytes,
    ensurePyPackages,
    setGlobal,
    deleteGlobal
  }
})