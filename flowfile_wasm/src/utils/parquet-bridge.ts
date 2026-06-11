/**
 * Parquet ⇄ Arrow IPC stream conversion via parquet-wasm.
 *
 * The Polars wheel Pyodide bundles is compiled without parquet support, but
 * with IPC — so the Parquet codec lives here in JS and the engine only ever
 * sees Arrow IPC stream bytes (pl.read_ipc_stream / pl.write_ipc_stream).
 *
 * parquet-wasm is CDN-loaded on first use (like Pyodide itself): it is NOT an
 * npm dependency, nothing loads at boot, and CSV/Excel flows never fetch it.
 */

const PARQUET_WASM_VERSION = '0.7.1'
const CDN_BASE = `https://cdn.jsdelivr.net/npm/parquet-wasm@${PARQUET_WASM_VERSION}/esm/`
const CDN_URL = `${CDN_BASE}parquet_wasm.js`

// Bundler-opaque dynamic import: a bare https: dynamic-import call left in the
// published lib bundle hard-fails webpack5/esbuild embedders even if they
// never use Parquet, and pragma comments don't survive esbuild minify.
const dynamicImport = new Function('u', 'return import(u)') as (url: string) => Promise<any>

let modPromise: Promise<any> | null = null

export function getParquetWasm(): Promise<any> {
  if (!modPromise) {
    modPromise = (async () => {
      try {
        const mod = await dynamicImport(CDN_URL)
        await mod.default({ module_or_path: `${CDN_BASE}parquet_wasm_bg.wasm` })
        return mod
      } catch (err) {
        modPromise = null
        throw new Error(
          'Could not load the Parquet engine from cdn.jsdelivr.net — check your network ' +
          `connection, or your page's Content-Security-Policy if embedding. (${err})`
        )
      }
    })()
  }
  return modPromise
}

/** Decode Parquet file bytes into Arrow IPC stream bytes for pl.read_ipc_stream. */
export async function parquetToIpcStream(bytes: Uint8Array): Promise<Uint8Array> {
  const mod = await getParquetWasm()
  const table = mod.readParquet(bytes)
  // intoIPCStream consumes the table; free only if conversion fails
  try {
    return table.intoIPCStream()
  } catch (err) {
    table.free?.()
    throw err
  }
}

/** Encode Arrow IPC stream bytes (from pl.write_ipc_stream) into Parquet file bytes. */
export async function ipcStreamToParquet(ipc: Uint8Array): Promise<Uint8Array> {
  const mod = await getParquetWasm()
  const table = mod.Table.fromIPCStream(ipc)
  // writeParquet takes ownership of the table
  return mod.writeParquet(table)
}
