/*
 * Real-Pyodide smoke test for the in-browser execution engine.
 *
 * Boots an actual Pyodide (v0.27.7, the version the app loads), materializes the
 * `src/pyodide/engine/` package into Pyodide's virtual FS and runs the SAME
 * bootstrap as `pyodide-store.ts::setupExecutionEngine` (namespace dump), then
 * replays the bridge Python from `flow-store.ts`.
 *
 * Why this exists: the engine package is loaded via a flat-namespace contract
 * (the bridge calls bare names like `execute_*`, and references engine internals
 * like `_lazyframes` and the engine's imported `gc`). Unit tests import the
 * package under CPython and CANNOT catch a broken browser namespace/bootstrap —
 * only running the real bridge against real Pyodide does. This guards that.
 *
 * Run locally:  npm install --no-save pyodide@0.27.7 && node tests/pyodide-smoke/smoke.cjs
 * (pyodide is intentionally NOT a package dependency — it is CDN-loaded by the app.)
 */
const fs = require('fs');
const path = require('path');
const { loadPyodide } = require('pyodide');

const ENGINE_DIR = path.resolve(__dirname, '../../src/pyodide/engine');
const ROOT = '/flowfile_engine';

// Mirror flow-store.ts toPythonJson(): double-encode so json.loads(<literal>) works.
const j = (o) => JSON.stringify(JSON.stringify(o));

const failed = [];
let pyodide;

async function run(label, code) {
  try {
    const res = await pyodide.runPythonAsync(code);
    let value = res;
    if (res && typeof res.toJs === 'function') {
      value = res.toJs({ dict_converter: Object.fromEntries });
      if (typeof res.destroy === 'function') res.destroy();
    }
    console.log(`  [ok]   ${label}`);
    return value;
  } catch (e) {
    failed.push(label);
    console.error(`  [FAIL] ${label}: ${String(e).split('\n').filter(Boolean).slice(-2).join(' | ')}`);
    return null;
  }
}

async function main() {
  pyodide = await loadPyodide();
  await pyodide.loadPackage(['polars']);

  // Materialize the engine package into Pyodide's FS (as the browser does).
  const FS = pyodide.FS;
  for (const dir of [ROOT, `${ROOT}/engine`]) {
    try { FS.mkdir(dir); } catch (_) { /* exists */ }
  }
  for (const file of fs.readdirSync(ENGINE_DIR)) {
    if (file.endsWith('.py')) {
      FS.writeFile(`${ROOT}/engine/${file}`, fs.readFileSync(path.join(ENGINE_DIR, file), 'utf8'), { encoding: 'utf8' });
    }
  }

  // Bootstrap — the same logic as setupExecutionEngine().
  await pyodide.runPythonAsync(`
import sys
if "${ROOT}" not in sys.path:
    sys.path.insert(0, "${ROOT}")
import engine
for _mod in [m for n, m in sys.modules.items() if n == "engine" or n.startswith("engine.")]:
    for _key, _val in vars(_mod).items():
        if not _key.startswith("__"):
            globals()[_key] = _val
del _mod, _key, _val
`);
  console.log('  [ok]   bootstrap (FS write + namespace dump)');

  // --- bridge Python that depends on the flat namespace ---
  pyodide.globals.set('_temp_content', 'id,name,age\n1,alice,30\n2,bob,25\n3,carol,40\n');
  await run('execute_read_csv', `
import json
execute_read_csv(1, _temp_content, json.loads(${j({ received_file: { table_settings: { has_headers: true, delimiter: ',' } } })}))
`);

  // flow-store.ts:1066 — orphan cleanup: bare _lazyframes + gc + clear_node (the crash).
  await run('orphan-cleanup (_lazyframes + gc + clear_node)', `
current_node_ids = {1}
orphaned_ids = [nid for nid in list(_lazyframes.keys()) if nid not in current_node_ids]
for nid in orphaned_ids:
    clear_node(nid)
gc.collect()
`);

  // flow-store.ts:1766 — bare _lazyframes again.
  const ids = await run('list(_lazyframes.keys())', 'list(_lazyframes.keys())');
  console.log('         registered ids =', JSON.stringify(ids));

  await run('execute_filter', `
import json
execute_filter(2, 1, json.loads(${j({ filter_input: { basic_filter: { field: 'age', operator: 'greater_than', value: '28' } } })}))
`);
  const out = await run('execute_output', `
import json
_r = execute_output(3, 2, json.loads(${j({ output_settings: { name: 'o.csv', table_settings: { delimiter: ',' } } })}))
_r["download"]["content"] if _r.get("success") else ("ERR:" + _r.get("error", ""))
`);
  console.log('         output rows =', JSON.stringify(out));

  // Other bridge entry points that cross module boundaries.
  await run('fetch_preview', 'fetch_preview(1, 100)');
  await run('propagate_schemas', `
import json
propagate_schemas(
    json.loads(${j({ order: [1, 2], nodes: { 1: { type: 'read' }, 2: { type: 'filter', input_ids: [1], left: 1, settings: { filter_input: { basic_filter: { field: 'age', operator: 'greater_than', value: '28' } } } } } })}),
    json.loads(${j({ 1: [{ name: 'id', data_type: 'Int64' }, { name: 'name', data_type: 'String' }, { name: 'age', data_type: 'Int64' }] })}),
)
`);

  // flow-store.ts:2248 — bare gc.collect().
  await run('gc.collect()', 'gc.collect()');

  // --- binary bridge (Phase 0 of Open Any File) ---
  // JS -> Python: Uint8Array crosses as a JsBuffer proxy; bridge strings call .to_py().
  pyodide.globals.set('_temp_bytes', new Uint8Array([0x50, 0x4b, 3, 4, 255]));
  const byteLen = await run('_temp_bytes.to_py() length', 'len(_temp_bytes.to_py())');
  if (byteLen !== 5) {
    failed.push('_temp_bytes length mismatch');
    console.error(`  [FAIL] _temp_bytes length mismatch: ${byteLen}`);
  }
  pyodide.globals.delete('_temp_bytes');

  // Python -> JS: bytes staged in the registry, pulled once via getBuffer('u8').
  await run('seed _output_binaries', '_output_binaries[99] = bytes([1, 2, 3, 4])');
  try {
    const proxy = await pyodide.runPythonAsync('take_output_binary(99)');
    const buf = proxy.getBuffer('u8');
    const bytes = new Uint8Array(buf.data);
    buf.release();
    proxy.destroy();
    const second = await pyodide.runPythonAsync('take_output_binary(99) is None');
    if (bytes.length === 4 && bytes[3] === 4 && second === true) {
      console.log('  [ok]   take_output_binary (getBuffer round-trip, pop semantics)');
    } else {
      failed.push('take_output_binary');
      console.error(`  [FAIL] take_output_binary: len=${bytes.length} second=${second}`);
    }
  } catch (e) {
    failed.push('take_output_binary');
    console.error(`  [FAIL] take_output_binary: ${e}`);
  }

  // --- Excel read path (Phase 1) — pinned micropip install + exact bridge replay ---
  await pyodide.loadPackage('micropip');
  await run('micropip install openpyxl (pinned)', `
import micropip
await micropip.install(['openpyxl==3.1.5'])
`);

  // Build an xlsx inside Pyodide, round-trip it through Node (getBuffer), then
  // push it back as the _temp_bytes global the bridge strings reference.
  const xlsxProxy = await pyodide.runPythonAsync(`
import io
import openpyxl
_wb = openpyxl.Workbook()
_ws = _wb.active
_ws.title = "people"
for _row in (["name", "age"], ["alice", 30], ["bob", 25]):
    _ws.append(_row)
_bio = io.BytesIO()
_wb.save(_bio)
_bio.getvalue()
`);
  const xlsxBuf = xlsxProxy.getBuffer('u8');
  const xlsxBytes = new Uint8Array(xlsxBuf.data);
  xlsxBuf.release();
  xlsxProxy.destroy();
  pyodide.globals.set('_temp_bytes', xlsxBytes);

  const sheetRes = await run('list_excel_sheets', 'list_excel_sheets(_temp_bytes.to_py())');
  if (!sheetRes || sheetRes.success !== true || String(sheetRes.sheets) !== 'people') {
    failed.push('list_excel_sheets result');
    console.error(`  [FAIL] list_excel_sheets result: ${JSON.stringify(sheetRes)}`);
  }

  const excelRes = await run('execute_read_excel', `
import json
execute_read_excel(10, _temp_bytes.to_py(), json.loads(${j({ received_file: { file_type: 'excel', table_settings: { file_type: 'excel', sheet_name: null, has_headers: true } } })}))
`);
  if (!excelRes || excelRes.success !== true) {
    failed.push('execute_read_excel result');
    console.error(`  [FAIL] execute_read_excel result: ${JSON.stringify(excelRes)}`);
  }
  pyodide.globals.delete('_temp_bytes');

  // --- Excel write path (Phase 2) — output bridge + binary pull, exactly as flow-store does.
  await run('micropip install xlsxwriter (pinned)', `
import micropip
await micropip.install(['XlsxWriter==3.2.0'])
`);
  const outRes = await run('execute_output (excel)', `
import json
execute_output(11, 10, json.loads(${j({ output_settings: { name: 'out.xlsx', file_type: 'excel', table_settings: { file_type: 'excel', sheet_name: 'Data' } } })}))
`);
  if (!outRes || outRes.success !== true || outRes.download?.content_kind !== 'binary') {
    failed.push('execute_output excel result');
    console.error(`  [FAIL] execute_output excel result: ${JSON.stringify(outRes)}`);
  } else {
    const outProxy = await pyodide.runPythonAsync('take_output_binary(11)');
    const outBuf = outProxy.getBuffer('u8');
    const outBytes = new Uint8Array(outBuf.data);
    outBuf.release();
    outProxy.destroy();
    if (outBytes.length > 100 && outBytes[0] === 0x50 && outBytes[1] === 0x4b) {
      console.log('  [ok]   take_output_binary (xlsx PK magic)');
    } else {
      failed.push('xlsx output bytes');
      console.error(`  [FAIL] xlsx output bytes: len=${outBytes.length}`);
    }
  }

  // --- Parquet chain (Phase 3) — engine IPC staging ⇄ parquet-wasm, both directions.
  // Uses parquet-wasm's esm build (the SAME artifact the browser CDN-loads),
  // initialized from the locally installed wasm. Requires:
  //   npm install --no-save parquet-wasm@0.7.1
  let pqMod = null;
  try {
    require.resolve('parquet-wasm/esm/parquet_wasm.js');
    pqMod = await import('parquet-wasm/esm');
    await pqMod.default({ module_or_path: fs.readFileSync(require.resolve('parquet-wasm/esm/parquet_wasm_bg.wasm')) });
  } catch (e) {
    failed.push('parquet-wasm load');
    console.error(`  [FAIL] parquet-wasm load (npm install --no-save parquet-wasm@0.7.1): ${e}`);
  }

  if (pqMod) {
    const pqOut = await run('execute_output (parquet → IPC staging)', `
import json
execute_output(12, 10, json.loads(${j({ output_settings: { name: 'out.parquet', file_type: 'parquet' } })}))
`);
    if (!pqOut || pqOut.success !== true || pqOut.download?.transport !== 'arrow-ipc') {
      failed.push('execute_output parquet result');
      console.error(`  [FAIL] execute_output parquet result: ${JSON.stringify(pqOut)}`);
    } else {
      try {
        const ipcProxy = await pyodide.runPythonAsync('take_output_binary(12)');
        const ipcBuf = ipcProxy.getBuffer('u8');
        const ipcBytes = new Uint8Array(ipcBuf.data);
        ipcBuf.release();
        ipcProxy.destroy();

        // IPC -> parquet (write path, consumes the table)
        const table = pqMod.Table.fromIPCStream(ipcBytes);
        const parquetBytes = pqMod.writeParquet(table);
        if (!(parquetBytes.length > 8 && parquetBytes[0] === 0x50 && parquetBytes[1] === 0x41)) {
          throw new Error(`bad PAR1 magic, len=${parquetBytes.length}`);
        }
        console.log('  [ok]   ipc -> writeParquet (PAR1 magic)');

        // parquet -> IPC -> engine (read path)
        const roundTripIpc = pqMod.readParquet(parquetBytes).intoIPCStream();
        pyodide.globals.set('_temp_bytes', roundTripIpc);
        const ipcRead = await run('execute_read_ipc', `
import json
execute_read_ipc(13, _temp_bytes.to_py(), json.loads("{}"))
`);
        pyodide.globals.delete('_temp_bytes');
        if (!ipcRead || ipcRead.success !== true) {
          throw new Error(`execute_read_ipc: ${JSON.stringify(ipcRead)}`);
        }

        // Categorical columns (dictionary-encoded IPC inputs) hit a different
        // wasm-only panic path in the view->classic converter — the export
        // cleaner must rebuild them too. Verified at BOTH export sites.
        const catOk = await run('categorical export survives', `
import io
import polars as pl
_cdf = pl.DataFrame({"cat": ["x", "y", "x"], "n": [1, 2, 3]}).cast({"cat": pl.Categorical})
_cb = io.BytesIO()
_cdf.write_ipc_stream(_cb, compat_level=pl.CompatLevel.oldest())
_r1 = execute_read_ipc(14, _cb.getvalue(), {})
_r2 = execute_output(15, 14, {"output_settings": {"name": "c.parquet", "file_type": "parquet"}})
_arrow = get_node_arrow(14)
bool(_r1["success"] and _r2["success"] and _arrow is not None and take_output_binary(15) is not None)
`);
        if (catOk !== true) {
          throw new Error(`categorical export: ${JSON.stringify(catOk)}`);
        }

        // Host pull API (Phase 5): get_node_arrow -> IPC bytes parseable by parquet-wasm.
        const arrowProxy = await pyodide.runPythonAsync('get_node_arrow(13)');
        const arrowBuf = arrowProxy.getBuffer('u8');
        const arrowBytes = new Uint8Array(arrowBuf.data);
        arrowBuf.release();
        arrowProxy.destroy();
        const hostTable = pqMod.Table.fromIPCStream(arrowBytes);
        hostTable.free?.();
        console.log('  [ok]   get_node_arrow (host-parseable IPC)');
      } catch (e) {
        failed.push('parquet chain');
        console.error(`  [FAIL] parquet chain: ${e}`);
      }
    }
  }

  if (failed.length) {
    console.error(`\nFAILED (${failed.length}): ${failed.join(', ')}`);
    process.exit(1);
  }
  console.log('\nAll Pyodide bridge smoke checks passed.');
}

main().catch((e) => {
  console.error('SMOKE ERROR:', e);
  process.exit(1);
});
