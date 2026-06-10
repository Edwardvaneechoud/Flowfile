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
