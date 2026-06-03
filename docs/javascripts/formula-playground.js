/*
 * Flowfile formula playground — an in-page, client-side runner for the
 * `[column]` formula syntax. Runs entirely in the browser via Pyodide
 * (CPython -> WebAssembly); no backend required.
 *
 * It lazy-loads the multi-MB Pyodide runtime only on the first "Run" click,
 * so it costs nothing on page view. It loads Polars + Pydantic as Pyodide
 * packages and installs the pure-Python `polars-expr-transformer` engine via
 * micropip.
 *
 * Activated on any element with class "formula-playground". Optional data-*:
 *   data-formula : starter formula (default "[price] * [quantity]")
 *   data-package : pip requirement for the engine (default below)
 */
(function () {
  "use strict";

  // The patched engine that makes `polars-ds` optional must be on PyPI for this
  // to resolve. Until then the widget shows a friendly message.
  var DEFAULT_PACKAGE = "polars-expr-transformer>=0.6.0";
  var PYODIDE_VERSION = "0.27.7"; // last Pyodide with a Polars wheel
  var PYODIDE_INDEX = "https://cdn.jsdelivr.net/pyodide/v" + PYODIDE_VERSION + "/full/";

  // Sample dataset shown in the playground (kept in sync with the docs examples).
  var SAMPLE = {
    product: ["Widget", "Gadget", "Tool"],
    price: [10.5, 25.0, 8.75],
    quantity: [100, 50, 200],
    discount: [0.1, 0.15, 0.05],
  };

  var pyodideReady = null; // shared promise across all widgets on the page

  function el(tag, cls, text) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text != null) e.textContent = text;
    return e;
  }

  function loadScript(src) {
    return new Promise(function (resolve, reject) {
      var s = document.createElement("script");
      s.src = src;
      s.onload = function () { resolve(); };
      s.onerror = function () { reject(new Error("Failed to load " + src)); };
      document.head.appendChild(s);
    });
  }

  // Boot Pyodide + Polars + the formula engine exactly once.
  function ensurePyodide(pkg, onStatus) {
    if (pyodideReady) return pyodideReady;
    pyodideReady = (async function () {
      onStatus("Loading Python runtime…");
      if (!window.loadPyodide) await loadScript(PYODIDE_INDEX + "pyodide.js");
      var py = await window.loadPyodide({ indexURL: PYODIDE_INDEX });

      onStatus("Loading Polars…");
      await py.loadPackage(["micropip", "polars", "pydantic"]);

      onStatus("Installing formula engine…");
      var micropip = py.pyimport("micropip");
      // deps=False: Polars + Pydantic already loaded as Pyodide packages, and the
      // optional native `polars-ds` is intentionally skipped.
      await micropip.install(pkg, false, false);

      onStatus("Preparing sample data…");
      py.globals.set("__sample_json", JSON.stringify(SAMPLE));
      await py.runPythonAsync(
        "import json, polars as pl\n" +
        "from polars_expr_transformer import simple_function_to_expr as _f\n" +
        "_df = pl.DataFrame(json.loads(__sample_json))\n"
      );
      return py;
    })().catch(function (e) {
      pyodideReady = null; // allow retry on next click
      throw e;
    });
    return pyodideReady;
  }

  async function runFormula(py, formula) {
    py.globals.set("__formula", formula);
    var out = await py.runPythonAsync(
      "import json\n" +
      "try:\n" +
      "    _res = _df.with_columns(_f(__formula).alias('result'))\n" +
      "    _payload = {'ok': True, 'columns': _res.columns, 'rows': _res.rows()}\n" +
      "except Exception as _e:\n" +
      "    _payload = {'ok': False, 'error': type(_e).__name__ + ': ' + str(_e)}\n" +
      "json.dumps(_payload, default=str)\n"
    );
    return JSON.parse(out);
  }

  function renderTable(container, columns, rows) {
    container.innerHTML = "";
    var table = el("table", "fp-table");
    var thead = el("thead");
    var htr = el("tr");
    columns.forEach(function (c) { htr.appendChild(el("th", null, c)); });
    thead.appendChild(htr);
    table.appendChild(thead);
    var tbody = el("tbody");
    rows.forEach(function (row) {
      var tr = el("tr");
      row.forEach(function (cell) { tr.appendChild(el("td", null, String(cell))); });
      tbody.appendChild(tr);
    });
    table.appendChild(tbody);
    container.appendChild(table);
  }

  function initWidget(root) {
    if (root.dataset.fpInit) return;
    root.dataset.fpInit = "1";

    var pkg = root.dataset.package || DEFAULT_PACKAGE;
    var starter = root.dataset.formula || "[price] * [quantity]";

    root.appendChild(el("div", "fp-label", "Sample data"));
    var sampleBox = el("div", "fp-sample");
    renderTable(sampleBox, Object.keys(SAMPLE),
      SAMPLE.product.map(function (_, i) {
        return Object.keys(SAMPLE).map(function (k) { return SAMPLE[k][i]; });
      }));
    root.appendChild(sampleBox);

    var label = el("label", "fp-label", "Formula (creates a `result` column)");
    root.appendChild(label);

    var input = el("input", "fp-input");
    input.type = "text";
    input.value = starter;
    input.spellcheck = false;
    input.setAttribute("aria-label", "Formula");
    root.appendChild(input);

    var bar = el("div", "fp-bar");
    var runBtn = el("button", "fp-run", "Run ▸");
    bar.appendChild(runBtn);
    var status = el("span", "fp-status", "");
    bar.appendChild(status);
    root.appendChild(bar);

    var output = el("div", "fp-output");
    root.appendChild(output);

    function setStatus(msg) { status.textContent = msg || ""; }

    async function run() {
      runBtn.disabled = true;
      output.innerHTML = "";
      try {
        var py = await ensurePyodide(pkg, setStatus);
        setStatus("Running…");
        var res = await runFormula(py, input.value);
        setStatus("");
        if (res.ok) {
          renderTable(output, res.columns, res.rows);
        } else {
          var err = el("div", "fp-error", res.error);
          output.appendChild(err);
        }
      } catch (e) {
        setStatus("");
        var box = el("div", "fp-error");
        box.textContent =
          "Could not start the playground: " + (e && e.message ? e.message : e) +
          "  (the live playground needs polars-expr-transformer ≥ 0.6.0 published to PyPI).";
        output.appendChild(box);
      } finally {
        runBtn.disabled = false;
      }
    }

    runBtn.addEventListener("click", run);
    input.addEventListener("keydown", function (ev) {
      if (ev.key === "Enter") { ev.preventDefault(); run(); }
    });
  }

  function initAll() {
    var nodes = document.querySelectorAll(".formula-playground");
    Array.prototype.forEach.call(nodes, initWidget);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initAll);
  } else {
    initAll();
  }
})();
