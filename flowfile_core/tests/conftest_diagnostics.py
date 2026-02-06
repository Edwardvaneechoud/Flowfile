"""
Pytest diagnostics plugin for investigating test performance slowdowns.

Activated ONLY when the environment variable PYTEST_DIAGNOSTICS=1 is set.

This plugin tracks:
  - Imports of flowfile_core.kernel.* modules (when, by whom, how long)
  - Test collection timing per file
  - Fixture setup/teardown timing
  - Collected vs deselected test counts (especially kernel-marked tests)
  - Summary report at session end

Usage:
    PYTEST_DIAGNOSTICS=1 poetry run pytest flowfile_core/tests -m "not kernel" ...
"""

import os
import sys
import time
import traceback

_ENABLED = os.environ.get("PYTEST_DIAGNOSTICS") == "1"

# ── State shared across hooks ────────────────────────────────────────────────

_kernel_imports: list[dict] = []  # {"module", "when", "traceback", "elapsed_s"}
_collection_timing: dict[str, dict] = {}  # file -> {"count", "deselected", "elapsed_s"}
_fixture_timing: list[dict] = []  # {"name", "elapsed_s", "phase"}
_test_timing: list[dict] = []  # {"nodeid", "setup_s", "call_s", "teardown_s"}
_session_start: float = 0.0
_collection_start: float = 0.0
_collection_end: float = 0.0
_phase = "unknown"  # "import", "collection", "execution", "teardown"

# Per-test accumulators
_current_test_times: dict[str, dict[str, float]] = {}

# ── Import interceptor ───────────────────────────────────────────────────────


class _KernelImportTracer:
    """
    A sys.meta_path finder that does NOT actually find/load anything.
    It only observes when modules matching flowfile_core.kernel* are imported
    and records the context (call stack, phase, timing).
    """

    _active_imports: set = set()  # prevent re-entrant logging

    def find_module(self, fullname, path=None):
        if not fullname.startswith("flowfile_core.kernel"):
            return None
        if fullname in self._active_imports:
            return None
        # Only record if the module is not yet loaded (first import)
        if fullname in sys.modules:
            return None
        self._active_imports.add(fullname)
        t0 = time.monotonic()
        # Let normal import machinery do the work – we just record the event
        # Return None so the next finder handles it.  We record from here because
        # by the time find_module returns, the module will be loaded by another
        # finder and we lose the stack.
        tb_lines = traceback.format_stack()
        # Trim the last 2 frames (this method + importlib internals)
        tb_text = "".join(tb_lines[:-2]).strip()

        # We cannot measure the import time from find_module alone because we
        # return None.  Instead, register an atexit-style callback in the import
        # machinery.  For simplicity, just record the wall time of this call.
        _kernel_imports.append(
            {
                "module": fullname,
                "when": _phase,
                "traceback": tb_text,
                "timestamp": time.monotonic(),
            }
        )
        self._active_imports.discard(fullname)
        return None


_tracer = _KernelImportTracer()

# ── Pytest hooks ─────────────────────────────────────────────────────────────


def pytest_configure(config):
    """Install the import tracer early so we catch collection-phase imports."""
    if not _ENABLED:
        return
    global _session_start, _phase
    _session_start = time.monotonic()
    _phase = "configure"
    # Insert our tracer at the very front of sys.meta_path
    if _tracer not in sys.meta_path:
        sys.meta_path.insert(0, _tracer)
    _log("Diagnostics plugin activated")

    # Snapshot which kernel modules are already loaded
    already = [m for m in sys.modules if m.startswith("flowfile_core.kernel")]
    if already:
        _log(f"Kernel modules already in sys.modules at configure time: {already}")
        for m in already:
            _kernel_imports.append(
                {
                    "module": m,
                    "when": "pre-configure (already loaded)",
                    "traceback": "(loaded before diagnostics plugin activated)",
                    "timestamp": 0.0,
                }
            )


def pytest_collection(session):
    if not _ENABLED:
        return
    global _collection_start, _phase
    _phase = "collection"
    _collection_start = time.monotonic()


def pytest_collection_modifyitems(config, items):
    """Log collected/deselected items per file, especially kernel-marked ones."""
    if not _ENABLED:
        return
    global _collection_end, _phase
    _collection_end = time.monotonic()
    _phase = "post-collection"

    # Build per-file stats
    file_stats: dict[str, dict] = {}
    for item in items:
        fspath = str(item.fspath)
        if fspath not in file_stats:
            file_stats[fspath] = {"collected": 0, "kernel_marked": 0}
        file_stats[fspath]["collected"] += 1
        # Check if item has the kernel marker
        if item.get_closest_marker("kernel"):
            file_stats[fspath]["kernel_marked"] += 1

    for fspath, stats in file_stats.items():
        _collection_timing[fspath] = stats

    total = len(items)
    kernel_count = sum(s["kernel_marked"] for s in file_stats.values())
    _log(
        f"Collection complete: {total} items across {len(file_stats)} files, "
        f"{kernel_count} with kernel marker"
    )


def pytest_deselected(items):
    """Track deselected items (e.g., kernel-marked tests excluded by -m 'not kernel')."""
    if not _ENABLED:
        return
    for item in items:
        fspath = str(item.fspath)
        if fspath not in _collection_timing:
            _collection_timing[fspath] = {"collected": 0, "kernel_marked": 0}
        _collection_timing[fspath].setdefault("deselected", 0)
        _collection_timing[fspath]["deselected"] += 1


def pytest_runtest_setup(item):
    if not _ENABLED:
        return
    global _phase
    _phase = "execution"
    _current_test_times[item.nodeid] = {"setup_start": time.monotonic()}


def pytest_runtest_call(item):
    if not _ENABLED:
        return
    times = _current_test_times.get(item.nodeid, {})
    times["call_start"] = time.monotonic()
    times["setup_end"] = time.monotonic()


def pytest_runtest_teardown(item, nextitem):
    if not _ENABLED:
        return
    times = _current_test_times.get(item.nodeid, {})
    times["teardown_start"] = time.monotonic()


def pytest_runtest_makereport(item, call):
    """Capture timing for each phase of a test."""
    if not _ENABLED:
        return
    times = _current_test_times.get(item.nodeid, {})
    if call.when == "setup":
        times["setup_duration"] = call.duration
    elif call.when == "call":
        times["call_duration"] = call.duration
    elif call.when == "teardown":
        times["teardown_duration"] = call.duration
        # Test is fully done – record summary
        _test_timing.append(
            {
                "nodeid": item.nodeid,
                "setup_s": times.get("setup_duration", 0.0),
                "call_s": times.get("call_duration", 0.0),
                "teardown_s": times.get("teardown_duration", 0.0),
            }
        )


def pytest_sessionfinish(session, exitstatus):
    if not _ENABLED:
        return
    global _phase
    _phase = "finished"

    # Remove our tracer
    if _tracer in sys.meta_path:
        sys.meta_path.remove(_tracer)

    session_elapsed = time.monotonic() - _session_start
    collection_elapsed = _collection_end - _collection_start if _collection_start else 0.0

    # Also do a final scan of sys.modules for kernel modules
    final_kernel_modules = sorted(m for m in sys.modules if m.startswith("flowfile_core.kernel"))

    # ── Print report ─────────────────────────────────────────────────────

    lines: list[str] = []
    lines.append("")
    lines.append("=" * 72)
    lines.append("  DIAGNOSTICS SUMMARY")
    lines.append("=" * 72)
    lines.append("")

    # Section 1: Kernel module imports
    lines.append("── Kernel Module Imports ──")
    if _kernel_imports:
        for entry in _kernel_imports:
            ts = (
                f"+{entry['timestamp'] - _session_start:.3f}s"
                if entry["timestamp"] > 0
                else "(before plugin)"
            )
            lines.append(f"  {entry['module']}")
            lines.append(f"    Phase: {entry['when']}  |  Time: {ts}")
            # Show abbreviated traceback (last 8 meaningful lines)
            tb = entry["traceback"]
            if tb and tb != "(loaded before diagnostics plugin activated)":
                tb_lines = tb.splitlines()
                # Show at most 10 lines from the end of the traceback
                show = tb_lines[-10:] if len(tb_lines) > 10 else tb_lines
                for tl in show:
                    lines.append(f"    {tl}")
            else:
                lines.append(f"    {tb}")
            lines.append("")
    else:
        lines.append("  (none detected during this session)")
        lines.append("")

    lines.append(f"  Final kernel modules in sys.modules ({len(final_kernel_modules)}):")
    for m in final_kernel_modules:
        lines.append(f"    - {m}")
    lines.append("")

    # Section 2: Collection timing
    lines.append("── Test Collection ──")
    lines.append(f"  Total collection time: {collection_elapsed:.2f}s")
    lines.append("")
    total_collected = 0
    total_deselected = 0
    total_kernel_marked = 0
    for fspath, stats in sorted(_collection_timing.items()):
        collected = stats.get("collected", 0)
        deselected = stats.get("deselected", 0)
        kernel_marked = stats.get("kernel_marked", 0)
        total_collected += collected
        total_deselected += deselected
        total_kernel_marked += kernel_marked
        # Shorten path for readability
        short = fspath.split("flowfile_core/tests/")[-1] if "flowfile_core/tests/" in fspath else fspath
        parts = [f"collected {collected}"]
        if deselected:
            parts.append(f"deselected {deselected}")
        if kernel_marked:
            parts.append(f"kernel-marked {kernel_marked}")
        lines.append(f"  {short}: {', '.join(parts)}")
    lines.append("")
    lines.append(
        f"  Totals: {total_collected} collected, {total_deselected} deselected, "
        f"{total_kernel_marked} kernel-marked"
    )
    lines.append(f"  Tests executed: {total_collected - total_deselected}")
    lines.append("")

    # Section 3: Slowest tests (by total time)
    lines.append("── Top 30 Slowest Tests (setup + call + teardown) ──")
    sorted_tests = sorted(
        _test_timing,
        key=lambda t: t["setup_s"] + t["call_s"] + t["teardown_s"],
        reverse=True,
    )
    for i, t in enumerate(sorted_tests[:30], 1):
        total = t["setup_s"] + t["call_s"] + t["teardown_s"]
        lines.append(
            f"  {i:2d}. {total:7.2f}s "
            f"(setup={t['setup_s']:.2f}s call={t['call_s']:.2f}s teardown={t['teardown_s']:.2f}s)  "
            f"{t['nodeid']}"
        )
    lines.append("")

    # Section 4: Slowest setup times
    lines.append("── Top 15 Slowest Test Setups ──")
    sorted_by_setup = sorted(_test_timing, key=lambda t: t["setup_s"], reverse=True)
    for i, t in enumerate(sorted_by_setup[:15], 1):
        lines.append(f"  {i:2d}. {t['setup_s']:7.2f}s  {t['nodeid']}")
    lines.append("")

    # Section 5: Aggregate fixture timing (setup+teardown totals)
    total_setup = sum(t["setup_s"] for t in _test_timing)
    total_call = sum(t["call_s"] for t in _test_timing)
    total_teardown = sum(t["teardown_s"] for t in _test_timing)
    lines.append("── Aggregate Timing ──")
    lines.append(f"  Session wall time:    {session_elapsed:.2f}s")
    lines.append(f"  Collection time:      {collection_elapsed:.2f}s")
    lines.append(f"  Total setup time:     {total_setup:.2f}s")
    lines.append(f"  Total call time:      {total_call:.2f}s")
    lines.append(f"  Total teardown time:  {total_teardown:.2f}s")
    lines.append(
        f"  Overhead (session - collection - sum of tests): "
        f"{session_elapsed - collection_elapsed - total_setup - total_call - total_teardown:.2f}s"
    )
    lines.append("")

    # Section 6: Check for docker/httpx import overhead
    lines.append("── Heavy Library Import Check ──")
    for lib_name in ("docker", "httpx", "uvicorn"):
        if lib_name in sys.modules:
            lines.append(f"  {lib_name}: LOADED in sys.modules")
        else:
            lines.append(f"  {lib_name}: not loaded")
    lines.append("")

    lines.append("=" * 72)
    lines.append("")

    report = "\n".join(lines)
    # Print to terminal reporter (visible in CI logs)
    config = session.config
    tr = config.pluginmanager.get_plugin("terminalreporter")
    if tr is not None:
        tr.write_line("")
        for line in lines:
            tr.write_line(line)
    else:
        print(report)


# ── Utility ──────────────────────────────────────────────────────────────────


def _log(msg: str):
    """Print a diagnostics message with a prefix."""
    print(f"[DIAG] {msg}", flush=True)
