"""``import flowfile`` and building a frame start nothing of the worker; a frame's first submit does.

Flowfile in Python computes on the worker embedded in its own process, made the first time a node submits to
``local``. Each case runs in a fresh interpreter, since this test process imported the worker long ago.
"""

import json
import subprocess
import sys
import textwrap

IMPORT_BUDGET_SECONDS = 7.0
"""What ``import flowfile`` may take in a fresh interpreter with its bytecode written: about three times the
2.35 s it took on a laptop when the budget was set."""

PROBE = textwrap.dedent(
    """
    import json
    import multiprocessing
    import sys
    import time
    from multiprocessing import resource_tracker


    def state():
        return {
            "worker_modules": sorted(m for m in sys.modules if m.split(".")[0] == "flowfile_worker"),
            "resource_tracker": resource_tracker._resource_tracker._pid,
            "children": len(multiprocessing.active_children()),
        }


    start = time.perf_counter()
    import flowfile as ff

    report = {"import_seconds": time.perf_counter() - start, "imported": state()}
    if sys.argv[1] == "build":
        df = ff.from_dict({"a": [1, 2, 3], "g": ["x", "y", "x"]})
        totals = df.group_by("g").agg(ff.col("a").sum().alias("total"))
        frame = (
            df.filter(ff.col("a") > 1)
            .with_columns((ff.col("a") * 2).alias("b"))
            .select("a", "g", "b")
            .join(totals, on="g")
            .sort("a")
            .unique()
            .head(10)
        )
        ff.explore(ff.from_dict({"x": [1]}))
        report["built"] = state()
        report["run_success"] = frame.flow_graph.run_graph().success
        report["ran"] = state()
        report["in_process"] = sys.modules["flowfile_worker.spawner"].in_process
    print("PROBE " + json.dumps(report))
    """
)


def _probe(tmp_path, mode: str) -> dict:
    """Run :data:`PROBE` in a fresh interpreter in ``mode`` (``import`` or ``build``) and return its report."""
    done = subprocess.run(
        [sys.executable, "-c", PROBE, mode], cwd=tmp_path, capture_output=True, text=True, timeout=300
    )
    lines = [line for line in done.stdout.splitlines() if line.startswith("PROBE ")]
    assert done.returncode == 0 and lines, done.stderr[-4000:]
    return json.loads(lines[-1].removeprefix("PROBE "))


def _nothing_started(state: dict) -> None:
    assert state == {"worker_modules": [], "resource_tracker": None, "children": 0}


def test_import_flowfile_stays_within_its_budget_and_imports_no_worker(tmp_path):
    _probe(tmp_path, "import")
    report = _probe(tmp_path, "import")
    assert report["import_seconds"] < IMPORT_BUDGET_SECONDS, report["import_seconds"]
    _nothing_started(report["imported"])


def test_building_a_frame_starts_no_worker_until_its_first_submit(tmp_path):
    report = _probe(tmp_path, "build")
    _nothing_started(report["imported"])
    _nothing_started(report["built"])
    assert report["run_success"] is True
    assert "flowfile_worker" in report["ran"]["worker_modules"]
    assert report["in_process"] is True
    assert report["ran"]["children"] == 0
