"""python -m test_utils.iceberg_rest ui [--hold] [--shots DIR]: a person adds the two local catalogs in a real Flowfile.

Each run keeps to what it makes, so it can run while somebody's own tables sit in the same Polaris and Lakekeeper, and
beside another run. It makes a Polaris catalog ``walk_<pid>_<hex>`` of its own (``sales``: ``orders`` with 1,000 rows
and ``customers`` with 5; ``finance``: ``invoices`` with 5; so Test reads "2 schemas") with a principal of the same
name, and a Lakekeeper warehouse of the same name (``demo.orders``, 1,000 rows), their files under
``s3://iceberg-proof/walks/<name>/``. It starts a core and a worker on free ports in desktop mode with a home, database
and secret store of their own and no AWS identity, builds the web frontend into a temporary folder and serves it with
``vite preview``, then runs ``flowfile_frontend/tests/external-catalog.spec.ts`` with the catalogs' addresses and the
principal's client id and secret in that child's environment only (the spec adds both catalogs, reads ``orders``
with a Catalog Reader and appends it to ``orders_copy`` with a Catalog Writer). After it, Polaris is added once more
over core's API and a saved flow of a Catalog Reader on ``polaris.sales.orders``, a filter on group 3 and a Catalog
Writer to ``polaris.sales.orders_grp3`` runs on the worker; three printed lines say how many rows Polaris then holds
there (read with the catalog's keys), whether Flowfile's catalog names both tables for the flow, and that the saved
flow holds no address, secret, token or storage key. A line that is not as expected ends in "-- NOT AS EXPECTED" and
the walk exits 1. Nothing secret is printed.

With ``--hold`` the spec does not run: the page's address and a 0600 file holding the client id and secret to type
are printed, and everything stays up until Ctrl-C or SIGTERM. However it ends (also on a signal during start-up or a
second one during cleanup), it stops every process it started and their children, removes its temporary folder and
drops its principal, its Polaris catalog, its Lakekeeper warehouse and their files. A catalog, warehouse, namespace,
table or principal it did not make is never dropped or written.
"""

from __future__ import annotations

import contextlib
import os
import secrets
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

import requests

from test_utils.iceberg_rest import fixtures, flow_demo

FRONTEND = fixtures.REPO_ROOT / "flowfile_frontend"
SPEC = "tests/external-catalog.spec.ts"
WALKS = "walks"
GRACE = 5.0


@dataclass
class _Run:
    """What this run made, so cleanup drops exactly that and nothing else."""

    name: str
    principal: fixtures.Principal | None = None
    lakekeeper_id: str | None = None
    scratch: Path | None = None

    @property
    def location(self) -> str:
        return f"s3://{fixtures.BUCKET}/{WALKS}/{self.name}"


class _Signals:
    """SIGINT and SIGTERM raise once; any later one is ignored, so no signal can cut cleanup short.

    Python's own handler raises on every Ctrl-C, so a second one used to abort cleanup halfway (a temporary folder
    left, a child never stopped). The handler signals no child itself: ``test_utils.services.stop`` signals core's
    and the worker's groups and tolerates only a group that is already gone, not one still exiting.
    """

    def __init__(self) -> None:
        self.stopping = False

    def install(self) -> None:
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, signum, frame) -> None:
        if not self.stopping:
            self.stopping = True
            raise KeyboardInterrupt


def _descendants():
    import psutil

    try:
        return psutil.Process().children(recursive=True)
    except psutil.Error:
        return []


def _signal_descendants(signum: int) -> None:
    """Send ``signum`` to every process this one started and to their process groups (each child leads its own)."""
    import psutil

    signalled = {os.getpgrp()}
    for proc in _descendants():
        with contextlib.suppress(psutil.Error, OSError):
            group = os.getpgid(proc.pid)
            if group in signalled:
                proc.send_signal(signum)
            else:
                os.killpg(group, signum)
                signalled.add(group)


def _stop_descendants() -> None:
    """Stop everything this process started: SIGTERM, ``GRACE`` seconds, then SIGKILL for what is left."""
    import psutil

    procs = _descendants()
    if not procs:
        return
    _signal_descendants(signal.SIGTERM)
    _, alive = psutil.wait_procs(procs, timeout=GRACE)
    if alive:
        _signal_descendants(signal.SIGKILL)
        psutil.wait_procs(alive, timeout=3)


def _polaris_catalog_body(run: _Run) -> dict:
    return {
        "catalog": {
            "name": run.name,
            "type": "INTERNAL",
            "readOnly": False,
            "properties": {"default-base-location": run.location},
            "storageConfigInfo": {
                "storageType": "S3",
                "allowedLocations": [run.location],
                "endpoint": fixtures.S3_ENDPOINT,
                "stsEndpoint": fixtures.S3_ENDPOINT,
                "pathStyleAccess": True,
                "region": fixtures.REGION,
            },
        }
    }


def _lakekeeper_catalog(run: _Run):
    from pyiceberg.catalog import load_catalog

    return load_catalog("walk", **{**fixtures.catalog_properties(), "warehouse": run.name})


def _make(run: _Run) -> None:
    """The run's own Polaris catalog, principal and Lakekeeper warehouse, seeded with the tables the spec expects."""
    import polars as pl

    from shared.iceberg_utils import write_table

    fixtures._manage("POST", "/catalogs", fixtures._root_headers(), _polaris_catalog_body(run))
    run.principal = fixtures.new_principal(run.name, catalog=run.name)
    body = fixtures.warehouse_body()
    body["warehouse-name"] = run.name
    body["storage-profile"]["key-prefix"] = f"{WALKS}/{run.name}"
    made = requests.post(f"{fixtures.CATALOG_URL}/management/v1/warehouse", json=body, timeout=60)
    if made.status_code >= 400:
        raise RuntimeError(fixtures.mask(f"Lakekeeper refused the warehouse ({made.status_code}): {made.text[:300]}"))
    run.lakekeeper_id = made.json()["warehouse-id"]

    signed = fixtures.signed_catalog(run.principal, warehouse=run.name)
    signed.create_namespace("sales")
    signed.create_namespace("finance")
    orders = flow_demo.orders_rows(1000)
    customers = pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Ada", "Ben", "Cas", "Dee", "Eli"],
            "city": ["Amsterdam", "Utrecht", "Ghent", "Leiden", "Delft"],
        }
    )
    invoices = pl.DataFrame(
        {"id": [101, 102, 103, 104, 105], "customer_id": [1, 2, 2, 4, 5], "amount": [120.0, 75.5, 310.25, 42.0, 99.9]}
    )
    for identifier, frame in (("sales.orders", orders), ("sales.customers", customers), ("finance.invoices", invoices)):
        table = signed.create_table(identifier, schema=frame.to_arrow(compat_level=pl.CompatLevel.oldest()).schema)
        write_table(table, frame.lazy(), "append")

    lake = _lakekeeper_catalog(run)
    lake.create_namespace("demo")
    arrow = orders.to_arrow(compat_level=pl.CompatLevel.oldest())
    lake.create_table("demo.orders", schema=arrow.schema).append(arrow)


def _polaris_namespaces(base: str, headers: dict, parent: list[str] | None = None) -> list[list[str]]:
    """Every namespace of a Polaris catalog, children before their parents."""
    params = {"parent": "\x1f".join(parent)} if parent else {}
    answer = requests.get(f"{base}/namespaces", headers=headers, params=params, timeout=30)
    if answer.status_code == 404:
        return []
    answer.raise_for_status()
    found: list[list[str]] = []
    for namespace in answer.json().get("namespaces", []):
        found += [*_polaris_namespaces(base, headers, namespace), namespace]
    return found


def _attempt(failures: list[str], what: str, step) -> None:
    """Run one cleanup step; a failure is noted and the next step runs all the same."""
    try:
        step()
    except Exception as exc:  # noqa: BLE001
        failures.append(f"{what}: {fixtures.mask(str(exc))[:200]}")


def _deleted(answer: requests.Response) -> None:
    if answer.status_code not in (200, 204, 404):
        raise RuntimeError(f"answered {answer.status_code}: {answer.text[:200]}")


def _listed(answer: requests.Response) -> list[dict]:
    if answer.status_code == 404:
        return []
    answer.raise_for_status()
    return answer.json().get("identifiers", [])


def _drop_polaris(run: _Run) -> None:
    """The run's views, tables, namespaces (children first), principal and catalog, as root, each step tried even
    after one failed: nothing outside the run's catalog is touched."""
    headers = fixtures._root_headers()
    base = f"{fixtures.SIGNED_CATALOG_URI}/v1/{run.name}"
    failures: list[str] = []
    namespaces: list[list[str]] = []
    _attempt(failures, "list its namespaces", lambda: namespaces.extend(_polaris_namespaces(base, headers)))
    for namespace in namespaces:
        dotted, path = ".".join(namespace), f"{base}/namespaces/{'%1F'.join(namespace)}"
        for kind in ("views", "tables"):
            names: list[str] = []
            _attempt(failures, f"list the {kind} of {dotted}", lambda kind=kind, path=path, names=names: names.extend(
                item["name"] for item in _listed(requests.get(f"{path}/{kind}", headers=headers, timeout=30))
            ))
            for name in names:
                url = f"{path}/{kind}/{name}"
                _attempt(failures, f"drop {dotted}.{name}", lambda url=url: _deleted(
                    requests.delete(url, headers=headers, timeout=30)
                ))
        _attempt(failures, f"drop {dotted}", lambda path=path: _deleted(
            requests.delete(path, headers=headers, timeout=30)
        ))
    _attempt(failures, "drop its principal", lambda: fixtures.drop_principal(run.name, catalog=run.name))
    _attempt(
        failures,
        "drop its catalog",
        lambda: fixtures._manage("DELETE", f"/catalogs/{run.name}", headers, ok=(200, 204, 404)),
    )
    if failures:
        raise RuntimeError("; ".join(failures))


def _lakekeeper_namespaces(lake, parent: tuple[str, ...] = ()) -> list[tuple[str, ...]]:
    """Every namespace of the walk's own Lakekeeper warehouse, children before their parents."""
    found: list[tuple[str, ...]] = []
    for namespace in lake.list_namespaces(parent):
        found += [*_lakekeeper_namespaces(lake, namespace), namespace]
    return found


def _drop_lakekeeper(run: _Run) -> None:
    """The run's own warehouse: its views, tables and namespaces (children first), each tried even after one failed,
    then the forced delete of the warehouse whatever they left."""
    warehouse = run.lakekeeper_id
    if warehouse is None:
        listed = requests.get(f"{fixtures.CATALOG_URL}/management/v1/warehouse", timeout=10).json()
        warehouse = next(
            (w.get("id") or w.get("warehouse-id") for w in listed.get("warehouses", []) if w.get("name") == run.name),
            None,
        )
    if warehouse is None:
        return
    failures: list[str] = []
    namespaces: list[tuple[str, ...]] = []
    lake = None
    try:
        lake = _lakekeeper_catalog(run)
        namespaces = _lakekeeper_namespaces(lake)
    except Exception as exc:  # noqa: BLE001
        failures.append(f"list its namespaces: {fixtures.mask(str(exc))[:200]}")
    for namespace in namespaces:
        for list_items, drop in ((lake.list_views, lake.drop_view), (lake.list_tables, lake.drop_table)):
            items: list = []
            _attempt(
                failures,
                f"list {'.'.join(namespace)}",
                lambda list_items=list_items, namespace=namespace, items=items: items.extend(list_items(namespace)),
            )
            for identifier in items:
                _attempt(
                    failures, f"drop {'.'.join(identifier)}", lambda drop=drop, identifier=identifier: drop(identifier)
                )
        _attempt(failures, f"drop {'.'.join(namespace)}", lambda namespace=namespace: lake.drop_namespace(namespace))
    deadline = time.monotonic() + 15
    while True:
        answer = requests.delete(
            f"{fixtures.CATALOG_URL}/management/v1/warehouse/{warehouse}", params={"force": "true"}, timeout=60
        )
        if answer.status_code != 409 or "UnfinishedTasks" not in answer.text or time.monotonic() > deadline:
            break
        time.sleep(0.5)
    if answer.status_code not in (200, 204, 404):
        raise RuntimeError(fixtures.mask(f"Lakekeeper answered {answer.status_code}: {answer.text[:300]}"))
    if failures:
        print(f"Dropped Lakekeeper warehouse {run.name} after: {'; '.join(failures)}", file=sys.stderr)


def _drop_files(run: _Run) -> None:
    client = fixtures.s3_client()
    prefix = f"{WALKS}/{run.name}/"
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=fixtures.BUCKET, Prefix=prefix):
        keys = [{"Key": item["Key"]} for item in page.get("Contents", [])]
        if keys:
            client.delete_objects(Bucket=fixtures.BUCKET, Delete={"Objects": keys, "Quiet": True})


def _clean_up(run: _Run) -> bool:
    """Every step runs even when one before it fails; a failure is one masked line. True when all of them did."""
    steps = (
        ("stop the processes it started", _stop_descendants),
        ("remove its temporary folder", lambda: run.scratch and shutil.rmtree(run.scratch, ignore_errors=True)),
        (f"drop its Polaris catalog and principal {run.name}", lambda: _drop_polaris(run)),
        (f"drop its Lakekeeper warehouse {run.name}", lambda: _drop_lakekeeper(run)),
        (f"remove its files under {run.location}", lambda: _drop_files(run)),
    )
    done = True
    for what, step in steps:
        try:
            step()
        except Exception as exc:  # noqa: BLE001
            done = False
            print(f"Could not {what}: {fixtures.mask(str(exc))[:300]}", file=sys.stderr)
    return done


def _flow_by_name(stack, run: _Run) -> bool:
    """Add Polaris over core's API, run a saved flow of a Catalog Reader and a Catalog Writer on the worker, and print
    what Polaris then holds, whether Flowfile's catalog names both tables for the flow, and what the flow holds."""
    import polars as pl

    from shared.iceberg_utils import scan_table, storage_properties, table_access

    principal = run.principal
    added = stack.post(
        "/catalog/external",
        {
            "name": "polaris",
            "uri": fixtures.SIGNED_CATALOG_URI,
            "warehouse": run.name,
            "sign_in": "client_credentials",
            "client_id": principal.client_id,
            "client_secret": principal.client_secret,
            "scope": fixtures.SIGNED_SCOPE,
        },
    )
    try:
        flow = flow_demo.catalog_nodes_flow("polaris.sales.orders", "polaris.sales", "orders_grp3")
        flow["flowfile_settings"]["execution_location"] = "remote"
        flow_id = stack.import_flow(flow)
        run_report = stack.run(flow_id)
        signed = fixtures.signed_catalog(principal, warehouse=run.name)
        held, keys = 0, []
        if signed.table_exists("sales.orders_grp3"):
            written = signed.load_table("sales.orders_grp3")
            held = scan_table(table_access(written)).select(pl.len()).collect().item()
            keys = fixtures.key_values(storage_properties(written))
        expected = flow_demo.orders_rows(1000).filter(pl.col("grp") == 3).height
        ran = run_report["success"] and held == expected
        failed = "" if run_report["success"] else f": {flow_demo.services.failed_nodes(run_report)}"
        print(
            "A saved flow of a Catalog Reader and a Catalog Writer ran on the worker: it read polaris.sales.orders, "
            f"kept group 3 and wrote polaris.sales.orders_grp3, which Polaris now holds with {held:,} rows "
            f"({expected:,} expected){flow_demo._flag(ran)}{fixtures.mask(failed)[:300]}."
        )
        tables = flow_demo.catalog_flow_tables(stack, flow_id)
        named = tables == (["orders"], ["orders_grp3"])
        print(
            "Flowfile's catalog names both tables for that flow, polaris.sales.orders as read and "
            f"polaris.sales.orders_grp3 as written: {'yes' if named else 'no'}{flow_demo._flag(named)}."
        )
        saved = flow_demo.saved_flow(stack, flow_id)
        token, _ = fixtures.fetch_token(principal)
        secrets = [principal.client_secret, principal.credential, token, *keys]
        clean = (
            bool(keys)
            and not flow_demo.leaks({"flow": saved}, secrets)
            and not flow_demo.secret_shapes(saved)
            and fixtures.SIGNED_CATALOG_URI not in saved
        )
        print(
            "The saved flow holds neither the catalog's address, its secret, a token nor a storage key: "
            f"{'yes' if clean else 'no'}{flow_demo._flag(clean)}."
        )
        return ran and named and clean
    finally:
        stack.http.delete(f"{stack.base}/catalog/external/{added['id']}", timeout=30)


def _wait(url: str, proc: subprocess.Popen, seconds: float = 120) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            return False
        try:
            if requests.get(url, timeout=2).status_code < 500:
                return True
        except requests.RequestException:
            pass
        time.sleep(0.5)
    return False


def _hold(run: _Run, page: str) -> int:
    principal = run.principal
    typed = run.scratch / "sign-in.txt"
    typed.write_text(
        f"Address:       {fixtures.SIGNED_CATALOG_URI}\nWarehouse:     {run.name}\n"
        f"Client id:     {principal.client_id}\nClient secret: {principal.client_secret}\n"
        f"Scope:         {fixtures.SIGNED_SCOPE}\n\nThe open catalog: {fixtures.CATALOG_URI}"
        f" (warehouse {run.name}), no sign-in.\n"
    )
    typed.chmod(0o600)
    print(
        "This Flowfile starts on a home of its own, so its catalog shows only General until you add a catalog with "
        "New > Add a catalog from elsewhere."
    )
    print(f"Flowfile: {page}/#/main/catalog?tab=catalog")
    print(f"What to type: {typed}")
    print(
        f"Both catalogs' warehouse is {run.name}, made for this run and dropped with what you write in it when it "
        f"stops; the shared Polaris warehouse {fixtures.SIGNED_WAREHOUSE!r} is not touched."
    )
    print(f"Ctrl-C, or kill {os.getpid()}, stops everything.", flush=True)
    while True:
        time.sleep(1)


def _walk(run: _Run, signals: _Signals, hold: bool, shots: str | None) -> int:
    from test_utils import services

    root = run.scratch
    env = flow_demo.service_env(root)
    os.environ.update(env)
    for name in ("AWS_PROFILE", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
        os.environ.pop(name, None)
    _make(run)
    with services.running_services(root / "stack", "electron", env, label="iceberg_ui") as stack:
        try:
            return _serve(run, stack, hold, shots)
        finally:
            signals.stopping = True
            _stop_descendants()


def _serve(run: _Run, stack, hold: bool, shots: str | None) -> int:
    """Build and serve the frontend beside the running core, then hold or run the spec and the flow by name."""
    from test_utils import services

    root, principal = run.scratch, run.principal
    web, out = services.free_port(), root / "web"
    vite = str(FRONTEND / "node_modules" / ".bin" / "vite")
    built = subprocess.run(
        [vite, "build", "--config", "vite.config.mjs", "--outDir", str(out), "--emptyOutDir", "--logLevel", "error"],
        cwd=FRONTEND,
        check=False,
        start_new_session=True,
    )
    if built.returncode != 0:
        print("The web frontend did not build.")
        return 1
    core_port = stack.base.rsplit(":", 1)[1]
    with (root / "preview.log").open("w") as log:
        preview = subprocess.Popen(
            [
                vite,
                "preview",
                "--config",
                "vite.config.mjs",
                "--outDir",
                str(out),
                "--host",
                "127.0.0.1",
                "--port",
                str(web),
                "--strictPort",
            ],
            cwd=FRONTEND,
            env={**os.environ, "FLOWFILE_CORE_PORT": core_port},
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    page = f"http://127.0.0.1:{web}"
    if not _wait(f"{page}/", preview):
        print("The preview server did not come up.")
        return 1
    if hold:
        return _hold(run, page)
    if shots:
        Path(shots).mkdir(parents=True, exist_ok=True)
    child_env = {
        **os.environ,
        "TEST_URL": page,
        "API_URL": stack.base,
        "EXTERNAL_CATALOG_URI": fixtures.SIGNED_CATALOG_URI,
        "EXTERNAL_CATALOG_WAREHOUSE": run.name,
        "EXTERNAL_CATALOG_CLIENT_ID": principal.client_id,
        "EXTERNAL_CATALOG_CLIENT_SECRET": principal.client_secret,
        "EXTERNAL_CATALOG_SCOPE": fixtures.SIGNED_SCOPE,
        "OPEN_CATALOG_URI": fixtures.CATALOG_URI,
        "OPEN_CATALOG_WAREHOUSE": run.name,
        "EXTERNAL_CATALOG_SHOTS": str(Path(shots).resolve()) if shots else "",
        "PLAYWRIGHT_HTML_OPEN": "never",
    }
    spec = subprocess.run(
        ["npx", "playwright", "test", SPEC, "--reporter=list", "--output", str(root / "results")],
        cwd=FRONTEND,
        env=child_env,
        capture_output=True,
        text=True,
        check=False,
        start_new_session=True,
    )
    print(fixtures.mask(spec.stdout[-6000:], fixtures.sign_in_values([principal.client_secret])))
    if spec.returncode != 0:
        print(fixtures.mask(spec.stderr[-3000:], [principal.client_secret]), file=sys.stderr)
        return spec.returncode
    return 0 if _flow_by_name(stack, run) else 1


def run_ui_check(argv: list[str]) -> int:
    hold = "--hold" in argv
    shots = argv[argv.index("--shots") + 1] if "--shots" in argv else None
    signals = _Signals()
    signals.install()
    if not (fixtures.catalog_ready() and fixtures.signed_catalog_ready()):
        print("Both local catalogs must run (Lakekeeper and Polaris).")
        print("Start them with: python -m test_utils.iceberg_rest start")
        return 1
    run = _Run(f"walk_{os.getpid()}_{secrets.token_hex(3)}")
    started = time.monotonic()
    result = 130
    try:
        run.scratch = Path(tempfile.mkdtemp(prefix="iceberg_ui_"))
        result = _walk(run, signals, hold, shots)
    except KeyboardInterrupt:
        result = 0 if hold else 130
    finally:
        signals.stopping = True
        stopping = time.monotonic()
        clean = _clean_up(run)
        print(
            f"Stopped in {time.monotonic() - stopping:.1f} s after a run of {stopping - started:.0f} s: "
            f"{'its' if clean else 'NOT ALL of its'} processes, folder, Polaris catalog and principal and "
            f"Lakekeeper warehouse {run.name} are gone.",
            flush=True,
        )
    return result
