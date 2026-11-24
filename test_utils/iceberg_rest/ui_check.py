"""python -m test_utils.iceberg_rest ui [--hold] [--shots DIR]: a person adds the two local catalogs in a real Flowfile.

It seeds Polaris (``sales``: ``orders`` with 1,000 rows and ``customers``; ``finance``: ``invoices``; every other
top-level namespace of this throwaway stack is dropped, so Test reads "2 schemas") and Lakekeeper (``demo.orders``),
makes a Polaris principal, starts a core and a worker on free ports in desktop mode with a home, database and
secret store of their own and no AWS identity, builds the web frontend into a temporary folder and serves it with
``vite preview``, then runs ``flowfile_frontend/tests/external-catalog.spec.ts`` with the catalogs' addresses and the
principal's client id and secret in that child's environment only. After it, Polaris is added once more over core's
API and a saved flow that names it by name runs on the worker; two printed lines say what Polaris then holds and
what the saved flow does not. Nothing secret is printed.

With ``--hold`` the spec does not run: the page's address and a 0600 file holding the client id and secret to type
are printed, and everything stays up until Ctrl-C (or SIGTERM). It stops only what it started and drops the principal.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from test_utils.iceberg_rest import fixtures, flow_demo

FRONTEND = fixtures.REPO_ROOT / "flowfile_frontend"
SPEC = "tests/external-catalog.spec.ts"


def _seed_polaris(principal: fixtures.Principal) -> None:
    import polars as pl

    from shared.iceberg_utils import write_table

    signed = fixtures.signed_catalog(principal)
    for (name,) in [ns for ns in signed.list_namespaces() if len(ns) == 1]:
        for child in signed.list_namespaces(name):
            signed.drop_namespace(child)
        for identifier in signed.list_tables(name):
            signed.drop_table(identifier)
        signed.drop_namespace(name)
    orders = flow_demo.orders_rows(1000)
    arrow = orders.to_arrow(compat_level=pl.CompatLevel.oldest())
    signed.create_namespace("sales")
    signed.create_namespace("finance")
    write_table(signed.create_table("sales.orders", schema=arrow.schema), orders.lazy(), "append")
    signed.create_table("sales.customers", schema=arrow.schema)
    signed.create_table("finance.invoices", schema=arrow.schema)


def _flow_by_name(stack, principal: fixtures.Principal) -> bool:
    """Add Polaris over core's API, run a saved flow that names it, and print what Polaris then holds."""
    import polars as pl

    from shared.iceberg_utils import scan_table, table_access

    added = stack.post("/catalog/external", {
        "name": "polaris", "uri": fixtures.SIGNED_CATALOG_URI, "warehouse": fixtures.SIGNED_WAREHOUSE,
        "sign_in": "client_credentials", "client_id": principal.client_id,
        "client_secret": principal.client_secret, "scope": fixtures.SIGNED_SCOPE,
    })
    try:
        flow = flow_demo.iceberg_flow("sales.orders", "sales.orders_grp3")
        for node in (flow["nodes"][0], flow["nodes"][2]):
            node["setting_input"]["cloud_storage_settings"]["iceberg_catalog"] = {"catalog": "polaris"}
        flow["flowfile_settings"]["execution_location"] = "remote"
        flow_id = stack.import_flow(flow)
        run = stack.run(flow_id)
        signed = fixtures.signed_catalog(principal)
        held = 0
        if run["success"] and signed.table_exists("sales.orders_grp3"):
            held = scan_table(table_access(signed.load_table("sales.orders_grp3"))).select(pl.len()).collect().item()
        expected = flow_demo.orders_rows(1000).filter(pl.col("grp") == 3).height
        ran = run["success"] and held == expected
        print(f'A saved flow that names the catalog "polaris" ran on the worker: it read sales.orders, kept group 3 '
              f"and wrote sales.orders_grp3, which Polaris now holds with {held:,} rows ({expected:,} expected)"
              f"{'' if ran else ' -- NOT AS EXPECTED: ' + flow_demo.services.failed_nodes(run)}.")
        saved = flow_demo.saved_flow(stack, flow_id)
        token, _ = fixtures.fetch_token(principal)
        secret_free = not flow_demo.leaks({"flow": saved}, [principal.client_secret, principal.credential, token])
        named = "polaris" in saved and fixtures.SIGNED_CATALOG_URI not in saved
        print(f"The saved flow names the catalog and holds neither its address nor its secret nor a token: "
              f"{'yes' if secret_free and named else 'NO'}.")
        return ran and secret_free and named
    finally:
        stack.http.delete(f"{stack.base}/catalog/external/{added['id']}", timeout=30)


def _wait(url: str, proc: subprocess.Popen, seconds: float = 120) -> bool:
    import requests

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


def _stop(proc: subprocess.Popen | None) -> None:
    if proc is None or proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()


def run_ui_check(argv: list[str]) -> int:
    hold = "--hold" in argv
    shots = argv[argv.index("--shots") + 1] if "--shots" in argv else None
    if not (fixtures.catalog_ready() and fixtures.signed_catalog_ready()):
        print("Both local catalogs must run (Lakekeeper and Polaris).")
        print("Start them with: python -m test_utils.iceberg_rest start")
        return 1
    from test_utils import services

    principal = fixtures.new_principal(f"flowfile_ui_{os.getpid()}")
    preview: subprocess.Popen | None = None
    try:
        with tempfile.TemporaryDirectory(prefix="iceberg_ui_") as scratch:
            root = Path(scratch)
            env = flow_demo.service_env(root)
            os.environ.update({name: value for name, value in env.items()})
            for name in ("AWS_PROFILE", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
                os.environ.pop(name, None)
            _seed_polaris(principal)
            flow_demo.seed_orders("demo")
            with services.running_services(root / "stack", "electron", env, label="iceberg_ui") as stack:
                web, out = services.free_port(), root / "web"
                vite = str(FRONTEND / "node_modules" / ".bin" / "vite")
                built = subprocess.run(
                    [vite, "build", "--config", "vite.config.mjs", "--outDir", str(out), "--emptyOutDir",
                     "--logLevel", "error"],
                    cwd=FRONTEND, check=False,
                )
                if built.returncode != 0:
                    print("The web frontend did not build.")
                    return 1
                core_port = stack.base.rsplit(":", 1)[1]
                with (root / "preview.log").open("w") as log:
                    preview = subprocess.Popen(
                        [vite, "preview", "--config", "vite.config.mjs", "--outDir", str(out), "--host", "127.0.0.1",
                         "--port", str(web), "--strictPort"],
                        cwd=FRONTEND, env={**os.environ, "FLOWFILE_CORE_PORT": core_port},
                        stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
                    )
                page = f"http://127.0.0.1:{web}"
                if not _wait(f"{page}/", preview):
                    print("The preview server did not come up.")
                    return 1
                if hold:
                    typed = root / "sign-in.txt"
                    typed.write_text(
                        f"Address:       {fixtures.SIGNED_CATALOG_URI}\nWarehouse:     {fixtures.SIGNED_WAREHOUSE}\n"
                        f"Client id:     {principal.client_id}\nClient secret: {principal.client_secret}\n"
                        f"Scope:         {fixtures.SIGNED_SCOPE}\n\nThe open catalog: {fixtures.CATALOG_URI}"
                        f" (warehouse {fixtures.WAREHOUSE}), no sign-in.\n"
                    )
                    typed.chmod(0o600)
                    print(f"Flowfile: {page}/#/main/catalog?tab=catalog")
                    print(f"What to type: {typed}")
                    print("Ctrl-C stops everything.")
                    signal.signal(signal.SIGINT, signal.default_int_handler)
                    signal.signal(signal.SIGTERM, signal.default_int_handler)
                    try:
                        while True:
                            time.sleep(1)
                    except KeyboardInterrupt:
                        return 0
                if shots:
                    Path(shots).mkdir(parents=True, exist_ok=True)
                child_env = {
                    **os.environ,
                    "TEST_URL": page,
                    "API_URL": stack.base,
                    "EXTERNAL_CATALOG_URI": fixtures.SIGNED_CATALOG_URI,
                    "EXTERNAL_CATALOG_WAREHOUSE": fixtures.SIGNED_WAREHOUSE,
                    "EXTERNAL_CATALOG_CLIENT_ID": principal.client_id,
                    "EXTERNAL_CATALOG_CLIENT_SECRET": principal.client_secret,
                    "EXTERNAL_CATALOG_SCOPE": fixtures.SIGNED_SCOPE,
                    "OPEN_CATALOG_URI": fixtures.CATALOG_URI,
                    "OPEN_CATALOG_WAREHOUSE": fixtures.WAREHOUSE,
                    "EXTERNAL_CATALOG_SHOTS": str(Path(shots).resolve()) if shots else "",
                    "PLAYWRIGHT_HTML_OPEN": "never",
                }
                spec = subprocess.run(
                    ["npx", "playwright", "test", SPEC, "--reporter=list", "--output", str(root / "results")],
                    cwd=FRONTEND, env=child_env, capture_output=True, text=True, check=False,
                )
                print(fixtures.mask(spec.stdout[-6000:], fixtures.sign_in_values([principal.client_secret])))
                if spec.returncode != 0:
                    print(fixtures.mask(spec.stderr[-3000:], [principal.client_secret]), file=sys.stderr)
                    return spec.returncode
                return 0 if _flow_by_name(stack, principal) else 1
    finally:
        _stop(preview)
        fixtures.drop_principal(principal.name)
