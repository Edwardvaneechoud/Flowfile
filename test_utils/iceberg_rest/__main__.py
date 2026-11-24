"""python -m test_utils.iceberg_rest start|stop|demo|flow|signin|ui|tables: the local Iceberg REST catalogs and walks.

``ui`` takes ``--hold`` (serve a real Flowfile for clicking by hand) and ``--shots DIR`` (where the screenshots go); it
makes a Polaris catalog and a Lakekeeper warehouse of its own and drops them when it stops. ``tables`` lists each table
of Polaris's shared warehouse with its rows, counted by reading it, and its last commit (read-only; ``--warehouse NAME``
for another one).
"""

import logging
import sys

VERBS = ("start", "stop", "demo", "flow", "signin", "ui", "tables")
USAGE = (
    f"usage: python -m test_utils.iceberg_rest {'|'.join(VERBS)}\n"
    "       python -m test_utils.iceberg_rest ui [--hold] [--shots DIR]\n"
    "       python -m test_utils.iceberg_rest tables [--warehouse NAME]"
)


def main(argv: list[str]) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if not argv or argv[0] not in VERBS or (argv[0] not in ("ui", "tables") and len(argv) != 1):
        print(USAGE, file=sys.stderr)
        return 2
    if argv[0] == "tables" and argv[1:] and (len(argv) != 3 or argv[1] != "--warehouse"):
        print(USAGE, file=sys.stderr)
        return 2
    if argv[0] == "demo":
        from test_utils.iceberg_rest.demo import run_demo

        return run_demo()
    if argv[0] == "flow":
        from test_utils.iceberg_rest.flow_demo import run_flow_demo

        return run_flow_demo()
    if argv[0] == "signin":
        from test_utils.iceberg_rest.signin_demo import run_signin_demo

        return run_signin_demo()
    if argv[0] == "ui":
        from test_utils.iceberg_rest.ui_check import run_ui_check

        return run_ui_check(argv[1:])
    if argv[0] == "tables":
        from test_utils.iceberg_rest.tables import run_tables

        return run_tables(argv[1:])
    from test_utils.iceberg_rest import fixtures

    if argv[0] == "stop":
        fixtures.stop()
        return 0
    if not fixtures.start():
        return 1
    print(f"Iceberg REST catalog: {fixtures.CATALOG_URI} (warehouse {fixtures.WAREHOUSE!r}), no sign-in")
    print(
        f"Iceberg REST catalog: {fixtures.SIGNED_CATALOG_URI} (warehouse {fixtures.SIGNED_WAREHOUSE!r}, Apache Polaris),"
        f" asks for a sign-in; its tokens live {fixtures.token_seconds()} seconds"
    )
    print(f"S3 endpoint:          {fixtures.S3_ENDPOINT} (bucket {fixtures.BUCKET!r})")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
