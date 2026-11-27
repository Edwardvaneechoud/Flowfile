"""python -m test_utils.iceberg_rest start|stop|ui: the local Iceberg REST catalogs and the UI walk.

``ui`` takes ``--hold`` (serve a real Flowfile for clicking by hand), ``--shots DIR`` (where the screenshots go) and
``--kernel-image IMAGE`` (the notebook kernel image the walk's core starts, default the shell's
``FLOWFILE_KERNEL_IMAGE_NOTEBOOK``, else core's own choice); it makes a Polaris catalog and a Lakekeeper warehouse of its
own and drops them when it stops.
"""

import logging
import sys

VERBS = ("start", "stop", "ui")
USAGE = (
    f"usage: python -m test_utils.iceberg_rest {'|'.join(VERBS)}\n"
    "       python -m test_utils.iceberg_rest ui [--hold] [--shots DIR] [--kernel-image IMAGE]"
)


def main(argv: list[str]) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if not argv or argv[0] not in VERBS or (argv[0] != "ui" and len(argv) != 1):
        print(USAGE, file=sys.stderr)
        return 2
    if argv[0] == "ui":
        from test_utils.iceberg_rest.ui_check import run_ui_check

        return run_ui_check(argv[1:])
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
