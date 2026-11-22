"""python -m test_utils.iceberg_rest start|stop|demo|flow: the local Iceberg REST catalog and two short walks through it."""

import logging
import sys

USAGE = "usage: python -m test_utils.iceberg_rest start|stop|demo|flow"


def main(argv: list[str]) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if len(argv) != 1 or argv[0] not in ("start", "stop", "demo", "flow"):
        print(USAGE, file=sys.stderr)
        return 2
    if argv[0] == "demo":
        from test_utils.iceberg_rest.demo import run_demo

        return run_demo()
    if argv[0] == "flow":
        from test_utils.iceberg_rest.flow_demo import run_flow_demo

        return run_flow_demo()
    from test_utils.iceberg_rest import fixtures

    if argv[0] == "stop":
        fixtures.stop()
        return 0
    if not fixtures.start():
        return 1
    print(f"Iceberg REST catalog: {fixtures.CATALOG_URI} (warehouse {fixtures.WAREHOUSE!r})")
    print(f"S3 endpoint:          {fixtures.S3_ENDPOINT} (bucket {fixtures.BUCKET!r})")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
