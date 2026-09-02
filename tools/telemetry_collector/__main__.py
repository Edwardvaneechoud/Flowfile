"""Run the telemetry collector with uvicorn: python -m tools.telemetry_collector."""

import uvicorn

from tools.telemetry_collector.app import app


def main() -> None:
    # access_log off: its request lines are the only place a client IP would be recorded
    uvicorn.run(app, host="0.0.0.0", port=8300, access_log=False)


if __name__ == "__main__":
    main()
