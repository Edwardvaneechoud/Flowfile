"""How a kernel container reaches core — the single answer, used by every caller.

There were two independent constructions of this URL, and they disagreed in two
ways. One hardcoded port 63578 while the other used the port core actually bound,
so on any run where core is not on 63578 — the desktop app port-scans for a free
pair — a kernel's log callbacks went to the right process while its API callbacks
went to a different one. They also branched on *different* Docker signals, so a
partial Docker setup could send one to the service name and the other to the host
gateway.

Kept free of the docker SDK so anything may import it. The caller passes the
discriminator rather than having it recomputed here, because only ``KernelManager``
knows how its containers were attached.
"""

import os

from flowfile_core.configs import settings


def core_base_url(on_docker_network: bool) -> str:
    """Base URL a kernel should use to call back into core.

    ``FLOWFILE_CORE_URL`` wins outright — an operator (or the kernel integration
    fixture) may need to name a host neither branch would guess.

    Otherwise the host follows how the container is attached: a kernel joined to
    core's Docker network resolves core by service name, while a kernel on the
    default bridge reaches the host through the ``host.docker.internal`` alias
    that ``_build_run_kwargs`` maps to ``host-gateway``. The port is always the
    one core actually bound, read at call time so a late ``--port`` is honoured.
    """
    override = (os.environ.get("FLOWFILE_CORE_URL") or "").strip()
    if override:
        return override.rstrip("/")
    host = "flowfile-core" if on_docker_network else "host.docker.internal"
    return f"http://{host}:{settings.SERVER_PORT}"
