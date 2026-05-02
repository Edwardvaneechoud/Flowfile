"""SSE primitives for AI endpoints.

Owned by W13. Will provide:

* keepalive comments every ~10s to defeat proxy idle timeouts;
* resumption tokens so a disconnected stream can pick up at the last
  successfully validated tool boundary;
* helpers for serialising provider chunks into the SSE wire format.

Until W13 lands, ``streaming`` exists only so other modules can declare typed
imports; no symbols are exported.
"""
