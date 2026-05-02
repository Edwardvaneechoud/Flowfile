"""Rate-limit-aware scheduler for provider calls.

Owned by W14. Will provide:

* per-provider window-aware token / request budgets;
* exponential backoff with jitter on 429 / 5xx;
* failure-is-free quota semantics (W14 reconciles with §5.5 of the plan).

Until W14 lands, ``scheduler`` exists only as a namespace for downstream
typed imports.
"""
