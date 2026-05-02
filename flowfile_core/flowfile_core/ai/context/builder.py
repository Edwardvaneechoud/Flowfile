"""Subgraph + schema + sample serialiser — owned by W22.

Composes the per-call prompt context: pinned node, upstream schemas, and
(per D009) optionally sample rows after the regex PII pass. Per D008,
output is the *user* portion of the prompt; system text comes from
``prompts/{base, surface}.md``.

Stub until W22.
"""
