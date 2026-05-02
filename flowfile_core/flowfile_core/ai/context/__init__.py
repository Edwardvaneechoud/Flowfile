"""Prompt-context construction — owned by W22 + W24.

Serialises the current subgraph, schemas, and (opt-in per D009) sample
rows into a token-budgeted system / user message pair. ``mentions`` parses
``@node`` / ``@col`` references; ``budget`` enforces caps so we never blow
the context window.
"""
