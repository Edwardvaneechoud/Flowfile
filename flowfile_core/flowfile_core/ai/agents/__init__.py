"""Per-surface agent implementations.

Three depth levels (per plan §2):

* ``assist``  — Level 1 single-shot (Phase 1 work).
* ``copilot`` — Level 2 short-context next-step (Phase 2 work).
* ``planner`` — Level 3 multi-turn with diff staging (W40).

Each agent composes its system prompt by concatenating ``prompts/base.md``
with its surface suffix per D008.
"""
