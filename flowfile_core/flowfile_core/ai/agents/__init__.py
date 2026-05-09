"""Per-surface agent implementations.

Three depth levels:

* ``assist``  — single-shot.
* ``copilot`` — short-context next-step.
* ``planner`` — multi-turn with diff staging.

Each agent composes its system prompt by concatenating
``prompts/base.md`` with its surface suffix.
"""
