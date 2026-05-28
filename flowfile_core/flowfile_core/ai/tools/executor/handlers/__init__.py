"""Per-op handlers — one module per top-level op family.

Each handler returns a :class:`ToolExecutionResult`. Handlers do not
import from each other; they share helpers via the parent ``executor``
package's ``_internal`` / ``refusals`` / ``coercions`` modules.
"""
