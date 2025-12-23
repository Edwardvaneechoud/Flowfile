"""
Flowfile Migration Tool

Converts old pickle-based .flowfile format to new YAML format.

Usage:
    python -m tools.migrate <path>
    python -m tools.migrate old_flow.flowfile
    python -m tools.migrate ./flows/  # migrate entire directory
"""

__version__ = "1.0.0"