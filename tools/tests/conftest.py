"""
Pytest configuration for the tools package tests.
"""

import sys
from pathlib import Path

# Ensure tools package is importable
REPO_ROOT = Path(__file__).parent.parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
