"""Analytics package for ADF-ADB Monitor Pro Max.

Pure, side-effect-free parsers over offline telemetry artifacts. These modules
never call external APIs or LLMs — they only read the structured dumps produced
by completed ADF/Databricks runs (see ADR-001 and ADR-002).
"""
from __future__ import annotations

import os
from pathlib import Path

# Repository root = two levels up from this file (src/analytics/__init__.py).
REPO_ROOT = Path(__file__).resolve().parents[2]


def get_telemetry_dir() -> Path:
    """Resolve the directory that holds telemetry artifacts.

    Honors the ``TELEMETRY_DIR`` environment variable (relative paths are
    resolved against the repository root); defaults to ``mock_telemetry``.
    """
    configured = os.getenv("TELEMETRY_DIR", "mock_telemetry")
    path = Path(configured)
    if not path.is_absolute():
        path = REPO_ROOT / path
    return path


__all__ = ["REPO_ROOT", "get_telemetry_dir"]
