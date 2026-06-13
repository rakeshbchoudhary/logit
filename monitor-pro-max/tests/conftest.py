"""Pytest fixtures + path setup for ADF-ADB Monitor Pro Max."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


@pytest.fixture(scope="session")
def telemetry_dir() -> Path:
    """Path to the bundled mock telemetry fixtures."""
    return REPO_ROOT / "mock_telemetry"
