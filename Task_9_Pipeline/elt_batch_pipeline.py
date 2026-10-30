"""Compatibility launcher for the root-level Task 9 pipeline."""
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_DIR))

from elt_batch_pipeline import run_pipeline  # noqa: E402


if __name__ == "__main__":
    result = run_pipeline()
    sys.exit(0 if result["database_pipeline_status"] == "PASS" else 1)
