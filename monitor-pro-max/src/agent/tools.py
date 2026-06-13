"""LangChain tool wrappers around the offline analytics parsers.

Each tool returns a JSON string so the LLM receives a stable, machine-readable
contract. The three tools map directly to the product spec:

  * get_historical_runs_summary
  * get_pipeline_data_flow
  * analyze_failed_or_canceled_run
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from langchain_core.tools import tool

from ..analytics import get_telemetry_dir
from ..analytics.data_flow_analyzer import (
    analyze_pipeline_data_flow,
    load_historical_runs,
    summarize_historical_runs,
)
from ..analytics.log_parser import parse_spark_trace


def _dumps(payload: object) -> str:
    return json.dumps(payload, indent=2, default=str)


def _find_log_for_run(run_id: str, telemetry_dir: Optional[Path] = None) -> Optional[str]:
    """Locate the Spark trace file whose embedded run_id matches ``run_id``."""
    base = Path(telemetry_dir or get_telemetry_dir())
    for log_path in sorted(base.glob("*.log")):
        try:
            profile = parse_spark_trace(log_file=log_path.name, telemetry_dir=base)
        except OSError:
            continue
        if profile.get("run_id") == run_id:
            return log_path.name
    return None


@tool
def get_historical_runs_summary() -> str:
    """Return a timeline + rollup of past pipeline runs.

    Use this to answer questions about overall run history: which runs
    succeeded/failed/were canceled, their durations, and DBU costs over time.
    Takes no arguments.
    """
    return _dumps(summarize_historical_runs())


@tool
def get_pipeline_data_flow(run_id: str) -> str:
    """Return row-level data-flow lineage for a single pipeline Run ID.

    Extracts the exact lineage for the run: source tables, rows read, rows
    filtered, rows transformed, target tables, rows written, per-stage timing,
    and the slowest (bottleneck) stage.

    Args:
        run_id: The pipeline run identifier, e.g. "RUN-2026-06-12-02".
    """
    return _dumps(analyze_pipeline_data_flow(run_id))


@tool
def analyze_failed_or_canceled_run(run_id: str) -> str:
    """Diagnose a failed or canceled run from its Spark driver trace.

    Locates the Spark stack trace associated with the Run ID and returns a
    plain-English diagnostic profile (primary failure category, exceptions,
    log-level counts, and recommended remediation). If no trace file exists,
    falls back to the run's recorded orchestration error message.

    Args:
        run_id: The pipeline run identifier, e.g. "RUN-2026-06-13-01".
    """
    log_file = _find_log_for_run(run_id)
    if log_file is not None:
        return _dumps(parse_spark_trace(log_file=log_file))

    # Fallback: no dedicated trace file — use orchestration metadata.
    runs = load_historical_runs()
    if not runs.empty and run_id in set(runs["run_id"]):
        row = runs[runs["run_id"] == run_id].iloc[0]
        return _dumps(
            {
                "run_id": run_id,
                "found": True,
                "source": "orchestration_metadata",
                "status": row.get("status"),
                "diagnostic_summary": row.get("error_message")
                or "No Spark trace and no recorded error message for this run.",
            }
        )

    return _dumps(
        {
            "run_id": run_id,
            "found": False,
            "error": f"No Spark trace or run record found for run_id '{run_id}'.",
        }
    )


ALL_TOOLS = [
    get_historical_runs_summary,
    get_pipeline_data_flow,
    analyze_failed_or_canceled_run,
]
