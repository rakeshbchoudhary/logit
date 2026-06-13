"""Dense Spark driver trace parser for qualitative failure diagnostics.

This parser extracts *narrative* failure signals (exceptions, root-cause hints,
remediation) from raw text logs. It is intentionally NOT used for row counts —
those come from structured event JSON (ADR-002).
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from . import get_telemetry_dir

DEFAULT_LOG_FILE = "spark_error_trace_04.log"

# Ordered: most specific / actionable root causes first.
_ERROR_SIGNATURES = [
    {
        "key": "out_of_memory",
        "pattern": re.compile(r"OutOfMemoryError|GC overhead limit exceeded|exceeding memory limits", re.I),
        "category": "Out of Memory",
        "explanation": "An executor exhausted its JVM heap (OutOfMemoryError / GC overhead).",
        "remediation": "Increase spark.executor.memory / spark.executor.memoryOverhead, reduce partition size, or fix data skew.",
    },
    {
        "key": "executor_lost",
        "pattern": re.compile(r"ExecutorLostFailure|Lost executor|heartbeat timed out", re.I),
        "category": "Executor Lost",
        "explanation": "One or more executors were lost (heartbeat timeout or container killed).",
        "remediation": "Often a downstream symptom of OOM or node pressure; right-size the cluster and check skew.",
    },
    {
        "key": "stage_failure",
        "pattern": re.compile(r"Job aborted due to stage failure|failed \d+ times", re.I),
        "category": "Stage Failure",
        "explanation": "A task failed its max retry attempts, aborting the stage and the job.",
        "remediation": "Inspect the failing task's root exception; address the underlying cause (memory/skew/IO).",
    },
    {
        "key": "task_write_failure",
        "pattern": re.compile(r"Task failed while writing rows", re.I),
        "category": "Write Failure",
        "explanation": "A task failed while writing output rows to storage.",
        "remediation": "Check sink permissions/capacity and the underlying exception (commonly OOM during write).",
    },
    {
        "key": "spill",
        "pattern": re.compile(r"spilling sort data|Not enough space to cache", re.I),
        "category": "Memory Spill",
        "explanation": "Heavy shuffle/sort spill to disk indicates memory pressure and slow stages.",
        "remediation": "Increase memory, repartition to reduce per-task size, or enable adaptive query execution.",
    },
]

_RUN_ID_RE = re.compile(r"run_id=([A-Za-z0-9\-]+)")
_FINAL_STATUS_RE = re.compile(r"final_status=([A-Z]+)")
_LEVEL_RE = re.compile(r"\b(ERROR|WARN|INFO)\b")
_EXCEPTION_RE = re.compile(r"^(?:Caused by: )?([\w\.]+(?:Error|Exception))(?::\s*(.*))?$")


# --------------------------------------------------------------------------- #
# IO
# --------------------------------------------------------------------------- #
def read_log(log_file: Optional[str] = None, telemetry_dir: Optional[Path] = None) -> str:
    """Read a Spark trace file as raw text."""
    base = telemetry_dir or get_telemetry_dir()
    path = Path(base) / (log_file or DEFAULT_LOG_FILE)
    with open(path, "r", encoding="utf-8") as fh:
        return fh.read()


# --------------------------------------------------------------------------- #
# Parsing
# --------------------------------------------------------------------------- #
def extract_run_id(log_text: str) -> Optional[str]:
    """Pull the embedded Databricks run_id from the trace, if present."""
    match = _RUN_ID_RE.search(log_text)
    return match.group(1) if match else None


def extract_final_status(log_text: str) -> Optional[str]:
    match = _FINAL_STATUS_RE.search(log_text)
    return match.group(1) if match else None


def extract_exceptions(log_text: str) -> List[Dict[str, str]]:
    """Extract distinct exception class names + messages from the trace."""
    seen: set[str] = set()
    exceptions: List[Dict[str, str]] = []
    for line in log_text.splitlines():
        match = _EXCEPTION_RE.match(line.strip())
        if match:
            cls = match.group(1)
            msg = (match.group(2) or "").strip()
            if cls not in seen:
                seen.add(cls)
                exceptions.append({"exception": cls, "message": msg})
    return exceptions


def count_log_levels(log_text: str) -> Dict[str, int]:
    """Tally ERROR/WARN/INFO line counts."""
    counts = {"ERROR": 0, "WARN": 0, "INFO": 0}
    for line in log_text.splitlines():
        match = _LEVEL_RE.search(line)
        if match:
            counts[match.group(1)] += 1
    return counts


def detect_error_signatures(log_text: str) -> List[Dict[str, str]]:
    """Match known failure signatures, preserving priority order."""
    matched: List[Dict[str, str]] = []
    for sig in _ERROR_SIGNATURES:
        if sig["pattern"].search(log_text):
            matched.append(
                {
                    "key": sig["key"],
                    "category": sig["category"],
                    "explanation": sig["explanation"],
                    "remediation": sig["remediation"],
                }
            )
    return matched


def parse_spark_trace(
    log_file: Optional[str] = None, telemetry_dir: Optional[Path] = None
) -> Dict[str, Any]:
    """Produce a structured, plain-English diagnostic profile from a Spark trace."""
    log_text = read_log(log_file, telemetry_dir)
    signatures = detect_error_signatures(log_text)
    exceptions = extract_exceptions(log_text)
    levels = count_log_levels(log_text)

    primary = signatures[0] if signatures else None
    if primary:
        summary = (
            f"Primary failure category: {primary['category']}. {primary['explanation']} "
            f"Recommended action: {primary['remediation']}"
        )
    elif exceptions:
        summary = f"Unclassified failure. First exception observed: {exceptions[0]['exception']}."
    else:
        summary = "No error or exception signatures detected in this trace."

    return {
        "run_id": extract_run_id(log_text),
        "final_status": extract_final_status(log_text),
        "log_level_counts": levels,
        "exceptions": exceptions,
        "error_signatures": signatures,
        "primary_category": primary["category"] if primary else None,
        "diagnostic_summary": summary,
    }
