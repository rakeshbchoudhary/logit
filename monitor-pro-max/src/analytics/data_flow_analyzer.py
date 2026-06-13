"""Row-level data-flow + cost/storage analytics over offline telemetry.

All row counts are derived from *structured event JSON* (Spark SQL metrics +
Delta Lake history), never scraped from text logs. See ADR-002.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from . import get_telemetry_dir

HISTORICAL_RUNS_FILE = "historical_runs_db.json"
DATA_FLOW_FILE = "pipeline_data_flow.json"
STORAGE_METRICS_FILE = "storage_growth_metrics.csv"


# --------------------------------------------------------------------------- #
# Loaders
# --------------------------------------------------------------------------- #
def _resolve(filename: str, telemetry_dir: Optional[Path] = None) -> Path:
    base = telemetry_dir or get_telemetry_dir()
    return Path(base) / filename


def load_historical_runs(telemetry_dir: Optional[Path] = None) -> pd.DataFrame:
    """Load aggregated run history into a tidy DataFrame."""
    path = _resolve(HISTORICAL_RUNS_FILE, telemetry_dir)
    with open(path, "r", encoding="utf-8") as fh:
        payload = json.load(fh)

    runs = payload.get("runs", [])
    df = pd.DataFrame(runs)
    if df.empty:
        return df

    for col in ("start_time", "end_time"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
    if "duration_seconds" in df.columns:
        df["duration_minutes"] = df["duration_seconds"] / 60.0
    return df


def load_data_flows(telemetry_dir: Optional[Path] = None) -> Dict[str, Any]:
    """Load the raw row-level data-flow document."""
    path = _resolve(DATA_FLOW_FILE, telemetry_dir)
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def load_storage_metrics(telemetry_dir: Optional[Path] = None) -> pd.DataFrame:
    """Load the storage-growth CSV snapshot matrix."""
    path = _resolve(STORAGE_METRICS_FILE, telemetry_dir)
    df = pd.read_csv(path)
    if "snapshot_date" in df.columns:
        df["snapshot_date"] = pd.to_datetime(df["snapshot_date"], errors="coerce")
    return df


# --------------------------------------------------------------------------- #
# Historical summary
# --------------------------------------------------------------------------- #
def summarize_historical_runs(telemetry_dir: Optional[Path] = None) -> Dict[str, Any]:
    """Return a timeline + rollup of execution statuses, durations, and costs."""
    df = load_historical_runs(telemetry_dir)
    if df.empty:
        return {"total_runs": 0, "timeline": [], "status_counts": {}, "totals": {}}

    status_counts = df["status"].value_counts().to_dict()
    timeline_cols = [
        c
        for c in (
            "run_id",
            "pipeline_name",
            "status",
            "start_time",
            "duration_seconds",
            "dbus_consumed",
            "error_message",
        )
        if c in df.columns
    ]
    timeline_df = df[timeline_cols].sort_values("start_time")
    timeline = json.loads(timeline_df.to_json(orient="records", date_format="iso"))

    totals = {
        "total_dbus_consumed": float(df.get("dbus_consumed", pd.Series(dtype=float)).sum()),
        "total_runtime_minutes": float(df.get("duration_seconds", pd.Series(dtype=float)).sum() / 60.0),
        "avg_duration_minutes": float(df.get("duration_seconds", pd.Series(dtype=float)).mean() / 60.0),
        "failure_rate": float(
            df["status"].isin(["Failed", "Canceled"]).mean()
        ),
    }

    return {
        "total_runs": int(len(df)),
        "status_counts": status_counts,
        "totals": totals,
        "timeline": timeline,
    }


# --------------------------------------------------------------------------- #
# Per-run data flow / lineage
# --------------------------------------------------------------------------- #
def get_flow_for_run(run_id: str, telemetry_dir: Optional[Path] = None) -> Optional[Dict[str, Any]]:
    """Return the raw flow document for a single run id, or None if missing."""
    doc = load_data_flows(telemetry_dir)
    for flow in doc.get("flows", []):
        if flow.get("run_id") == run_id:
            return flow
    return None


def analyze_pipeline_data_flow(run_id: str, telemetry_dir: Optional[Path] = None) -> Dict[str, Any]:
    """Map row-level metrics + table lineage for a specific run.

    Produces a normalized lineage summary:
    Source Table -> Rows Read -> Rows Filtered -> Rows Transformed -> Target -> Rows Written.
    """
    flow = get_flow_for_run(run_id, telemetry_dir)
    if flow is None:
        return {"run_id": run_id, "found": False, "error": f"No data flow found for run_id '{run_id}'."}

    stages = flow.get("stages", [])
    stage_df = pd.DataFrame(stages)

    totals = {
        "rows_read": int(stage_df.get("rows_read", pd.Series(dtype=int)).sum()),
        "rows_filtered": int(stage_df.get("rows_filtered", pd.Series(dtype=int)).sum()),
        "rows_transformed": int(stage_df.get("rows_transformed", pd.Series(dtype=int)).sum()),
        "rows_written": int(stage_df.get("rows_written", pd.Series(dtype=int)).sum()),
        "total_spilled_bytes": int(stage_df.get("spilled_bytes", pd.Series(dtype=int)).sum()),
        "total_stage_seconds": int(stage_df.get("duration_seconds", pd.Series(dtype=int)).sum()),
    }

    source_tables = sorted({s.get("source_table") for s in stages if s.get("source_table")})
    target_tables = sorted({s.get("target_table") for s in stages if s.get("target_table")})

    # Bottleneck = slowest stage by wall-clock duration.
    bottleneck = None
    if not stage_df.empty and "duration_seconds" in stage_df.columns:
        idx = stage_df["duration_seconds"].idxmax()
        bottleneck = stage_df.loc[idx].to_dict()

    return {
        "run_id": run_id,
        "found": True,
        "pipeline_name": flow.get("pipeline_name"),
        "status": flow.get("status"),
        "source_tables": source_tables,
        "target_tables": target_tables,
        "stages": stages,
        "totals": totals,
        "bottleneck_stage": bottleneck,
    }


def build_data_flow_volume_frame(telemetry_dir: Optional[Path] = None) -> pd.DataFrame:
    """Flatten all flows into one DataFrame for cross-run volume charts."""
    doc = load_data_flows(telemetry_dir)
    rows: List[Dict[str, Any]] = []
    for flow in doc.get("flows", []):
        for stage in flow.get("stages", []):
            rows.append(
                {
                    "run_id": flow.get("run_id"),
                    "pipeline_name": flow.get("pipeline_name"),
                    "status": flow.get("status"),
                    **stage,
                }
            )
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- #
# Cost efficiency (compute vs. storage)
# --------------------------------------------------------------------------- #
def compute_cost_efficiency(telemetry_dir: Optional[Path] = None) -> pd.DataFrame:
    """Join DBU spend against storage growth to expose compute-to-storage efficiency.

    Returns a per-date frame with total DBUs, total storage GB, daily storage
    delta, and a ``dbus_per_gb_added`` efficiency ratio.
    """
    storage = load_storage_metrics(telemetry_dir)
    if storage.empty:
        return storage

    daily = (
        storage.groupby("snapshot_date")
        .agg(total_size_gb=("size_gb", "sum"), total_dbus=("dbus_consumed", "sum"))
        .reset_index()
        .sort_values("snapshot_date")
    )
    daily["storage_growth_gb"] = daily["total_size_gb"].diff().fillna(0.0)
    daily["dbus_per_gb_added"] = daily.apply(
        lambda r: (r["total_dbus"] / r["storage_growth_gb"]) if r["storage_growth_gb"] > 0 else None,
        axis=1,
    )
    return daily
