"""Unit tests for the data-flow and Spark log parsers."""
from __future__ import annotations

from src.analytics.data_flow_analyzer import (
    analyze_pipeline_data_flow,
    build_data_flow_volume_frame,
    compute_cost_efficiency,
    get_flow_for_run,
    load_historical_runs,
    load_storage_metrics,
    summarize_historical_runs,
)
from src.analytics.log_parser import (
    count_log_levels,
    detect_error_signatures,
    extract_exceptions,
    extract_final_status,
    extract_run_id,
    parse_spark_trace,
    read_log,
)


# --------------------------------------------------------------------------- #
# data_flow_analyzer
# --------------------------------------------------------------------------- #
def test_load_historical_runs(telemetry_dir):
    df = load_historical_runs(telemetry_dir)
    assert not df.empty
    assert {"run_id", "status", "dbus_consumed"}.issubset(df.columns)
    assert "duration_minutes" in df.columns


def test_summarize_historical_runs(telemetry_dir):
    import os

    os.environ["TELEMETRY_DIR"] = str(telemetry_dir)
    summary = summarize_historical_runs(telemetry_dir)
    assert summary["total_runs"] >= 1
    assert "Succeeded" in summary["status_counts"]
    assert 0.0 <= summary["totals"]["failure_rate"] <= 1.0
    assert isinstance(summary["timeline"], list)


def test_get_flow_for_run_found_and_missing(telemetry_dir):
    found = get_flow_for_run("RUN-2026-06-12-02", telemetry_dir)
    assert found is not None
    assert found["run_id"] == "RUN-2026-06-12-02"
    assert get_flow_for_run("RUN-DOES-NOT-EXIST", telemetry_dir) is None


def test_analyze_pipeline_data_flow_success(telemetry_dir):
    result = analyze_pipeline_data_flow("RUN-2026-06-12-02", telemetry_dir)
    assert result["found"] is True
    assert result["pipeline_name"] == "pl_transform_sales_silver"
    assert "bronze.sales_raw" in result["source_tables"]
    assert "silver.sales_clean" in result["target_tables"]
    totals = result["totals"]
    # Per-stage rows_read summed: READ 1.19M + FILTER 1.19M + TRANSFORM 0.905M.
    assert totals["rows_read"] == 1_190_000 + 1_190_000 + 905_000
    assert totals["rows_filtered"] == 285_000
    assert totals["rows_written"] == 905_000
    assert result["bottleneck_stage"] is not None


def test_analyze_pipeline_data_flow_missing(telemetry_dir):
    result = analyze_pipeline_data_flow("RUN-NOPE", telemetry_dir)
    assert result["found"] is False
    assert "error" in result


def test_build_data_flow_volume_frame(telemetry_dir):
    df = build_data_flow_volume_frame(telemetry_dir)
    assert not df.empty
    assert {"run_id", "operation", "rows_read"}.issubset(df.columns)


def test_load_storage_metrics(telemetry_dir):
    df = load_storage_metrics(telemetry_dir)
    assert not df.empty
    assert {"snapshot_date", "table_name", "size_gb", "dbus_consumed"}.issubset(df.columns)


def test_compute_cost_efficiency(telemetry_dir):
    df = compute_cost_efficiency(telemetry_dir)
    assert not df.empty
    assert {"storage_growth_gb", "dbus_per_gb_added", "total_dbus"}.issubset(df.columns)
    # First row's growth is baselined to 0.
    assert df.iloc[0]["storage_growth_gb"] == 0.0


# --------------------------------------------------------------------------- #
# log_parser
# --------------------------------------------------------------------------- #
def test_read_log_and_run_id(telemetry_dir):
    text = read_log(telemetry_dir=telemetry_dir)
    assert "OutOfMemoryError" in text
    assert extract_run_id(text) == "RUN-2026-06-13-01"
    assert extract_final_status(text) == "FAILED"


def test_extract_exceptions(telemetry_dir):
    text = read_log(telemetry_dir=telemetry_dir)
    exceptions = extract_exceptions(text)
    names = {e["exception"] for e in exceptions}
    assert any("OutOfMemoryError" in n for n in names)
    assert any("SparkException" in n for n in names)


def test_count_log_levels(telemetry_dir):
    text = read_log(telemetry_dir=telemetry_dir)
    levels = count_log_levels(text)
    assert levels["ERROR"] > 0
    assert levels["WARN"] > 0


def test_detect_error_signatures_priority(telemetry_dir):
    text = read_log(telemetry_dir=telemetry_dir)
    sigs = detect_error_signatures(text)
    assert sigs, "expected at least one matched signature"
    # OOM is the highest-priority signature and present in the trace.
    assert sigs[0]["key"] == "out_of_memory"


def test_parse_spark_trace_profile(telemetry_dir):
    profile = parse_spark_trace(telemetry_dir=telemetry_dir)
    assert profile["run_id"] == "RUN-2026-06-13-01"
    assert profile["primary_category"] == "Out of Memory"
    assert "Recommended action" in profile["diagnostic_summary"]
    assert profile["log_level_counts"]["ERROR"] > 0


def test_detect_error_signatures_empty():
    assert detect_error_signatures("just a normal informational line") == []
