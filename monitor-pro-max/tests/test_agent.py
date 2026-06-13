"""Tests for the agent tools and tool-calling routing (mocked LLM)."""
from __future__ import annotations

import json
import os
from unittest.mock import MagicMock

import pytest

from src.agent import tools as tools_mod
from src.agent.diagnostic_agent import MissingAPIKeyError, ask_agent, build_agent
from src.agent.tools import (
    ALL_TOOLS,
    analyze_failed_or_canceled_run,
    get_historical_runs_summary,
    get_pipeline_data_flow,
)


@pytest.fixture(autouse=True)
def _point_telemetry_at_mock(telemetry_dir, monkeypatch):
    monkeypatch.setenv("TELEMETRY_DIR", str(telemetry_dir))


# --------------------------------------------------------------------------- #
# Tool registration + contracts
# --------------------------------------------------------------------------- #
def test_all_tools_registered():
    names = {t.name for t in ALL_TOOLS}
    assert names == {
        "get_historical_runs_summary",
        "get_pipeline_data_flow",
        "analyze_failed_or_canceled_run",
    }


def test_get_historical_runs_summary_tool():
    payload = json.loads(get_historical_runs_summary.invoke({}))
    assert payload["total_runs"] >= 1
    assert "status_counts" in payload


def test_get_pipeline_data_flow_tool():
    payload = json.loads(get_pipeline_data_flow.invoke({"run_id": "RUN-2026-06-12-02"}))
    assert payload["found"] is True
    assert payload["totals"]["rows_written"] == 905_000


def test_get_pipeline_data_flow_tool_missing():
    payload = json.loads(get_pipeline_data_flow.invoke({"run_id": "RUN-MISSING"}))
    assert payload["found"] is False


def test_analyze_failed_run_uses_trace():
    payload = json.loads(analyze_failed_or_canceled_run.invoke({"run_id": "RUN-2026-06-13-01"}))
    assert payload["primary_category"] == "Out of Memory"
    assert payload["run_id"] == "RUN-2026-06-13-01"


def test_analyze_failed_run_fallback_to_metadata():
    # This run has an orchestration error message but no dedicated .log trace.
    payload = json.loads(analyze_failed_or_canceled_run.invoke({"run_id": "RUN-2026-06-11-02"}))
    assert payload["found"] is True
    assert payload["source"] == "orchestration_metadata"
    assert "canceled" in payload["diagnostic_summary"].lower()


def test_analyze_failed_run_unknown():
    payload = json.loads(analyze_failed_or_canceled_run.invoke({"run_id": "RUN-GHOST"}))
    assert payload["found"] is False


def test_find_log_for_run(telemetry_dir):
    assert tools_mod._find_log_for_run("RUN-2026-06-13-01", telemetry_dir) == "spark_error_trace_04.log"
    assert tools_mod._find_log_for_run("RUN-NOPE", telemetry_dir) is None


# --------------------------------------------------------------------------- #
# Agent construction + routing (mocked LLM)
# --------------------------------------------------------------------------- #
def test_build_agent_passes_tools_to_create_agent(monkeypatch):
    """build_agent should hand all 3 tools + system prompt to create_agent."""
    captured = {}

    def fake_create_agent(model, tools, system_prompt=None):
        captured["model"] = model
        captured["tools"] = tools
        captured["system_prompt"] = system_prompt
        return "compiled-graph"

    monkeypatch.setattr("src.agent.diagnostic_agent.create_agent", fake_create_agent)
    fake_model = MagicMock()
    result = build_agent(model=fake_model)
    assert result == "compiled-graph"
    assert captured["model"] is fake_model
    assert len(captured["tools"]) == 3
    assert "ADF-ADB Monitor Pro Max" in captured["system_prompt"]


def test_ask_agent_routes_to_executor():
    """ask_agent should pass the question through and return the last message content."""
    mock_executor = MagicMock()
    mock_msg = MagicMock()
    mock_msg.content = "RUN-2026-06-13-01 failed due to OOM."
    mock_executor.invoke.return_value = {"messages": [mock_msg]}
    answer = ask_agent("Why did it fail?", agent=mock_executor)
    assert "OOM" in answer
    args = mock_executor.invoke.call_args[0][0]
    assert args["messages"][-1]["content"] == "Why did it fail?"


def test_build_llm_missing_key_raises(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "anthropic")
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    with pytest.raises(MissingAPIKeyError):
        build_agent()


def test_build_llm_invalid_provider(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "not-a-provider")
    with pytest.raises(ValueError):
        build_agent()
