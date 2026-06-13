"""ADF-ADB Monitor Pro Max - Streamlit dashboard.

Top panel: Plotly charts for data-flow volume and compute/storage cost trends.
Bottom panel: tool-calling diagnostic chat agent.

Run with:  streamlit run src/app.py
"""
from __future__ import annotations

import sys
from pathlib import Path

# Ensure the repo root is importable when run as a top-level Streamlit script.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

from src.analytics.data_flow_analyzer import (
    analyze_pipeline_data_flow,
    build_data_flow_volume_frame,
    compute_cost_efficiency,
    load_historical_runs,
)

load_dotenv()

st.set_page_config(page_title="ADF-ADB Monitor Pro Max", page_icon="📊", layout="wide")


# --------------------------------------------------------------------------- #
# Cached loaders
# --------------------------------------------------------------------------- #
@st.cache_data(show_spinner=False)
def _runs() -> pd.DataFrame:
    return load_historical_runs()


@st.cache_data(show_spinner=False)
def _flows() -> pd.DataFrame:
    return build_data_flow_volume_frame()


@st.cache_data(show_spinner=False)
def _efficiency() -> pd.DataFrame:
    return compute_cost_efficiency()


# --------------------------------------------------------------------------- #
# Header
# --------------------------------------------------------------------------- #
st.title("📊 ADF-ADB Monitor Pro Max")
st.caption("Offline, post-execution analytics for Azure Data Factory + Databricks pipelines.")

runs_df = _runs()
flows_df = _flows()
eff_df = _efficiency()

# KPI row
if not runs_df.empty:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Runs", len(runs_df))
    failed = int(runs_df["status"].isin(["Failed", "Canceled"]).sum())
    c2.metric("Failed / Canceled", failed)
    c3.metric("Total DBUs", f"{runs_df['dbus_consumed'].sum():.1f}")
    c4.metric("Avg Duration (min)", f"{(runs_df['duration_seconds'].mean() / 60):.1f}")

st.divider()

# --------------------------------------------------------------------------- #
# TOP PANEL: charts
# --------------------------------------------------------------------------- #
st.header("Metric Trends")

left, right = st.columns(2)

with left:
    st.subheader("Data Flow Volume by Run")
    if not flows_df.empty:
        vol = (
            flows_df.groupby(["run_id", "status"])[
                ["rows_read", "rows_filtered", "rows_transformed", "rows_written"]
            ]
            .sum()
            .reset_index()
        )
        melted = vol.melt(
            id_vars=["run_id", "status"],
            value_vars=["rows_read", "rows_filtered", "rows_transformed", "rows_written"],
            var_name="metric",
            value_name="rows",
        )
        fig = px.bar(
            melted,
            x="run_id",
            y="rows",
            color="metric",
            barmode="group",
            title="Rows Read / Filtered / Transformed / Written",
        )
        fig.update_layout(xaxis_tickangle=-30, legend_title_text="Metric")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data-flow telemetry available.")

with right:
    st.subheader("Compute Cost (DBUs) Over Time")
    if not runs_df.empty:
        cost = runs_df.sort_values("start_time")
        fig2 = px.bar(
            cost,
            x="start_time",
            y="dbus_consumed",
            color="status",
            hover_data=["run_id", "pipeline_name", "duration_seconds"],
            title="DBUs Consumed per Run",
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No run history available.")

# Compute-to-storage efficiency
st.subheader("Compute-to-Storage Efficiency")
if not eff_df.empty:
    fig3 = go.Figure()
    fig3.add_bar(x=eff_df["snapshot_date"], y=eff_df["storage_growth_gb"], name="Storage Growth (GB)")
    fig3.add_trace(
        go.Scatter(
            x=eff_df["snapshot_date"],
            y=eff_df["total_dbus"],
            name="DBUs Consumed",
            yaxis="y2",
            mode="lines+markers",
        )
    )
    fig3.update_layout(
        title="Daily Storage Growth vs. DBUs Consumed",
        yaxis=dict(title="Storage Growth (GB)"),
        yaxis2=dict(title="DBUs", overlaying="y", side="right"),
        legend=dict(orientation="h"),
    )
    st.plotly_chart(fig3, use_container_width=True)
    with st.expander("Efficiency table (DBUs per GB added)"):
        st.dataframe(eff_df, use_container_width=True)
else:
    st.info("No storage telemetry available.")

# Run history + per-run lineage explorer
st.divider()
st.header("Run Explorer")
if not runs_df.empty:
    st.dataframe(
        runs_df[
            ["run_id", "pipeline_name", "status", "duration_seconds", "dbus_consumed", "error_message"]
        ],
        use_container_width=True,
    )
    run_ids = runs_df["run_id"].tolist()
    selected = st.selectbox("Inspect row-level lineage for a run:", run_ids)
    if selected:
        flow = analyze_pipeline_data_flow(selected)
        if flow.get("found"):
            st.write(
                f"**Pipeline:** {flow['pipeline_name']} | **Status:** {flow['status']} | "
                f"**Sources:** {', '.join(flow['source_tables']) or '—'} | "
                f"**Targets:** {', '.join(flow['target_tables']) or '—'}"
            )
            st.json(flow["totals"])
            st.dataframe(pd.DataFrame(flow["stages"]), use_container_width=True)
        else:
            st.warning(flow.get("error", "No lineage found for this run."))

# --------------------------------------------------------------------------- #
# BOTTOM PANEL: diagnostic chat
# --------------------------------------------------------------------------- #
st.divider()
st.header("🩺 Diagnostic Agent")
st.caption("Ask about run history, row-level lineage, or why a run failed/was canceled.")

if "messages" not in st.session_state:
    st.session_state.messages = []

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

prompt = st.chat_input("e.g. Why did RUN-2026-06-13-01 fail?")
if prompt:
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        try:
            from src.agent.diagnostic_agent import build_agent

            if "agent" not in st.session_state:
                st.session_state.agent = build_agent()
            with st.spinner("Analyzing telemetry..."):
                result = st.session_state.agent.invoke(
                    {"messages": [{"role": "user", "content": prompt}]}
                )
            out_messages = result.get("messages", [])
            answer = out_messages[-1].content if out_messages else "(no response)"
        except Exception as exc:  # noqa: BLE001 - surface config errors to the user
            answer = (
                f"⚠️ Agent unavailable: {exc}\n\n"
                "Set `LLM_PROVIDER` and the matching API key in your `.env` "
                "(see `.env.example`). The charts above work without an API key."
            )
        st.markdown(answer)
        st.session_state.messages.append({"role": "assistant", "content": answer})
