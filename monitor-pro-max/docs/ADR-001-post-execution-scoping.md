# ADR-001: Post-Execution (Offline) Analysis Scoping

- **Status:** Accepted
- **Date:** 2026-06-13
- **Deciders:** AI Acceleration Engineering / DataOps Architecture

## Context

"ADF-ADB Monitor Pro Max" must observe Azure Data Factory (ADF) pipelines and
Databricks (ADB) Spark jobs. There are two fundamentally different observability
postures:

1. **Live / streaming monitoring** — tail running jobs, subscribe to event hubs,
   poll the ADF/Databricks REST APIs in near real-time, and alert mid-flight.
2. **Post-execution (offline) forensics** — analyze runs that have already
   *completed, stopped, or been canceled* using the telemetry they left behind.

Live monitoring requires always-on infrastructure (event hub consumers,
websocket/poller daemons, alerting backends), introduces availability/SLO
obligations, and couples the tool tightly to platform rate limits and auth
session lifetimes.

## Decision

**We scope this tool to post-execution (offline) analysis only.**

The tool ingests *historical telemetry artifacts* that a finished run produces:

- ADF pipeline run details (status, duration, error messages).
- Databricks Spark SQL execution metrics + Delta Lake history (row-level flow).
- Compute (DBU) and storage growth telemetry over time.

It does **not** attach to running jobs, hold long-lived sessions, or emit live
alerts.

## Rationale

- **Determinism & reproducibility:** Completed runs produce immutable artifacts.
  Analysis is repeatable and testable, which suits a forensics/diagnostics tool.
- **Lower operational burden:** No always-on consumers, no SLOs on a live stream.
- **Cleaner testing story:** We can ship deterministic `mock_telemetry/` fixtures
  and assert on parser outputs (>60% coverage) without live credentials.
- **Decoupled ingestion:** The same parsers work on mock dumps today and on real
  ADF/Databricks exports tomorrow — only the *source adapter* changes.

## Consequences

- The agent answers "what happened and why" questions, not "what is happening now".
- A future ingestion adapter (ADF REST + Databricks Jobs/SQL API) can land
  artifacts into `TELEMETRY_DIR`; the analytics/agent layers stay unchanged.
- Real-time alerting, if ever needed, becomes a separate companion service.

## Alternatives Considered

- **Full live monitoring suite** — rejected for now: high ops cost, weak fit for a
  diagnostics/forensics use case, harder to test deterministically.
- **Hybrid (live + offline)** — deferred: start offline; add a thin live adapter
  later only if a concrete real-time requirement emerges.
