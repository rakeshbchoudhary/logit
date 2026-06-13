# ADR-002: Structured Event JSON as the Source of Truth for Row Counts

- **Status:** Accepted
- **Date:** 2026-06-13
- **Deciders:** AI Acceleration Engineering / DataOps Architecture

## Context

A core feature is **row-level data flow lineage**:

> Source Table → Rows Read → Rows Filtered → Rows Transformed → Target Table → Rows Written

There are two candidate sources for these row counts:

1. **Raw text logs** (Spark driver `stdout`/`stderr`, log4j output) — parse with
   regular expressions to scrape numbers like `read 100000 rows`.
2. **Structured event metrics** — Spark `SQLMetrics` / `SparkListener` event JSON
   and **Delta Lake** transaction `history()` / commit `operationMetrics`
   (`numOutputRows`, `numTargetRowsInserted`, `numCopiedRows`, etc.).

## Decision

**Row counts and lineage are derived exclusively from structured event JSON
(Spark SQL metrics + Delta Lake history). Raw text logs are used only for
*qualitative* failure diagnostics, never for *quantitative* row accounting.**

## Rationale

- **Accuracy:** Delta `operationMetrics` and Spark accumulators are the canonical,
  engine-emitted counters. Scraping text logs is lossy and order-dependent.
- **Stability:** Log line formats change across Spark/DBR versions and log levels
  (`INFO` lines may be suppressed entirely). JSON metric keys are part of a stable
  event schema.
- **Completeness:** Many counters (rows filtered by pushdown, rows copied during
  `MERGE`, files added/removed) never appear in human-readable logs at all.
- **Testability:** A JSON contract is trivial to fixture and assert against; regex
  scraping invites brittle, locale-sensitive tests.
- **Separation of concerns:** Quantitative truth (JSON) vs. qualitative narrative
  (logs). The `analyze_failed_or_canceled_run` tool reads logs for *why it broke*;
  the `get_pipeline_data_flow` tool reads JSON for *how many rows moved*.

## Consequences

- `data_flow_analyzer.py` consumes `pipeline_data_flow.json` (structured) only.
- `log_parser.py` consumes the dense Spark trace (`*.log`) for diagnostics text and
  must **not** be trusted for row counts.
- Real-world ingestion should call Delta `DESCRIBE HISTORY` / `_delta_log` commit
  JSON and the Spark REST `sql` endpoint, landing them as the same JSON schema.

## Alternatives Considered

- **Regex over raw logs** — rejected: brittle, incomplete, version-sensitive.
- **Re-running jobs with instrumentation** — rejected: violates the offline,
  post-execution scope (ADR-001) and is non-deterministic.
