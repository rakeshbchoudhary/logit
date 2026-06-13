# ADF-ADB Monitor Pro Max

> **Offline, post-execution analytics for Azure Data Factory (ADF) + Azure Databricks (ADB) pipelines.**

Monitor Pro Max ingests the telemetry that *completed, stopped, or canceled* runs
leave behind — ADF orchestration metadata, Databricks Spark SQL execution metrics,
and Delta Lake storage history — and turns it into:

- **Processing bottleneck analysis** (durations, slow stages).
- **Row-level data flow lineage** (Source → Rows Read → Filtered → Transformed → Target → Written).
- **Compute-to-storage cost efficiency** (DBUs consumed vs. storage growth).
- **Plain-English failure diagnostics** via a tool-calling LLM agent.

This is a **forensics / post-mortem** tool, not a live monitor. See
[`docs/ADR-001-post-execution-scoping.md`](docs/ADR-001-post-execution-scoping.md).

---

## Architecture

```
            ┌──────────────────────────────────────────────┐
            │              Streamlit Dashboard               │
            │   Top: Plotly data-flow + cost charts          │
            │   Bottom: Diagnostic chat (tool-calling agent)  │
            └───────────────┬───────────────┬────────────────┘
                            │               │
                  ┌─────────▼──────┐  ┌──────▼───────────────┐
                  │  Analytics     │  │  LangChain Agent      │
                  │  (Pandas)      │  │  (Claude 3.5 / GPT-4o)│
                  │  log_parser    │  │  3 custom @tools      │
                  │  data_flow     │  └──────┬───────────────┘
                  └─────────┬──────┘         │
                            │   reads        │ calls
                  ┌─────────▼────────────────▼──────────────┐
                  │            mock_telemetry/                │
                  │  historical_runs_db.json                  │
                  │  pipeline_data_flow.json                  │
                  │  storage_growth_metrics.csv               │
                  │  spark_error_trace_04.log                 │
                  └───────────────────────────────────────────┘
```

### Custom agent tools

| Tool | Purpose |
|------|---------|
| `get_historical_runs_summary` | Timeline of execution statuses, durations, and costs. |
| `get_pipeline_data_flow` | Row-level lineage for one Run ID (read → filtered → transformed → written). |
| `analyze_failed_or_canceled_run` | Reads the Spark driver trace for a Run ID and returns a plain-English diagnostic. |

---

## Project layout

```
monitor-pro-max/
├── .github/workflows/ci.yml      # pytest on every push/PR
├── mock_telemetry/               # deterministic offline fixtures
├── src/
│   ├── analytics/                # Pandas parsers (log + data flow)
│   ├── agent/                    # LangChain tools + diagnostic agent
│   └── app.py                    # Streamlit dashboard
├── tests/                        # pytest (>60% coverage target)
├── docs/                         # Architecture Decision Records
├── requirements.txt
└── .env.example
```

---

## Quickstart

```bash
# 1. Create & activate a virtual env (Python 3.11+)
python3.11 -m venv .venv
source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure credentials
cp .env.example .env
#   then edit .env: set LLM_PROVIDER + the matching API key

# 4. Launch the dashboard
streamlit run src/app.py
```

The dashboard works **without** an API key for the charts and parsers; only the
chat agent requires a valid LLM key.

---

## Configuration

All config is loaded from `.env` (see `.env.example`):

| Variable | Description |
|----------|-------------|
| `LLM_PROVIDER` | `anthropic` or `openai`. |
| `ANTHROPIC_API_KEY` / `ANTHROPIC_MODEL` | Claude 3.5 Sonnet credentials. |
| `OPENAI_API_KEY` / `OPENAI_MODEL` | GPT-4o credentials. |
| `TELEMETRY_DIR` | Where telemetry artifacts live (default `mock_telemetry`). |
| `ADF_*`, `AZURE_*`, `DATABRICKS_*` | Live-integration placeholders (offline by default). |

---

## Testing

```bash
pytest                      # run the suite
pytest --cov=src --cov-report=term-missing   # with coverage
```

CI runs the same suite automatically via
[`.github/workflows/ci.yml`](.github/workflows/ci.yml).

---

## Connecting real ADF + Databricks (roadmap)

The analytics and agent layers are decoupled from ingestion. To go live:

1. Export ADF run details via the **ADF REST API** (`Pipeline Runs - Query By Factory`).
2. Export Spark SQL metrics + Delta `DESCRIBE HISTORY` via the **Databricks Jobs/SQL API**.
3. Land them in `TELEMETRY_DIR` using the same JSON/CSV schema as `mock_telemetry/`.

No changes to `src/analytics` or `src/agent` are required — only a new source adapter.
Row counts always come from structured event JSON, never scraped logs
(see [`docs/ADR-002-data-flow-metrics.md`](docs/ADR-002-data-flow-metrics.md)).

---

## License

Internal tooling scaffold — add your organization's license here.
