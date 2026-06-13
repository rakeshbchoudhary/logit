"""Tool-calling conversational diagnostic agent.

Routes post-execution diagnostics questions to the custom analytics tools using
either Anthropic Claude 3.5 Sonnet or OpenAI GPT-4o, selected via ``LLM_PROVIDER``.
"""
from __future__ import annotations

import os
from typing import List, Optional

from dotenv import load_dotenv
from langchain.agents import create_agent

from .tools import ALL_TOOLS

load_dotenv()

SYSTEM_PROMPT = """You are "ADF-ADB Monitor Pro Max", an expert DataOps diagnostics \
assistant for Azure Data Factory and Databricks pipelines.

You analyze ONLY historical, post-execution telemetry (completed, stopped, or \
canceled runs). You never claim to observe live/running jobs.

You have three tools:
- get_historical_runs_summary: overall timeline of statuses, durations, and DBU costs.
- get_pipeline_data_flow: row-level lineage for one Run ID (read -> filtered -> transformed -> written).
- analyze_failed_or_canceled_run: plain-English failure diagnosis from a Run ID's Spark trace.

Guidance:
- Always call the appropriate tool(s) before asserting numbers; never invent row counts or costs.
- When asked "why did a run fail", call analyze_failed_or_canceled_run.
- When asked about row counts / lineage, call get_pipeline_data_flow.
- Cite concrete numbers and table names from tool output.
- Be concise, structured, and actionable.
"""


class MissingAPIKeyError(RuntimeError):
    """Raised when the selected provider has no API key configured."""


def _build_llm():
    """Instantiate the chat model for the configured provider."""
    provider = os.getenv("LLM_PROVIDER", "anthropic").strip().lower()

    if provider == "anthropic":
        if not os.getenv("ANTHROPIC_API_KEY"):
            raise MissingAPIKeyError(
                "LLM_PROVIDER=anthropic but ANTHROPIC_API_KEY is not set. See .env.example."
            )
        from langchain_anthropic import ChatAnthropic

        return ChatAnthropic(
            model=os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-20241022"),
            temperature=0,
        )

    if provider == "openai":
        if not os.getenv("OPENAI_API_KEY"):
            raise MissingAPIKeyError(
                "LLM_PROVIDER=openai but OPENAI_API_KEY is not set. See .env.example."
            )
        from langchain_openai import ChatOpenAI

        return ChatOpenAI(model=os.getenv("OPENAI_MODEL", "gpt-4o"), temperature=0)

    raise ValueError(f"Unsupported LLM_PROVIDER '{provider}'. Use 'anthropic' or 'openai'.")


def build_agent(model=None):
    """Construct a tool-calling agent (LangChain v1 ``create_agent`` graph).

    Args:
        model: Optional pre-built chat model (useful for tests/mocks). If omitted,
               one is created from environment configuration.

    Returns:
        A compiled agent graph ready to ``invoke`` with a ``messages`` payload.
    """
    if model is None:
        model = _build_llm()
    return create_agent(model, ALL_TOOLS, system_prompt=SYSTEM_PROMPT)


def ask_agent(question: str, chat_history: Optional[List] = None, agent=None) -> str:
    """Single-turn convenience wrapper. Returns the agent's text answer."""
    executor = agent or build_agent()
    messages = list(chat_history or [])
    messages.append({"role": "user", "content": question})
    result = executor.invoke({"messages": messages})
    out_messages = result.get("messages", [])
    return out_messages[-1].content if out_messages else ""
