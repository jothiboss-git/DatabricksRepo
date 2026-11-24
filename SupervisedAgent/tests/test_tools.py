"""Tool-level tests for the local agent tools: contracts, error handling, approval gating.

Hermetic tests mock the Vector Search client and run anywhere. The live test queries the real
``agentic_ai.03_gold.search_index`` index and skips unless Databricks auth is configured
(DATABRICKS_HOST + DATABRICKS_TOKEN, or a config profile) — run it where `agentbricks dev` works.
"""

import asyncio
import json
import os
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from agent.agent import REQUIRE_APPROVAL
from agent.tools import all_tools
from agent.tools.knowledge_search import INDEX_NAME, QUERY_COLUMNS, search_knowledge
from agent.tools.sample_tool import get_current_time
from agent.tools.send_message import send_message
from agents import FunctionTool


def invoke(tool: FunctionTool, **kwargs) -> str:
    """Invoke a FunctionTool's wrapped function the way the Agents SDK would."""
    ctx = SimpleNamespace(
        tool_name=tool.name,
        run_config=SimpleNamespace(trace_include_sensitive_data=True),
    )
    return asyncio.run(tool.on_invoke_tool(ctx, json.dumps(kwargs)))


def fake_response(rows: list) -> SimpleNamespace:
    return SimpleNamespace(result=SimpleNamespace(data_array=rows))


# --- get_current_time ---


def test_get_current_time_returns_iso_datetime():
    out = invoke(get_current_time)
    datetime.fromisoformat(out)  # raises if not a valid ISO 8601 timestamp


# --- search_knowledge (hermetic) ---


def test_search_knowledge_queries_index_with_expected_arguments():
    client = MagicMock()
    client.vector_search_indexes.query_index.return_value = fake_response(
        [["c1", "report.pdf", "chunk text", ["uri"], 0.87]]
    )
    with patch("agent.tools.knowledge_search.workspace_client", return_value=client):
        out = json.loads(invoke(search_knowledge, query="what is in the report?", num_results=5))
    client.vector_search_indexes.query_index.assert_called_once_with(
        index_name=INDEX_NAME,
        columns=QUERY_COLUMNS,
        query_text="what is in the report?",
        num_results=5,
    )
    assert out == {
        "results": [
            {
                "chunk_id": "c1",
                "file_name": "report.pdf",
                "chunk_to_retrieve": "chunk text",
                "page_uris": ["uri"],
                "score": 0.87,
            }
        ]
    }


def test_search_knowledge_surfaces_unavailable_index_gracefully():
    client = MagicMock()
    client.vector_search_indexes.query_index.side_effect = RuntimeError(
        "Vector index agentic_ai.03_gold.search_index is not ready."
    )
    with patch("agent.tools.knowledge_search.workspace_client", return_value=client):
        out = invoke(search_knowledge, query="anything")
    # The tool must not raise: the model gets a message it can relay to the user.
    assert "Knowledge search is unavailable" in out
    assert "not ready" in out


def test_search_knowledge_reports_no_matches():
    client = MagicMock()
    client.vector_search_indexes.query_index.return_value = fake_response([])
    with patch("agent.tools.knowledge_search.workspace_client", return_value=client):
        out = json.loads(invoke(search_knowledge, query="nothing matches"))
    assert out["results"] == []
    assert "No matching passages" in out["message"]


def test_search_knowledge_pads_short_rows():
    client = MagicMock()
    client.vector_search_indexes.query_index.return_value = fake_response(
        [["c1", "a.pdf", None, None, 0.5]]
    )
    with patch("agent.tools.knowledge_search.workspace_client", return_value=client):
        out = json.loads(invoke(search_knowledge, query="x"))
    row = out["results"][0]
    assert row["score"] == 0.5
    assert row["chunk_to_retrieve"] is None and row["page_uris"] is None


# --- approval gating ---


def test_send_message_is_gated_by_approval():
    assert send_message.needs_approval is True
    assert "send_message" in REQUIRE_APPROVAL


def test_read_tools_do_not_require_approval():
    assert get_current_time.needs_approval is False
    assert search_knowledge.needs_approval is False
    assert "get_current_time" not in REQUIRE_APPROVAL
    assert "search_knowledge" not in REQUIRE_APPROVAL


def test_local_tools_registry():
    names = {t.name for t in all_tools()}
    assert {"get_current_time", "search_knowledge", "send_message"} <= names


# --- live index test ---


@pytest.mark.skipif(
    not (
        os.getenv("DATABRICKS_HOST")
        and (os.getenv("DATABRICKS_TOKEN") or os.getenv("DATABRICKS_CONFIG_PROFILE"))
    ),
    reason="live test needs Databricks auth (DATABRICKS_HOST/TOKEN) for agentic_ai.03_gold.search_index",
)
def test_search_knowledge_live_index():
    out = json.loads(invoke(search_knowledge, query="what is this document about?", num_results=3))
    assert out["results"], "index has 11 rows; expected at least one passage back"
    first = out["results"][0]
    assert set(first) == {"chunk_id", "file_name", "chunk_to_retrieve", "page_uris", "score"}
    print(json.dumps(out, indent=2)[:2000])
