"""Knowledge-search tool backed by a Databricks AI Search (Vector Search) index.

Answers questions from the user's own ingested documents: chunks of
``agentic_ai.03_gold.final_chunk`` (file_name, chunk_id, chunk_to_embed, chunk_to_retrieve,
page_uris), embedded with ``databricks-qwen3-embedding-0-6b`` and indexed in
``agentic_ai.03_gold.search_index`` on endpoint ``search_rag_new``.

Importing this module needs no Databricks auth (the workspace client is resolved only when the
tool runs), so hermetic tests keep working. The agent tool is sync; the Agents SDK runs sync
function tools in a worker thread, so the blocking REST call doesn't stall the event loop.
"""

import json

import mlflow
from agents import function_tool
from mlflow.entities import SpanType

from databricks_agentkit import workspace_client

# Fully qualified index name and the columns returned to the model. `chunk_to_retrieve` is the
# text chunk to show; the API always appends the similarity score as the last value in each row.
INDEX_NAME = "agentic_ai.03_gold.search_index"
QUERY_COLUMNS = ["chunk_id", "file_name", "chunk_to_retrieve", "page_uris"]
# What the model-facing JSON keeps per chunk (the extra "content" key is for the trace only).
TOOL_RESULT_KEYS = ("chunk_id", "file_name", "chunk_to_retrieve", "page_uris", "score")


@mlflow.trace(span_type=SpanType.RETRIEVER)
def _traced_query_index(query: str, num_results: int) -> list[dict]:
    """RETRIEVER-span wrapper: runs only inside an active trace (the agent run opens one)."""
    return _fetch_chunks(query, num_results)


def _query_index(query: str, num_results: int) -> list[dict]:
    """Query the vector index, opening a RETRIEVER span when a trace is active.

    MLflow's retrieval scorers (RetrievalGroundedness / RetrievalRelevance / RetrievalSufficiency)
    read the retrieved context from the last RETRIEVER span's output, so it must be the list of
    chunk dicts, each carrying a recognized text key ("content"). Standalone calls (hermetic
    tests) skip span creation so no stray traces get exported.
    """
    if mlflow.get_active_trace_id() is None:
        return _fetch_chunks(query, num_results)
    return _traced_query_index(query, num_results)


def _fetch_chunks(query: str, num_results: int) -> list[dict]:
    """Call the index and map rows to chunk dicts."""
    response = workspace_client().vector_search_indexes.query_index(
        index_name=INDEX_NAME,
        columns=QUERY_COLUMNS,
        query_text=query,
        num_results=num_results,
    )
    rows = (response.result and response.result.data_array) or []
    chunks = []
    for row in rows:
        # Pad short rows defensively, then zip columns + appended score into a dict.
        values = list(row) + [None] * (len(QUERY_COLUMNS) + 1 - len(row))
        chunk = dict(zip(QUERY_COLUMNS + ["score"], values, strict=False))
        chunk["content"] = chunk["chunk_to_retrieve"]  # the text key MLflow judges evaluate
        chunks.append(chunk)
    return chunks


@function_tool
def search_knowledge(query: str, num_results: int = 5) -> str:
    """Search the user's own knowledge base (their ingested documents) for relevant passages.

    Use this for any question about the user's documents — reports, notes, policies — instead of
    answering from general knowledge. Returns the most similar text chunks with their source file
    name and page URIs.

    Args:
        query: The question or topic to look up, phrased in the user's words.
        num_results: How many passages to return (default 5, max ~20).
    """
    try:
        chunks = _query_index(query, num_results)
    except Exception as error:
        # Most common while the endpoint is provisioning or the pipeline hasn't synced yet — a
        # raw traceback would only confuse the model. Surface a short, actionable message.
        return (
            f"Knowledge search is unavailable: {type(error).__name__}: {error}. "
            "Tell the user their document index could not be queried, and only answer from "
            "general knowledge if they accept it."
        )

    if not chunks:
        return json.dumps({"results": [], "message": "No matching passages in the knowledge base."})

    results = [{key: chunk[key] for key in TOOL_RESULT_KEYS} for chunk in chunks]
    return json.dumps({"results": results}, ensure_ascii=False, default=str)
