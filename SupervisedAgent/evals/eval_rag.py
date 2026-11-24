"""RAG quality evaluation for the knowledge-search agent.

Runs the real agent end to end on an eval set of questions about the indexed document
(agentic_ai.03_gold.search_index over "Content Server 20.3 installation.docx"), then scores each
answer with MLflow's built-in retrieval scorers plus two deterministic checks:

* RetrievalGroundedness — is the answer implied by the retrieved passages (no hallucination)?
* RetrievalRelevance   — were the retrieved chunks relevant to the question?
* RetrievalSufficiency  — did the retrieved chunks contain enough information to answer?
* cites_source_or_disclaims — answer cites the source file, or clearly says nothing was found
* retriever_span_present — instrumentation contract: the trace carries a RETRIEVER span

Run it where Databricks auth is configured (the same place `agentbricks dev` works):

    uv run python evals/eval_rag.py

The eval set is 6 cases over an 11-chunk index — a smoke signal, not a statistically
meaningful quality estimate. Grow EVAL_SET as more documents are indexed.
"""

import asyncio
import os
from uuid import uuid4

import mlflow
from mlflow.entities import Feedback
from mlflow.genai.scorers import (
    RetrievalGroundedness,
    RetrievalRelevance,
    RetrievalSufficiency,
    scorer,
)

from agent.agent import configure, run_agent

# Same experiment agentbricks uses for tracing (agent.toml [tracing] experiment_name), so the
# eval traces and the interactive traces land together.
EXPERIMENT = "/Shared/agentbricks_traces/newagent-oqmsfn"

SOURCE_FILE = "content server 20.3"  # lowercased; the indexed doc's name (models may cite it without ".docx")

EVAL_SET = [
    {
        "inputs": {"query": "How do I fix the SMTP connection failure during Content Server install?"},
        "expectations": {"expected_facts": ["open IIS6", "start the service", "SMTP automatic"]},
    },
    {
        "inputs": {"query": "What do I do after the Content Server installation finishes?"},
        "expectations": {"expected_facts": ["restart the VM", "docabase", "IDQL"]},
    },
    {
        "inputs": {"query": "Which tool do I use to validate the docbase connection and how?"},
        "expectations": {"expected_facts": ["IDQL", "user name", "password"]},
    },
    {
        "inputs": {"query": "Where is the SMTP host configured?"},
        "expectations": {"expected_facts": ["Host Name", "SMTP"]},
    },
    {
        "inputs": {"query": "What comes after the content server installation is completed and tested?"},
        "expectations": {"expected_facts": ["upgrade content server to 20"]},
    },
    {
        # No-match control: the question is unrelated to the indexed document. The agent must say
        # nothing relevant was found rather than answer from general knowledge (hallucination risk).
        "inputs": {"query": "What is the capital of France?"},
    },
]


@scorer
def cites_source_or_disclaims(outputs) -> Feedback:
    """Deterministic: the answer cites the source file, or clearly disclaims finding nothing."""
    answer = str(outputs)
    lower = answer.lower()
    disclaimers = (
        "no matching", "no relevant", "not find", "couldn't find", "does not contain",
        "doesn't contain", "nothing in", "not covered",
    )
    if SOURCE_FILE in lower:
        return Feedback(value=True, rationale="Answer cites the indexed source file.")
    if any(d in lower for d in disclaimers):
        return Feedback(value=True, rationale="Answer explicitly says nothing relevant was found.")
    return Feedback(
        value=False,
        rationale="Answer neither cites the source file nor says nothing was found.",
    )


@scorer
def retriever_span_present(trace) -> Feedback:
    """Deterministic: the trace carries a RETRIEVER span (required by the retrieval scorers)."""
    spans = trace.data.spans if trace.data else []
    types = [str(s.span_type) for s in spans]
    has_retriever = any("RETRIEVER" in t.upper() for t in types)
    return Feedback(
        value=has_retriever,
        rationale=f"Span types in trace: {types}.",
    )


async def _ask(query: str) -> str:
    """Run one agent turn and return the final assistant message."""
    from agents import ItemHelpers
    from agents.items import MessageOutputItem

    final = ""
    async with run_agent(
        [{"role": "user", "content": query}],
        session_id=f"eval-{uuid4()}",  # fresh session per case so memory never leaks across cases
        actor="rag-eval",
    ) as result:
        async for event in result.stream_events():
            if event.type == "run_item_stream_event" and isinstance(event.item, MessageOutputItem):
                text = ItemHelpers.text_message_output(event.item)
                if text:
                    final = text
    return final or "(no answer produced)"


def predict_fn(query: str) -> str:
    """mlflow.genai.evaluate calls this per case; returns the agent's answer for the query."""
    return asyncio.run(_ask(query))


def main() -> None:
    # Make agentkit's start_trace active when run standalone (agentbricks dev sets these itself).
    os.environ.setdefault("MLFLOW_TRACKING_URI", "databricks")
    os.environ.setdefault("MLFLOW_EXPERIMENT_NAME", EXPERIMENT)
    configure()  # wire auth + model client + tracing once
    mlflow.set_experiment(EXPERIMENT)

    scorers = [
        RetrievalGroundedness(),
        RetrievalRelevance(),
        RetrievalSufficiency(),
        cites_source_or_disclaims,
        retriever_span_present,
    ]
    result = mlflow.genai.evaluate(data=EVAL_SET, predict_fn=predict_fn, scorers=scorers)

    print("\n================ EVAL SUMMARY ================")
    run_id = getattr(result, "run_id", None) or "(see MLflow UI for the run)"
    print(f"Eval run: {run_id}")
    for metric, value in sorted((result.metrics or {}).items()):
        if value is not None:
            print(f"  {metric}: {value}")

    table = result.tables.get("eval_results")
    if table is not None:
        print("\n---------------- PER CASE ----------------")
        for row in table.to_dict(orient="records"):
            query = (row.get("inputs") or {}).get("query", "")
            print(f"\nQ: {query}")
            for key, value in row.items():
                if key in ("inputs", "outputs", "expectations") or value is None:
                    continue
                print(f"  {key}: {value}")
            answer = str(row.get("outputs", ""))
            print(f"  A: {answer[:300]}")


if __name__ == "__main__":
    main()
