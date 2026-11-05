# RAG_Pipeline

A Declarative Automation Bundle that ingests, parses, chunks, and indexes knowledge-base documents for RAG using Databricks AI functions and AI Search (Vector Search).

## What the Pipeline Does

The [RAG_KnowledgeBase_Document_Pipeline](src/rag_pipeline_job/RAG_KnowledgeBase_Document_Pipeline) notebook incrementally processes documents uploaded to the `agentic_ai.01_bronze/raw_documents` UC volume:

1. **Incremental discovery** — walks the volume and keeps only files that are new, changed (size/mtime), or previously failed; unchanged files are skipped.
2. **Parse & chunk** — parses each document with the `ai_parse_document` SQL AI function and chunks it with `ai_prep_search` (semantic, context-enriched chunks).
3. **Write to gold** — idempotent MERGE keyed on `chunk_id` into `agentic_ai.03_gold.final_chunk`, with stale-chunk cleanup for re-uploaded files.
4. **State tracking** — records per-file results in `agentic_ai.01_bronze.document_processing_log` so re-runs are safe and failures are isolated (one bad file never fails the run).
5. **Index sync** — triggers an AI Search sync for `agentic_ai.03_gold.document_index`, whose DELTA_SYNC index computes embeddings automatically.

## Bundle Resources

| Resource | Type | Description |
| --- | --- | --- |
| `rag_pipeline_job` | Lakeflow Job | "RAG Pipeline Job" — runs the notebook, triggered on file arrival in `/Volumes/agentic_ai/01_bronze/raw_documents/` (max 1 concurrent run, queue enabled) |
| `RAG_KnowledgeBase_Document_Pipeline` | Notebook | Document parsing/chunking/indexing pipeline (see `src/rag_pipeline_job/`) |

### Job Parameters

The job overrides the notebook's widgets via task parameters:

| Parameter | Default |
| --- | --- |
| `volume_path` | `/Volumes/agentic_ai/01_bronze/raw_documents` |
| `index_name` | `agentic_ai.03_gold.document_index` |
| `chunk_size` | `512` |
| `chunk_overlap` | `20` |
| `log_table` | `agentic_ai.01_bronze.document_processing_log` |

Supported file types: `doc`, `docx`, `jpeg`, `jpg`, `md`, `pdf`, `png`, `ppt`, `pptx`, `tif`, `tiff`, `txt`, `xls`, `xlsx`.

## Deployment Targets

The bundle defines two targets in `databricks.yml` (both use catalog `workspace`):

- **dev** (default): development mode — deployed resources get a `[dev <user>]` prefix and schedules/triggers are paused.
- **prod**: production mode — single deployed copy with `CAN_MANAGE` permissions for the owner, schema `prod`.

## Getting Started

### 1. Deployment

- Click the **deployment rocket** 🚀 in the left sidebar to open the **Deployments** panel, then click **Deploy**.

### 2. Running the Job

- The job runs automatically when a new file arrives in `/Volumes/agentic_ai/01_bronze/raw_documents/` (file-arrival trigger).
- To run it manually, hover over **RAG Pipeline Job** in the **Deployments** panel and click the **Run** button.

### 3. Managing Resources

- Use the **Add** dropdown to add resources to the bundle.
- Click **Schedule** on a notebook within the bundle to create a **job definition** that schedules the notebook.

## Documentation

- For information on using **Declarative Automation Bundles in the workspace**, see: [Declarative Automation Bundles in the workspace](https://docs.databricks.com/aws/en/dev-tools/bundles/workspace-bundles)
- For details on the **Declarative Automation Bundles format** used in this bundle, see: [Declarative Automation Bundles Configuration reference](https://docs.databricks.com/aws/en/dev-tools/bundles/reference)
