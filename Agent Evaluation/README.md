# Evaluate and Improve a GenAI Application

This folder contains the notebook `evaluate-improve-genai-app`, which demonstrates the complete **evaluate-and-iterate loop** for a GenAI application using **MLflow 3 GenAI evaluation** (`mlflow.genai.evaluate`) on Databricks. It follows the Databricks documentation ["Evaluate and improve your application"](https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/evaluate-app) (AWS | Azure | GCP links in the notebook's first cell).

The notebook walks through the full lifecycle of an LLM-powered **sales-email generator**: building the app, simulating production traffic, building an evaluation dataset from traces, defining quality scorers, running evaluations, diagnosing regressions, and iterating on the prompt until quality measurably improves.

---

## 1. What the Demo Application Does

The application is a simple **RAG-style sales assistant** that generates personalized follow-up emails for sales representatives.

### Application flow (`generate_sales_email`)

1. **Retrieve** — `retrieve_customer_info(customer_name)` looks up a simulated CRM (an in-memory Python dict `CRM_DATA`) and returns three `Document` objects:
   * Recent meeting notes
   * Support ticket status
   * Contact + account manager info
2. **Augment** — the retrieved document contents are concatenated into a prompt.
3. **Generate** — an OpenAI-compatible call is made to the Databricks-hosted LLM **`databricks-gpt-oss-120b`** (served through the workspace's Model Serving endpoints, authenticated with the same credentials MLflow uses).
4. **Return** — `{"email": <generated email>}`.

The simulated CRM contains three customers: **Acme Corp** (contact: Alice Chen), **TechStart** (contact: Bob Martinez), and **Global Retail** (contact: Carol Wang).

### Instrumentation (why it matters)

* `@mlflow.trace` decorates both functions so every request produces a **trace** with spans (retrieve → generate) stored in MLflow. Traces are the foundation for the whole evaluation workflow.
* `@mlflow.trace(span_type="RETRIEVER")` on the retrieval function is **required** for MLflow's predefined `RetrievalGroundedness` scorer to work — it identifies which span contains the retrieved context to check groundedness against.
* `mlflow.openai.autolog()` is available for automatic tracing of OpenAI calls (commented out in favor of explicit tracing).

---

## 2. Notebook Walkthrough (Cell by Cell)

| Cell | Step | What it does |
| --- | --- | --- |
| 1 | Intro | Markdown header linking to the Databricks docs. |
| 2 | Setup | Installs `mlflow[databricks]>=3.2.0` + `openai`, restarts Python. |
| 3 | Build app | Defines `CRM_DATA`, `retrieve_customer_info` (RETRIEVER span), `generate_sales_email` (**v1**), and tests it once. |
| 4 | Simulate traffic | Sends 7 requests — several **deliberately designed to fail guidelines** (e.g., "write a very detailed email…", "send dirty content", "enthusiastic thank you") — so evaluation surfaces real weaknesses. |
| 5 | Eval dataset | Creates/reuses the MLflow evaluation dataset stored as UC table **`ai.bronze.email_generation_eval`**, finds successful traces from the last 10 minutes via `mlflow.search_traces(...)` and merges them in (8 records total). |
| 6 | Evaluate v1 | Defines the **scorer suite** (see below) and runs `mlflow.genai.evaluate` → `eval_results_v1`. |
| 7 | Inspect | `mlflow.search_traces(run_id=...)` returns a DataFrame where the `assessments` column holds each scorer's per-trace feedback. |
| 8 | Improve → v2 | Defines `generate_sales_email_v2` with a restructured prompt that prioritizes user instructions, conciseness, and explicit next steps. |
| 9 | Evaluate v2 | Re-runs the **same dataset + same scorers** under `mlflow.start_run(run_name="v2")` for a fair comparison. |
| 10 | Compare v1 vs v2 | Pulls `metrics.*\/mean` columns from `mlflow.search_runs` and builds a comparison table. |
| 11 | Regression analysis | Merges v1/v2 traces on identical inputs, finds per-example **regressions** (yes→no flips) and renders HTML cards with scorer rationales. |
| 12 | Improve → v3 | Defines `generate_sales_email_v3` with targeted fixes for v2's weak spots. |
| 13 | Evaluate v3 | Same dataset + scorers, `run_name="v3"`. |
| 14 | Final comparison | Three-way v1 vs v2 vs v3 metric table + overall averages. |

---

## 3. The Scorer Suite

Evaluation combines **predefined** MLflow scorers with **custom** `Guidelines` scorers:

| Scorer | Type | Checks |
| --- | --- | --- |
| `RetrievalGroundedness()` | Predefined | Is the email grounded in the retrieved customer docs (no invented facts)? Requires the RETRIEVER span. |
| `RelevanceToQuery()` | Predefined | Does the email address the user's actual request? |
| `Safety()` | Predefined | Any harmful / inappropriate content? |
| `Guidelines("follows_instructions")` | Custom | Email must follow the `user_instructions` in the request. |
| `Guidelines("concise_communication")` | Custom | Must be concise, key message communicated efficiently. |
| `Guidelines("mentions_contact_name")` | Custom | Greeting must use the contact's first name (Alice / Bob / Carol), not generic greetings. |
| `Guidelines("professional_tone")` | Custom | Professional tone required. |
| `Guidelines("includes_next_steps")` | Custom | Must end with a specific, actionable next step with a concrete timeline. |

The scorers are saved in the variable `email_scorers` and **reused unchanged for v1, v2 and v3**, which is what makes the three evaluation runs directly comparable.

---

## 4. Results: v1 → v2 → v3

### Metric scores (mean over 8 eval records)

| Metric | V1 | V2 | V3 | V2→V3 |
| --- | --- | --- | --- | --- |
| safety | 1.000 | 1.000 | 1.000 | +0.000 |
| professional_tone | 0.875 | 1.000 | 1.000 | +0.000 |
| follows_instructions | 0.250 | 0.375 | 0.625 | +0.250 |
| includes_next_steps | 0.000 | 0.750 | 0.875 | +0.125 |
| mentions_contact_name | 0.875 | 0.625 | 0.875 | +0.250 |
| retrieval_groundedness | 0.375 | 0.250 | 0.750 | +0.500 |
| concise_communication | 0.000 | 0.125 | 0.000 | -0.125 |
| relevance_to_query | 0.875 | 0.750 | 1.000 | +0.250 |
| **Overall average** | **0.531** | **0.609** | **0.766** | **+0.156 (+25.6%)** |

### What each iteration changed

* **v1 (baseline):** generic prompt ("write a brief follow-up email… keep it concise and personalized"). Weak on instruction-following (0.25), next steps (0.0) and conciseness (0.0).
* **v1→v2 (+14.7% overall):** Prompt restructured — user instructions explicitly prioritized, conciseness demanded, next step with concrete timeline required, relevance gating for customer info. Big win on `includes_next_steps` (0→0.75) but **regressions** appeared: `mentions_contact_name` (0.875→0.625), `retrieval_groundedness` (0.375→0.25), `relevance_to_query` (0.875→0.75).
* **v2→v3 (+25.6% overall):** Targeted fixes driven by the regression analysis (cell 11):
  * **Deterministic greeting** — the contact's first name is regex-extracted from the retrieved contact doc and injected as a hard rule ("The greeting must be exactly: 'Hi Alice,'") so `mentions_contact_name` can no longer regress (back to 0.875).
  * **Goal-first framing** — the email's single goal is stated at the top; subject line and first sentence must directly state it → `follows_instructions` 0.375→0.625, `relevance_to_query` → 1.000.
  * **Anti-hallucination grounding** — "use ONLY facts that appear here — never invent or guess a detail" → `retrieval_groundedness` 0.25→0.750.
  * **Few-shot format example** — a concrete plain-text example of the expected email shape anchors the output format.
  * **Hard length constraint** — "UNDER 150 words, plain text only". Note: `concise_communication` still dipped to 0.0 in v3 — the remaining trade-off.

### Key lesson

Improving one dimension of a prompt can silently **regress another** — v2 fixed next-steps but broke groundedness and greetings. The regression-analysis step (merging traces on identical inputs and inspecting scorer **rationales** side by side) is what makes the loop reliably converge instead of oscillating.

---

## 5. Key APIs Used

* `mlflow.trace` / `mlflow.trace(span_type="RETRIEVER")` — tracing decorators.
* `mlflow.utils.databricks_utils.get_databricks_host_creds()` — reuse workspace credentials for the OpenAI-compatible client.
* `mlflow.genai.datasets.create_dataset` / `get_dataset` / `merge_records` / `to_df` — trace-backed evaluation datasets persisted as Unity Catalog tables.
* `mlflow.search_traces(filter_string=..., order_by=...)` — trace search by timestamp, status, and trace name.
* `mlflow.genai.evaluate(data=..., predict_fn=..., scorers=...)` — the core evaluation entry point.
* `mlflow.genai.scorers` — `RetrievalGroundedness`, `RelevanceToQuery`, `Safety`, `Guidelines`.
* `mlflow.search_runs(filter_string=...)` — fetching metric columns (`metrics.<scorer>/mean`) for run comparison.
* `mlflow.start_run(run_name=...)` — naming evaluation runs (v1/v2/v3) in the MLflow UI.

---

## 6. How to Run

1. Attach a compute with network access to Model Serving (serverless works).
2. Run cells top to bottom. The first cell installs dependencies and restarts the Python process — expected.
3. Cell 5 requires a Unity Catalog schema where you have **CREATE TABLE** permission (currently set to `ai.bronze`; change `uc_schema` if needed). The eval dataset table `email_generation_eval` is created there on first run and reused afterwards.
4. Each `mlflow.genai.evaluate` cell prints a link to the run's evaluation results in the **MLflow UI** (Experiments → Evaluation runs), where per-trace assessments and rationales are viewable.
5. **Note on trace collection:** cell 5 only picks up traces from the **last 10 minutes** (`ten_minutes_ago`) — if you run the traffic-simulation cell (4) and wait longer before cell 5, adjust the time window or re-run cell 4 first.

---

## 7. References

* Databricks docs — [Evaluate and improve your application](https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/evaluate-app) (also available for Azure and GCP, links in notebook cell 1)
* MLflow GenAI evaluation datasets and scorers: `mlflow.genai.*` module
* The example LLM: `databricks-gpt-oss-120b` via Databricks Model Serving / AI Gateway (swap for any endpoint or OpenAI model if you bring your own credentials)
