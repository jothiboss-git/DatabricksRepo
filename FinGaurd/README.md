# FinGuard — Real-Time Fraud Detection & Alerting Platform

FinGuard is an end-to-end, real-time financial fraud detection system built entirely on Databricks. It ingests live transaction events from a Confluent Cloud Kafka topic, processes them through a **medallion architecture** (Bronze → Silver → Gold) using Lakeflow Spark Declarative Pipelines (SDP), detects two classes of fraud risk — **high-value transactions** and **watchlist (blacklisted card) matches** — and automatically notifies affected customers by email. A fraud monitoring dashboard provides real-time visibility for analysts.

---

## 1. Architecture Overview

```
Confluent Cloud Kafka (topic: my_topic)          fraud_watchlist CSV/JSON files
              │                                            (Unity Catalog Volume)
              │ spark.readStream (Kafka)                          │ Auto Loader (cloudFiles)
              ▼                                                   ▼
   ┌─────────────────────── BRONZE ───────────────────────────────────┐
   │  finguard.bronze.transaction        finguard.bronze.fraud_     │
   │  (raw Kafka JSON payload)            watchlist_data (raw)      │
   │                                      finguard.bronze.customers │
   └──────────────────────────────┬────────────────────────────────┘
                                  │  parse, clean, type-cast
                                  ▼
   ┌─────────────────────── SILVER ──────────────────────────────────┐
   │  finguard.silver.transaction         finguard.silver.fraud_    │
   │  finguard.silver.customers            watchlist_data            │
   └──────────────────────────────┬─────────────────────────────────┘
                                  │  streaming joins + aggregations
                                  ▼
   ┌──────────────────────────── GOLD ───────────────────────────────┐
   │  high_alert        (amount > customer transaction_limit)       │
   │  fraud_alert       (card on watchlist, 5-min watermark join)    │
   │  transaction_count_by_10min / _slidingwindow (windowed counts)  │
   │  alert_notification_log / fraudcard_alert_notification_log     │
   └──────────────────────────────┬─────────────────────────────────┘
                                  │  ForEachBatch email sinks
                                  ▼
                   Gmail SMTP → customer email alerts
                   + FinGuard Fraud Monitoring Dashboard
```

**Unity Catalog layout:** catalog `finguard` with schemas `bronze`, `silver`, `gold`. Customer silver table is published to `finguard.silver` by a separate pipeline.

---

## 2. Repository / Folder Structure

| Path | Purpose |
| --- | --- |
| `01_kafka_streaming_test` | Notebook that prototypes reading the Kafka topic with Structured Streaming, with SASL_SSL auth pulled from the secret scope. |
| `02_Setup_secret_scope` | Creates the Databricks secret scope `finguard-scope` via the Secrets REST API and stores Kafka connection details (JSON) and the Gmail app password. |
| `03_Send_Email` | Batch-mode HTML email sender: reads alerts from `finguard.gold.high_alert`, groups them by customer email, and sends consolidated alerts via Gmail SMTP (port 587, STARTTLS). |
| `Autoloader_test.py` | Auto Loader prototype: streams fraud-watchlist JSON files from a UC Volume (`cloudFiles` format), adds file metadata + ingestion timestamp, writes to a Delta table with `trigger(availableNow=True)`. |
| `fraud_watchlist_file_generator/` | Data generator: reads `fraud_watchlist.csv`, converts each row to a one-record JSON file, and drops them into the volume `/Volumes/finguard/bronze/fraud_watchlist/source_data/` — one file every 5 seconds to simulate a live feed. It is **resumable**: it scans existing files for the latest `watchlist_id` and skips already-processed rows. |
| `Fingaurd Pipeline/` | Main SDP pipeline source (bronze, silver, gold, alert subfolders) — see §3. |
| `Customer_Ingestion_Pipeline/` | Separate SDP pipeline that ingests the customer dimension (bronze → silver). |
| `New Pipeline 2026-10-09 21:48/` | Scratch/empty pipeline draft (transformations folder is blank) — kept for reference. |

---

## 3. The Fingaurd Pipeline (main SDP pipeline)

Pipeline ID: `3a3ad6be-89c1-4a3d-a3bf-0f6c2172ba73` · Serverless, Photon enabled · Libraries: `bronze/**`, `silver/Transaction_silver.py`, `silver/fraud_watchlist_silver.py`, `gold/**`, `alert/**`.

### Bronze layer (raw ingestion)

* **`bronze/Transaction_bronze.py`** — Streaming table `finguard.bronze.transaction`. Reads the Kafka topic (`spark.readStream` with `kafka.bootstrap.servers` and SASL_SSL/PLAIN auth, with the shaded `kafkashaded...PlainLoginModule` JAAS class). Kafka connection details (bootstrap servers, topic, API key/secret) are read from secret scope `finguard-scope`, key `kafka_connection_details`, and never hardcoded. The `value` column holds the JSON payload: `transaction_id, customer_id, card_number, merchant_id, merchant_name, merchant_category, amount, currency (INR), transaction_type, payment_channel, device_id, city, country, transaction_timestamp, is_international, status`. An `_ingested_at` column captures arrival time.
* **`bronze/fraud_watchlist_bronze.py`** — Streaming table `finguard.bronze.fraud_watchlist_data`. Uses **Auto Loader** (`cloudFiles`) to incrementally ingest the watchlist JSON files landing in the volume, with schema location for schema tracking and `_rescued_data` for malformed records.
* Bronze also hosts `finguard.bronze.customers` (customer dimension, 22 columns).

### Silver layer (clean, typed)

* **`silver/Transaction_silver.py`** — Streaming table `finguard.silver.transaction`. Parses the bronze JSON `value` into typed columns and normalizes the transaction record.
* **`silver/fraud_watchlist_silver.py`** — Streaming table `finguard.silver.fraud_watchlist_data`. Cleans and types the watchlist record: `watchlist_id, entity_id (card number), watch_type, action, reason_code, risk_level, status, effective_from, reported_by, reported_source, city, country`.
* Customer silver lives in the separate pipeline (below).

### Gold layer (business logic & alerting)

* **`gold/High Transaction Alert.py` → `finguard.gold.high_alert`** — **Stream–static join**: streams `silver.transaction` joined to `silver.customers`, flagging transactions where `amount > transaction_limit` for that customer. Enriches each alert with customer contact details.
* **`gold/FraudCard_alert.py` → `finguard.gold.fraud_alert`** — **Stream–stream join**: joins `silver.transaction` to `silver.fraud_watchlist_data` on `card_number = entity_id` within a **5-minute watermark window** on `transaction_timestamp` vs `effective_from`, then left-joins `silver.customers` for contact info. Produces a 37-column alert record (alert id/type/timestamp, full transaction fields, watchlist fields, customer fields).
* **`gold/Trasaction_Count_bymints.py` → `finguard.gold.transaction_count_by_10min`** — Windowed aggregation: 10-minute **tumbling window** counts of transactions per merchant category, with a 10-minute watermark for late data.
* **`gold/Trasaction_Count_sliding_window.py` → `finguard.gold.transaction_count_slidingwindow`** — Same aggregation but with a 10-minute window that **slides every 5 minutes**, giving overlapping window counts for smoother trend tracking.

### Alert layer (email sinks)

* **`alert/Alert Email Notification.py`** — A `@dp.foreach_batch_sink` + `@dp.append_flow` pair that reads **new** rows from `finguard.gold.high_alert` each micro-batch, groups alerts by customer email, sends one consolidated HTML email per customer (Gmail SMTP, app password from secret `gmail_api_key`), and writes a delivery record to `finguard.gold.alert_notification_log`. Writes are **idempotent** via `txnVersion`/`txnAppId`, so pipeline restarts never duplicate emails.
* **`alert/Fraudcard_email alert.py`** — Same tested pattern for watchlist matches: streams `finguard.gold.fraud_alert`, sends "Fraud Watchlist Match" emails (watch type, risk level, reason in the body; action/reported_by in the footer), logs to `finguard.gold.fraudcard_alert_notification_log` under a separate `txnAppId`. Customers with a null email (possible due to the left join) are logged as `SKIPPED_NO_EMAIL` rather than crashing the sink.

---

## 4. Customer_Ingestion_Pipeline (separate SDP pipeline)

Pipeline ID: `82412a9d-7a18-4fba-89af-fd56cf57361f` · Library: `Silver/**`.

`Silver/customer_silver_ingestion.py` reads `finguard.bronze.customers` (created by the main pipeline) as a stream and publishes **`finguard.silver.customers`** (~1,000 rows), converting `account_open_date` from varchar to a proper `DATE` type and adding a `silver_ingested_at` audit timestamp. This is the customer dimension used by both gold alert tables.

---

## 5. Monitoring Dashboard

**FinGuard Fraud Monitoring Dashboard** (AI/BI) — 13 widgets:

* Row 1: four KPI counters (high-value alerts, fraud watchlist alerts, total transactions, watchlist entries).
* Rows 2–4: six bar charts — alerts by merchant category, by risk level, customer-segment spend, watch type, payment channel, notification status.
* Row 5: two data tables of the 20 most recent high-value and watchlist alerts.
* Row 6: customer risk-score distribution.

All datasets are raw SQL against `finguard.gold.*` / `finguard.silver.*` tables.

---

## 6. Key Concepts & Technologies Used

| Concept | How it is used in FinGuard |
| --- | --- |
| **Medallion architecture** | Bronze = raw/immutable (Kafka payloads, raw files), Silver = cleaned/typed/conformed, Gold = business-level aggregates and alert records. |
| **Lakeflow Spark Declarative Pipelines (SDP)** | All layers are defined declaratively with the `pyspark.pipelines` API (`@dp.table`, `@dp.foreach_batch_sink`, `@dp.append_flow`); Databricks manages orchestration, checkpoints, and table materialization. |
| **Streaming tables** | Bronze/silver/gold tables are declared as streaming tables, so each pipeline update incrementally processes only new data. |
| **Apache Kafka + Structured Streaming** | Live transaction events consumed from Confluent Cloud with `spark.readStream.format("kafka")`, SASL_SSL/PLAIN auth, keyed off Kafka offset/partition/timestamp metadata. |
| **Auto Loader (`cloudFiles`)** | Incremental, exactly-once file ingestion for the fraud watchlist from a UC Volume, with schema inference, schema location, and `_rescued_data` for bad records. |
| **Watermarks** | `withWatermark` bounds late data in stateful operations — 10 minutes for windowed counts, a 5-minute window for the stream–stream watchlist join, keeping streaming state finite. |
| **Tumbling vs sliding windows** | 10-min tumbling windows give disjoint counts per merchant category; the sliding variant (10-min window, 5-min slide) gives overlapping, smoother trends. |
| **Stream–static vs stream–stream joins** | High-value alerts join a slowly changing customer table (stream–static, no state); fraud alerts join two unbounded streams (stream–stream, requires watermark + time bounds). |
| **ForEachBatch sinks + append flows** | The email notification "stage" is a foreach-batch micro-batch writer declared inside the pipeline; append flows let one logical flow feed a sink. |
| **Idempotent writes (`txnVersion`/`txnAppId`)** | Exactly-once semantics for the notification log — restarts and re-processing never duplicate emails or log rows. |
| **Databricks Secret Scopes + Secrets API** | All credentials (Kafka bootstrap/key/secret as one JSON secret, Gmail app password) live in scope `finguard-scope` and are fetched at runtime — never hardcoded in source. |
| **Gmail SMTP (STARTTLS)** | Customer notifications are sent as consolidated HTML emails over `smtp.gmail.com:587` using a Gmail app password; a single SMTP connection is reused for all recipients in a batch. |
| **Unity Catalog & Volumes** | Tables are governed in catalog `finguard` (bronze/silver/gold schemas); the volume `/Volumes/finguard/bronze/fraud_watchlist/` holds source files, schema location, and checkpoints. |
| **Serverless compute + Photon** | Both pipelines run serverless with the Photon engine — no cluster management, faster vectorized execution. |

---

## 7. Data Flow Summary (end-to-end)

1. **Ingest**: Kafka transaction events stream into `bronze.transaction`; watchlist JSON files are dropped into the volume (by the generator notebook or an external feed) and picked up by Auto Loader into `bronze.fraud_watchlist_data`; customer data lands in `bronze.customers`.
2. **Clean**: Silver streaming tables parse JSON to typed columns, fix data types (`account_open_date` → DATE), and add audit timestamps.
3. **Detect**: Gold tables detect high-value transactions (vs customer limits) and watchlist card matches (watermarked stream–stream join), plus windowed transaction-count metrics.
4. **Notify**: ForEachBatch email sinks send consolidated HTML alerts to each affected customer and log delivery status.
5. **Monitor**: The dashboard gives analysts live KPIs, distributions, and recent-alert tables.

---

## 8. Security Notes

* Kafka credentials, and the Gmail app password are stored **only** in secret scope `finguard-scope` (`kafka_connection_details`, `gmail_api_key`) and injected at runtime.
* The setup notebook retrieves the workspace API token from the notebook context — it is never printed to persistent output in production use.
* Masked/derived columns (e.g., partial card numbers in emails) should be considered if alerts are ever broadened beyond the account holder.
