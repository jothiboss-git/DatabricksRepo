from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dp.table(
    name="finguard.gold.fraud_alert",
    comment="Gold layer streaming table detecting fraud alerts by joining transactions with fraud watchlist data on card number within a watermark window, enriched with customer details.",
)
def fraud_alert() -> DataFrame:
    # Read streaming transactions from silver layer with watermark
    transactions = (
        spark.readStream.table("finguard.silver.transaction")
        .withWatermark("transaction_timestamp", "5 minutes")
    )

    # Read streaming fraud watchlist from silver layer with watermark
    fraud_watchlist = (
        spark.readStream.table("finguard.silver.fraud_watchlist_data")
        .withWatermark("effective_from", "5 minutes")
    )

    # Stream-to-stream join: transactions with fraud watchlist on card number within time window
    # entity_id in fraud watchlist corresponds to card_number in transactions
    tx_watchlist = (
        transactions.alias("t")
        .join(
            fraud_watchlist.alias("f"),
            (F.col("t.card_number") == F.col("f.entity_id")),
            "inner",
        )
    )

    # Read customer dimension as a static DataFrame from silver layer
    customers = spark.read.table("finguard.silver.customers")

    # Join the result with customer data on customer_id
    joined = tx_watchlist.join(
        customers.alias("c"),
        F.col("t.customer_id") == F.col("c.customer_id"),
        "left",
    )

    # Select and filter relevant columns
    return joined.select(
        # Alert columns
        F.concat(F.lit("FRAD-"), F.col("t.transaction_id"),F.col("watchlist_id")).alias("alert_id"),
        F.lit("Fraud Watchlist Match").alias("alert_type"),
        F.current_timestamp().alias("alert_timestamp"),
        # Transaction columns
        F.col("t.transaction_id"),
        F.col("t.customer_id"),
        F.col("t.card_number").alias("transaction_card_number"),
        F.col("t.merchant_id"),
        F.col("t.merchant_name"),
        F.col("t.merchant_category"),
        F.col("t.amount"),
        F.col("t.currency"),
        F.col("t.transaction_type"),
        F.col("t.payment_channel"),
        F.col("t.city").alias("transaction_city"),
        F.col("t.country").alias("transaction_country"),
        F.col("t.transaction_timestamp"),
        F.col("t.is_international"),
        F.col("t.status").alias("transaction_status"),
        # Fraud watchlist columns
        F.col("f.watchlist_id"),
        F.col("f.watch_type"),
        F.col("f.entity_id").alias("watchlist_entity_id"),
        F.col("f.risk_level"),
        F.col("f.action"),
        F.col("f.reason_code"),
        F.col("f.reason_description"),
        F.col("f.reported_by"),
        F.col("f.reported_source"),
        F.col("f.status").alias("watchlist_status"),
        F.col("f.effective_from"),
        F.col("f.city").alias("watchlist_city"),
        F.col("f.country").alias("watchlist_country"),
        # Customer columns
        F.concat(F.col("c.first_name"), F.lit(" "), F.col("c.last_name")).alias("customer_name"),
        F.col("c.email"),
        F.col("c.card_type"),
        F.col("c.customer_segment"),
        F.col("c.risk_score").alias("customer_risk_score"),
        F.col("c.annual_income"),
        F.col("c.transaction_limit"),
    )
