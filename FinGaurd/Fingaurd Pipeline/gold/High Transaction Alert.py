from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, current_timestamp, concat, lit


@dp.table(
    name="finguard.gold.high_alert",
    comment="Gold layer streaming table containing high-value transaction alerts where amount exceeds customer transaction_limit.",
)
def high_transaction_alert() -> DataFrame:
    # Read streaming transactions from silver layer
    transactions = spark.readStream.table("finguard.silver.transaction")

    # Read customer dimension as a static DataFrame from silver layer
    customers = spark.read.table("finguard.silver.customers")

    # Join transactions with customers on customer_id
    # and filter where transaction amount exceeds the customer's transaction_limit
    joined = (
        transactions.alias("t")
        .join(customers.alias("c"), on="customer_id", how="left")
        .filter(col("t.amount") > col("c.transaction_limit"))
    )

    # Select important fields from both tables and add alert-specific columns
    return joined.select(
        # --- Alert columns ---
        concat(lit("High Transaction ID - "), col("t.transaction_id")).alias("alert_id"),
        lit("High value transaction").alias("alert_type"),
        current_timestamp().alias("alert_timestamp"),
        # --- Transaction fields ---
        col("t.transaction_id"),
        col("customer_id"),
        col("t.card_number").alias("transaction_card_number"),
        col("t.merchant_id"),
        col("t.merchant_name"),
        col("t.merchant_category"),
        col("t.amount"),
        col("t.currency"),
        col("t.transaction_type"),
        col("t.payment_channel"),
    
        col("t.city").alias("transaction_city"),
        col("t.country").alias("transaction_country"),
        col("t.transaction_timestamp"),
        col("t.is_international"),
        col("t.status"),
        col("t.silver_ingested_at").alias("transaction_ingested_at"),
        # --- Customer fields ---
        concat(col("c.first_name"),lit("-") ,col("c.last_name")).alias("customer_name"),
        col("c.email"),
        col("c.card_number").alias("customer_card_number"),
        col("c.card_type"),
        col("c.transaction_limit"),
        col("c.risk_score"),
        col("c.customer_segment"),
        col("c.annual_income"),
        col("c.account_open_date"),
    )
