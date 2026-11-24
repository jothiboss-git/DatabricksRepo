from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, count, window


@dp.table(
    name="finguard.gold.transaction_count_slidingwindow",
    comment="Gold streaming table with transaction count aggregated per 10-minute tumbling window and  every 5 min aggregation ",
)
def transaction_count_by_10min() -> DataFrame:
    """
    Reads transaction events from the silver layer, applies a 10-minute watermark
    on transaction_timestamp, and aggregates the count of transactions per
    10-minute tumbling window.
    """
    return (
        spark.readStream.table("finguard.silver.transaction")
        .withWatermark("transaction_timestamp", "10 minutes")
        .groupBy(
            window(col("transaction_timestamp"), "10 minutes","5 minutes"),
            col("merchant_category"),
        )
        .agg(count("*").alias("transaction_count"))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("merchant_category"),
            col("transaction_count"),
        )
    )
