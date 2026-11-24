import json

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
@dp.table(
    name="finguard.silver.fraud_watchlist_data",
    comment="Cleaned Fraud Watchlist .",
)
def fraud_watchlist_silver() -> DataFrame:
    
    bronze_df=(spark.readStream.table("finguard.bronze.fraud_watchlist_data" ))

    cleaned_df=bronze_df.select(
    F.upper(F.col("watchlist_id")).alias("watchlist_id"),
    F.col("watch_type"),
    F.upper(F.col("entity_id")).alias("entity_id"),
    F.upper(F.col("risk_level")).alias("risk_level"),
    F.upper(F.col("action")).alias("action"),
    F.col("reason_code"),
    F.col("reason_description"),
    F.col("reported_by"),
    F.col("reported_source"),
    F.col("status"),
    F.col("city"),
    F.col("country"),
    F.to_timestamp(F.col("effective_from"), "yyyy-MM-dd HH:mm:ss" ).alias("effective_from"),
    F.col("_rescued_data"),
    F.col("file_path"),
    F.col("ingestion_timestamp").alias("bronze_ingestion_timestamp"),
    F.current_timestamp().alias("ingestion_timestamp")
    )
    return cleaned_df
