from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, to_date


@dp.table(
    name="finguard.silver.customers",
    comment="Silver layer customer data with account_open_date converted to DATE and silver ingestion timestamp",
)
@dp.expect_or_drop("customer_id is not null", "customer_id IS NOT NULL")
@dp.expect_or_drop("account_open_date is not null", "account_open_date IS NOT NULL")
@dp.expect("account_open_date is a valid date", "account_open_date IS NOT NULL")
@dp.expect("customer_id is positive", "customer_id > 0")
@dp.expect("annual_income is not negative", "annual_income >= 0")

def customers():
    return (
        spark.readStream.table("finguard.bronze.customers")
        .withColumn("account_open_date", to_date(col("account_open_date")))
        .withColumn("silver_ingested_at", current_timestamp())
    )
