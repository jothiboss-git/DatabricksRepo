import json

from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame


@dp.table(
    name="finguard.bronze.transaction",
    comment="Bronze streaming table ingesting transaction events from Confluent Kafka topic.",
)
def transaction() -> DataFrame:
    # Retrieve Kafka connection details from Databricks secret scope
    kafka_connection = dbutils.secrets.get(
        scope="finguard-scope",
        key="kafka_connection_details",
    )
    parsed = json.loads(kafka_connection)

    bootstrap_servers = parsed["bootstrap_servers"]
    api_key = parsed["api_key"]
    api_secret = parsed["api_secret"]
    topic = parsed["topic"]

    jaas_config = (
        f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="{api_key}" password="{api_secret}";'
    )

    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", jaas_config)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr(
            "CAST(key AS STRING) AS key",
            "CAST(value AS STRING) AS value",
            "topic",
            "partition",
            "offset",
            "timestamp AS kafka_timestamp",
            "timestampType",
            "current_timestamp() AS _ingested_at",
        )
    )