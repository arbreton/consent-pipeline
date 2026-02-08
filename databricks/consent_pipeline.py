# Databricks notebook source — also runnable locally with spark-submit.
#
#   Local dev (parquet, file source):
#     SOURCE=local INPUT_PATH=data/input TABLES=data/tables \
#       spark-submit databricks/consent_pipeline.py
#
#   Azure (Delta, Event Hubs source; connection string via env/secret scope):
#     SOURCE=eventhubs FORMAT=delta EVENTHUB_CONN_STR='Endpoint=sb://...' \
#       spark-submit databricks/consent_pipeline.py
#
# Medallion: raw file/event rows -> bronze (raw + ingest_ts) -> silver
# (validated, deduped) + quarantine (violations). Bronze is append-only and
# never mutated: silver can always be rebuilt from it.

import os

from pyspark.sql import functions as F

SOURCE = os.getenv("SOURCE", "local")
INPUT_PATH = os.getenv("INPUT_PATH", "data/input")
CHECKPOINT = os.getenv("CHECKPOINT", "data/checkpoints")
TABLES = os.getenv("TABLES", "data/tables")
FORMAT = os.getenv("FORMAT", "parquet")  # delta on Databricks
TRIGGER_ONCE = os.getenv("TRIGGER_ONCE", "")  # set to "1" for batch-style demo runs

spark.sql("set spark.sql.shuffle.partitions=8")

if SOURCE == "eventhubs":
    import json as _json  # noqa: E402  (kept local to this branch)

    conn = os.environ["EVENTHUB_CONN_STR"]
    eh_conf = {
        "eventhubs.connectionString": conn,
        "eventhubs.startingPosition": _json.dumps({"offset": "-1"}),
    }
    raw = spark.readStream.format("eventhubs").options(**eh_conf).load()
    parsed = raw.select(F.col("body").cast("string").alias("json")).select(
        F.from_json(
            "json",
            "user_id STRING, consent STRING, platform STRING, "
            "event_ts DOUBLE, event_id STRING",
        ).alias("e")
    ).select("e.*")
else:
    parsed = (
        spark.readStream.format("json")
        .schema(
            "user_id STRING, consent STRING, platform STRING, "
            "event_ts DOUBLE, event_id STRING"
        )
        .option("maxFilesPerTrigger", 1)
        .load(INPUT_PATH)
    )

bronze = parsed.withColumn("ingest_ts", F.current_timestamp())

now_unix = F.unix_timestamp(F.current_timestamp())
valid = (
    F.col("user_id").isNotNull()
    & F.col("consent").isin("granted", "denied")
    & F.col("platform").isin("ios", "android", "web")
    & (F.col("event_ts") <= now_unix + 300)
)

silver = (
    bronze.filter(valid)
    .withWatermark("ingest_ts", "1 hour")
    .dropDuplicates(["event_id"])
)
quarantine = bronze.filter(~valid)


def start(df, name):
    w = df.writeStream.format(FORMAT).outputMode("append")
    w = w.option("checkpointLocation", f"{CHECKPOINT}/{name}")
    if TRIGGER_ONCE:
        w = w.trigger(once=True)
    return w.start(f"{TABLES}/{name}")


q1 = start(silver, "silver")
q2 = start(quarantine, "quarantine")

if TRIGGER_ONCE:
    spark.streams.awaitAnyTermination(300)
else:
    spark.streams.awaitAnyTermination()
