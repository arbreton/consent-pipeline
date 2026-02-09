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
# Medallion: raw rows -> bronze (raw + ingest_ts + violations, append-only) ->
# silver (clean, deduped) + quarantine (rejects keep their violation list).
# Bronze is written ONCE; silver/quarantine stream from bronze, so the source
# (Event Hubs) is consumed a single time. Silver can always be rebuilt from
# bronze.

import os

from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("consent-pipeline").getOrCreate()
spark.sql("set spark.sql.shuffle.partitions=8")

SOURCE = os.getenv("SOURCE", "local")  # local | eventhubs
INPUT_PATH = os.getenv("INPUT_PATH", "data/input")
CHECKPOINT = os.getenv("CHECKPOINT", "data/checkpoints")
TABLES = os.getenv("TABLES", "data/tables")
FORMAT = os.getenv("FORMAT", "parquet")  # delta on Databricks
TRIGGER_ONCE = os.getenv("TRIGGER_ONCE", "")  # set to "1" for bounded demo runs

# Single source of truth for the event schema (was written out twice).
SCHEMA_DDL = (
    "user_id STRING, event_id STRING, consent STRING, "
    "platform STRING, event_ts DOUBLE"
)


def read_source():
    if SOURCE == "eventhubs":
        conn = os.environ["EVENTHUB_CONN_STR"]
        eh_conf = {
            "eventhubs.connectionString": conn,
            "eventhubs.startingPosition": '{"offset": "-1"}',
        }
        raw = spark.readStream.format("eventhubs").options(**eh_conf).load()
        parsed = raw.select(F.col("body").cast("string").alias("json")).select(
            F.from_json("json", SCHEMA_DDL).alias("e")
        )
        return parsed.select("e.*")
    return (
        spark.readStream.format("json")
        .schema(SCHEMA_DDL)
        .option("maxFilesPerTrigger", 1)
        .load(INPUT_PATH)
    )


parsed = read_source().withColumn(
    "event_time", F.to_timestamp(F.from_unixtime(F.col("event_ts")))
)

now_unix = F.unix_timestamp(F.current_timestamp())

# Every reject keeps its violation list (quarantine is debuggable, not a pit).
violations = F.array_compact(
    F.array(
        F.when(F.col("user_id").isNull(), F.lit("missing_user_id")),
        F.when(F.col("event_id").isNull(), F.lit("missing_event_id")),
        F.when(
            F.col("consent").isNull() | ~F.col("consent").isin("granted", "denied"),
            F.lit("bad_consent"),
        ),
        F.when(
            F.col("platform").isNull()
            | ~F.col("platform").isin("ios", "android", "web"),
            F.lit("bad_platform"),
        ),
        F.when(F.col("event_ts").isNull(), F.lit("bad_ts")),
        F.when(F.col("event_ts") > now_unix + 300, F.lit("future_ts")),
        F.when(F.col("event_ts") < now_unix - 7 * 86400, F.lit("stale_event")),
    )
)

bronze = parsed.withColumn("violations", violations).withColumn(
    "ingest_ts", F.current_timestamp()
)

# NULL-safe split: a NULL comparison is neither true nor false, so without
# coalesce a null row would land in NEITHER stream (silently dropped).
is_clean = F.coalesce(F.size("violations") == 0, F.lit(False))


def start(df, name):
    w = (
        df.writeStream.format(FORMAT)
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT}/{name}")
    )
    if TRIGGER_ONCE:
        w = w.trigger(once=True)
    return w.start(f"{TABLES}/{name}")


def downstream():
    """Silver + quarantine read from bronze (never from the raw source)."""
    b = spark.readStream.format(FORMAT).load(f"{TABLES}/bronze")
    ok = (
        b.filter(is_clean)
        .withWatermark("event_time", "1 hour")
        .dropDuplicates(["event_id"])
    )
    bad = b.filter(~is_clean)
    return ok, bad


if TRIGGER_ONCE:
    # Bounded demo run: bronze first, then downstream over finished bronze.
    bq = start(bronze, "bronze")
    bq.awaitTermination()
    silver_df, quar_df = downstream()
    q1, q2 = start(silver_df, "silver"), start(quar_df, "quarantine")
    q1.awaitTermination(600)
    q2.awaitTermination(600)
else:
    bq = start(bronze, "bronze")
    silver_df, quar_df = downstream()
    q1, q2 = start(silver_df, "silver"), start(quar_df, "quarantine")
    spark.streams.awaitAnyTermination()
