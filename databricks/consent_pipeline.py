# Databricks notebook source — also runnable locally with spark-submit.
#
#   Local dev (parquet, file source):
#     SOURCE=local INPUT_PATH=data/input TABLES=data/tables \
#       spark-submit databricks/consent_pipeline.py
#
#   Azure (Delta, Event Hubs Kafka endpoint — no connector library needed):
#     SOURCE=kafka FORMAT=delta EH_NAMESPACE=consentxxxxx-ehns \
#       EVENTHUB_CONN_STR='Endpoint=sb://...' \
#       spark-submit databricks/consent_pipeline.py
#   Secret handling: env var here is for local dev only. On Databricks read
#   the connection string from a secret scope (or use a managed identity via
#   the Access Connector in terraform/) instead of a literal.
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

SOURCE = os.getenv("SOURCE", "local")  # local | kafka
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
    if SOURCE == "kafka":
        # Event Hubs exposes a Kafka endpoint, so Spark's built-in kafka
        # source works with no extra connector. Partition key = user_id is
        # set on the producer side (see generator/produce_events.py).
        ns = os.environ["EH_NAMESPACE"]  # e.g. consentxxxxx-ehns
        pwd = os.environ["EVENTHUB_CONN_STR"]
        return (
            spark.readStream.format("kafka")
            .option(
                "kafka.bootstrap.servers",
                f"{ns}.servicebus.windows.net:9093",
            )
            .option("subscribe", "consent-events")
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism", "PLAIN")
            .option(
                "kafka.sasl.jaas.config",
                "kafkashaded.org.apache.kafka.common.security.plain."
                'PlainLoginModule required username="$ConnectionString" '
                f'password="{pwd}";',
            )
            .option("startingOffsets", "earliest")
            .load()
            .select(F.col("value").cast("string").alias("json"))
            .select(F.from_json("json", SCHEMA_DDL).alias("e"))
            .select("e.*")
        )
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
        F.when(
            F.col("user_id").isNull() | (F.col("user_id") == ""),
            F.lit("missing_user_id"),
        ),
        F.when(
            F.col("event_id").isNull() | (F.col("event_id") == ""),
            F.lit("missing_event_id"),
        ),
        F.when(
            F.col("consent").isNull() | ~F.col("consent").isin("granted", "denied"),
            F.lit("bad_consent"),
        ),
        F.when(
            F.col("platform").isNull()
            | ~F.col("platform").isin("ios", "android", "web"),
            F.lit("bad_platform"),
        ),
        F.when(
            F.col("event_ts").isNull() | F.isnan(F.col("event_ts")),
            F.lit("bad_ts"),
        ),
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
        # availableNow (trigger(once) is deprecated): process everything
        # currently available, then stop — no arbitrary timeouts.
        w = w.trigger(availableNow=True)
    return w.start(f"{TABLES}/{name}")


def downstream():
    """Silver + quarantine read from bronze (never from the raw source)."""
    b = spark.readStream.format(FORMAT).load(f"{TABLES}/bronze")
    ok = (
        b.filter(is_clean)
        .withWatermark("event_time", "1 hour")
        # Bounded state: keys are evicted once the watermark passes
        # event_time + delay (plain dropDuplicates would grow forever).
        # Rows arriving later than the watermark are dropped by the engine —
        # our stale_event rule quarantines anything older than 7 days first.
        .dropDuplicatesWithinWatermark(["event_id"])
    )
    bad = b.filter(~is_clean)
    return ok, bad


def build_gold():
    """Gold: current consent per user (latest event_time wins).

    This is the table a consent product actually queries. On Delta the
    incremental path is a MERGE (Databricks-only); the parquet/local path
    rewrites the snapshot, which is equivalent for a demo-sized table.
    """
    from pyspark.sql import Window

    gold_path = f"{TABLES}/gold"
    latest = (
        spark.read.format(FORMAT)
        .load(f"{TABLES}/silver")
        .withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("user_id").orderBy(F.desc("event_time"))
            ),
        )
        .filter("rn = 1")
        .drop("rn")
    )
    if FORMAT == "delta":
        latest.createOrReplaceTempView("gold_updates")
        try:
            spark.sql(
                f"""MERGE INTO delta.`{gold_path}` t USING gold_updates s
                    ON t.user_id = s.user_id
                    WHEN MATCHED AND s.event_time > t.event_time
                      THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *"""
            )
            return
        except Exception:
            pass  # gold doesn't exist yet: seed it below
    latest.write.format(FORMAT).mode("overwrite").save(gold_path)


if TRIGGER_ONCE:
    # Bounded demo run: bronze first, then downstream over finished bronze.
    bq = start(bronze, "bronze")
    bq.awaitTermination()
    silver_df, quar_df = downstream()
    q1, q2 = start(silver_df, "silver"), start(quar_df, "quarantine")
    # awaitAnyTermination returns when the FIRST query ends — wait for both.
    q1.awaitTermination(600)
    q2.awaitTermination(600)
    build_gold()
else:
    bq = start(bronze, "bronze")
    silver_df, quar_df = downstream()
    q1, q2 = start(silver_df, "silver"), start(quar_df, "quarantine")
    spark.streams.awaitAnyTermination()
