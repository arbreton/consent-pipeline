# Design FAQ

## Why each piece

- **Event Hubs, 8 partitions.** Partition key = `user_id`, so one user's
  events stay ordered. 8 is plenty for a demo; more partitions would only
  burn more throughput units.
- **Built-in Kafka source against Event Hubs.** No connector library, no
  extra dependency to version-pin. The trade-off: SASL config by hand and
  the connection string handled carefully (secret scope on Databricks).
- **Bronze written once, everything downstream reads bronze.** The raw
  source is consumed a single time; silver, quarantine and gold are pure
  functions of bronze. Replays and rebuilds never touch Event Hubs twice.
- **Watermarked dedupe, bounded state.**
  `withWatermark("event_time", "1 hour").dropDuplicatesWithinWatermark(["event_id"])`
  keeps the state store proportional to the horizon instead of growing with
  total history. Rows arriving later than the watermark are dropped by the
  engine — a Spark semantic, not a choice — which is why anything older
  than 7 days is quarantined as `stale_event` before it ever reaches state.
- **Medallion + quarantine with reasons.** Bronze = evidence, silver =
  product, quarantine = debugging inbox with a `violations` array on every
  row. Mirrors the immutable-raw / curated-layer thinking of a Hive→Iceberg
  migration.
- **Gold via MERGE (Delta) / snapshot (parquet).** The serving layer is one
  row per user with the latest consent — the table a consent product
  actually queries. MERGE is incremental on Delta; the parquet rewrite is
  equivalent at demo scale.
- **Quality rules.** Each maps to a real incident class: bad SDK release
  (bad enum/platform), clock skew (future), stale replays (stale), producer
  retries (duplicates via `event_id`).

## Trade-offs worth knowing

**"Why not write straight to the dashboard DB?"**
A slow dashboard query would backpressure ingestion and lose events. The
lake decouples them; Power BI reads gold on its own schedule.

**"What happens when the job dies mid-batch?"**
Checkpoints plus a replayable source: restart from the last committed
offset. Silver stays idempotent through `event_id` dedupe, so replays are
safe. Retention is 3 days — replay depth beyond that is gone.

**"Duplicates?"**
Producer retries reuse `event_id`; the watermarked dedupe collapses them
within the horizon. Outside the horizon the engine drops late arrivals;
outside 7 days our own rule quarantines them first.

**"Schema change — a new platform?"**
The silver filter is an allowlist: unknown platforms quarantine with
`bad_platform` — visible, counted, alertable — instead of silently passing
or crashing the stream. Promote to the allowlist deliberately.

**"Cost of running this 24/7?"**
Event Hubs Standard bills throughput units hourly whether idle or not;
Databricks bills DBUs per uptime. Demo economics: run, screenshot,
`terraform destroy`.
