# Architecture notes + interview Q&A

## Why each piece

- **Event Hubs, 8 partitions.** Partition key = `user_id`, so one user's
  events stay ordered. 8 is plenty for a demo and shows you think about
  parallelism vs. cost (more partitions = more throughput units burned).
- **Structured Streaming + watermark + dropDuplicates.** Exactly-once
  *effect* (idempotent silver) without transactions: watermark bounds state,
  `event_id` dedupes producer retries.
- **Medallion (bronze/silver/quarantine).** Bronze = evidence, silver =
  product, quarantine = debugging inbox. Mirrors Hive→Iceberg migration
  thinking: immutable raw layer, curated layer on top.
- **Quality rules.** Non-null user, closed consent enum, known platform,
  sane timestamp (no future > 5 min skew, no ancient rows). Each rule maps
  to a real production incident class (bad SDK release, clock skew, …).

## Likely questions + answers

**"Why not write straight to the dashboard DB?"**
Coupling. A slow dashboard query would backpressure ingestion and lose
events. The lake decouples them; Power BI reads silver on its own schedule.

**"What happens when the job dies mid-batch?"**
Checkpoints + replayable source: restart from the last committed offset.
Silver stays idempotent via `event_id` dedupe, so replays are safe.

**"Duplicates?"**
Producer retries reuse `event_id`; watermarked `dropDuplicates` collapses
them within the lateness horizon. Late dupes outside the horizon land in
quarantine for inspection instead of corrupting silver.

**"How do you handle schema change (new platform)?"**
Silver filter is an allowlist: unknown platforms quarantine with
`bad_platform` — visible, counted, alertable — instead of silently passing
or crashing the stream. Promote to allowlist deliberately.

**"Cost if this ran 24/7?"**
Event Hubs Standard bills throughput units hourly whether idle or not;
Databricks bills DBUs per uptime. For a demo: run, screenshot, `terraform
destroy`. Say this out loud — interviewers love cost awareness.
