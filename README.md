# Consent-events pipeline — Event Hubs → Databricks → Delta Lake

Personal project mirroring a production consent pipeline (Pinterest-style) on
Azure stack: streaming ingestion, medallion storage, data-quality quarantine,
dashboard, IaC + CI.

```
            ┌─────────────┐     ┌──────────────────────┐     ┌──────────┐
 events ──▶ │ Event Hubs  │ ──▶ │ Databricks Structured │ ──▶ │  Delta   │
 (8 parts)  │ (buffer,    │     │ Streaming (PySpark):  │     │  Lake:   │
            │  replayable)│     │ validate + dedupe     │     │ bronze / │
            └─────────────┘     └──────────────────────┘     │ silver / │
                                        │                   │ quaran-  │
                                        │ rejects           │ tine     │
                                        ▼                   └──────────┘
                                   quarantine                     │
                                                              Power BI
```

## Repo map

- `generator/produce_events.py` — synthetic consent events (with a tunable
  bad-row rate) for local runs and demos.
- `databricks/consent_pipeline.py` — the streaming job. File source + parquet
  locally, Event Hubs source + Delta on Azure (env switch, same code).
- `src/quality.py` — the validation rules in pure Python (unit-tested; the
  notebook mirrors them in Spark SQL).
- `terraform/` — Event Hubs namespace + hub, Data Lake Gen2 account with
  bronze/silver/quarantine containers.
- `tests/` — pytest suite for the rules.
- `docs/architecture.md` — decisions + interview Q&A.

## Quickstart (local, no Azure needed)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pytest -q                                            # rules pass
python generator/produce_events.py --count 200       # data/input/*.json
SOURCE=local TRIGGER_ONCE=1 spark-submit databricks/consent_pipeline.py
ls data/tables/silver data/tables/quarantine         # clean vs rejected rows
```

(Azure run: `terraform apply`, then the same job with
`SOURCE=eventhubs FORMAT=delta`. Tear the resource group down after demos —
an idle Standard namespace still bills.)

## Design decisions (short)

1. **Buffer before compute.** Event Hubs in front means backpressure and
   replays are free; the job can restart without data loss.
2. **Bronze is append-only.** Raw rows land untouched; silver is always
   rebuildable. Recovery story > cleverness.
3. **Bad rows quarantine, never silently drop.** Every reject keeps its
   violation list — debuggable, auditable.
4. **Same code, two sinks.** `FORMAT`/`SOURCE` env switches keep local dev
   (parquet/files) and prod (Delta/Event Hubs) on one code path.
