# Consent-events pipeline — Event Hubs → Databricks → Delta Lake

![ci](https://github.com/arbreton/consent-pipeline/actions/workflows/ci.yml/badge.svg)

Streaming consent-event pipeline on Azure stack: Kafka-protocol ingestion,
medallion storage with a data-quality quarantine, a gold "current consent"
table, IaC + CI.

```
            ┌─────────────┐     ┌──────────────────────┐     ┌──────────┐
 events ──▶ │ Event Hubs  │ ──▶ │ Databricks Structured │ ──▶ │  Delta   │
 (8 parts,  │ (buffer,    │     │ Streaming (PySpark):  │     │  Lake:   │
 key=user)  │  replayable)│     │ validate + dedupe     │     │ bronze / │
            └─────────────┘     └──────────────────────┘     │ silver / │
                                        │                    │ quaran-  │
                                        │ rejects            │ tine /   │
                                        ▼                    │ gold     │
                                   quarantine                └──────────┘
                                                                   │
                                              Power BI ◀── gold (per-user current consent)
```

## Prerequisites

| Need | Version | Notes |
| --- | --- | --- |
| Python | 3.12 | local dev + CI |
| Java | 11 or 17 | local Spark runs only (CI installs Temurin 17) |
| pyspark | 3.5.1 | pinned in `requirements.txt` / `pyproject.toml` |
| Delta | Databricks Runtime | Delta paths run on Databricks, not locally |
| Terraform | ≥ 1.5 | Azure deploy |
| Azure CLI + subscription | — | `terraform apply` |

## Environment variables

| Var | Default | Meaning |
| --- | --- | --- |
| `SOURCE` | `local` | `local` (JSON files) or `kafka` (Event Hubs Kafka endpoint) |
| `FORMAT` | `parquet` | `parquet` locally, `delta` on Databricks |
| `TRIGGER_ONCE` | unset | set to `1` for a bounded `availableNow` demo run |
| `INPUT_PATH` | `data/input` | file-source directory (`SOURCE=local`) |
| `TABLES` / `CHECKPOINT` | `data/tables` / `data/checkpoints` | output + checkpoint roots |
| `EH_NAMESPACE` | — | Event Hubs namespace, no suffix (`SOURCE=kafka`) |
| `EVENTHUB_CONN_STR` | — | connection string; local-dev only — on Databricks use a secret scope |

## Data contract

Field | Type | Rule | Violation code
--- | --- | --- | ---
`user_id` | string | non-null, non-empty | `missing_user_id`
`event_id` | string | non-null, non-empty (dedupe key) | `missing_event_id`
`consent` | string | one of `granted`, `denied` | `bad_consent`
`platform` | string | one of `ios`, `android`, `web` | `bad_platform`
`event_ts` | epoch seconds | numeric (no bool/NaN/null) | `bad_ts`
`event_ts` | epoch seconds | ≤ now + 300s | `future_ts`
`event_ts` | epoch seconds | ≥ now − 7 days | `stale_event`

Same codes in `src/quality.py` (pure Python, unit-tested) and in Spark SQL in
the notebook — by design, so local reasoning matches cluster behavior.

## Sample output

Seeded demo run (`--count 200 --error-rate 0.05 --seed 7`), validated with
the Python mirror of the rules:

```
in: 200 | silver: 187 | quarantine: 13
quarantine breakdown: bad_platform ×5, bad_ts ×3, bad_consent ×3,
                      future_ts ×1, missing_user_id ×1
```

## Quickstart (local, no Azure needed)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pytest -q                                            # unit + flow tests
python generator/produce_events.py --count 200       # data/input/*.json
SOURCE=local TRIGGER_ONCE=1 spark-submit databricks/consent_pipeline.py
ls data/tables/{bronze,silver,quarantine,gold}       # every row accounted for
```

## Running on Azure

```bash
cd terraform && terraform init && terraform apply   # unique names via random suffix
python generator/produce_events.py --sink eventhubs \
  --conn-str "$EVENTHUB_CONN_STR" --count 200
# Databricks: upload databricks/consent_pipeline.py as a notebook (or bundle
# it as a job), set SOURCE=kafka FORMAT=delta, connection string from a
# secret scope. Consumer group databricks-cg and lake access are in Terraform.
```

Cost (rough, eastus, 2026): Event Hubs Standard ≈ $0.045/hr per TU whether
idle or not; Databricks DBUs per uptime; storage is cents. For a demo: run,
screenshot, then `terraform destroy`.

## Repo map

- `generator/produce_events.py` — synthetic events (tunable bad-row rate) to
  files or Event Hubs (`--sink eventhubs`, partition key `user_id`).
- `databricks/consent_pipeline.py` — the streaming job. Bronze written once;
  silver/quarantine/gold derive from it. Same code, both sinks.
- `src/quality.py` — validation rules in pure Python, unit-tested incl.
  boundary cases (±300s, 7-day edge, bool/NaN/null).
- `terraform/` — Event Hubs + consumer group, Data Lake Gen2 (4 containers),
  Databricks workspace + lake Access Connector.
- `tests/` — rule tests, no-Spark flow test, full Spark e2e (CI, needs JVM).
- `docs/design-faq.md` — decisions and trade-offs.

## Limitations / roadmap

- The live Event Hubs send path and the Delta MERGE gold path are
  implemented but exercised only in CI-shaped runs here — no live Azure
  subscription on this machine. Treat the local parquet run as the verified
  path until someone runs it against real infra.
- `tflint` + `checkov` are not in CI yet (no runner here to validate the
  config against); the Terraform follows their usual asks (TLS 1.2, tags,
  managed identity instead of keys).

## License

MIT — see `LICENSE`.
