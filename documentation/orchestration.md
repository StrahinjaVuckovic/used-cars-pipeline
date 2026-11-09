# Orchestration

How the pipeline runs every day: which tool starts what, in which order, and how a failure stops the rest.

## Split between ADF and Databricks

The mentor's guideline: notebooks live in Databricks; scheduling is done by Azure Data Factory (ADF); the vehicle
sources are processed as a Databricks pipeline, while the service book, the brand lookup and the exchange-rate API
go through ADF.

| Responsibility | Tool |
|---|---|
| Daily schedule and the order of all steps | ADF (`pl_daily_master` + trigger `tr_daily_master`) |
| Moving files landing → Bronze | ADF (`pl_ingest_bronze_sources`, Copy activities) |
| Exchange rates API → Bronze *(planned)* | ADF (Copy activity with a REST source) |
| Cleaning and combining (Silver), Gold | Databricks notebooks, grouped into Databricks Jobs |

ADF never processes data itself: it starts Databricks Jobs with the **Databricks Job activity**, waits for them
and fails when a Job fails. A Job keeps the task graph that belongs to Databricks (for example the four vehicle
sources in parallel), so the same Job can also be started by hand in Databricks.

## Daily master pipeline

```
tr_daily_master  (schedule trigger, every day 02:00 Europe/Belgrade – created, currently STOPPED)
└─ pl_daily_master                                            parameters: ingestion_date=latest, dev_mode=false
   1. ingest_bronze            → pl_ingest_bronze_sources     6 × Copy: landing → bronze/<Source>/ingestion_date=…
   2. silver_vehicle_listings  → pl_silver_vehicle_listings   Databricks Job silver_vehicle_listings
   3. silver_service_events    → pl_silver_service_events     Databricks Job silver_service_events
   (planned) exchange rates: pl_ingest_exchange_rates (API → Bronze) and a Job for silver.exchange_rates
             + the priced view, after steps 1–2
   (planned) gold: Job gold, after all Silver steps
```

- Every step starts only when the previous one **succeeded** (`dependencyConditions: Succeeded`), and every
  Execute Pipeline activity has **Wait on completion** on, so a step never reads data that is still being written.
- If a step fails, the following steps do not run and the master pipeline is marked failed in ADF Monitor.
  The Silver tables keep their previous (complete) version: every write is a Delta overwrite, which is atomic.
- `silver_service_events` runs **after** `silver_vehicle_listings`, so its coverage check (service-book vehicles
  present in the listings) compares with the listings of the same day.
- The parameters `ingestion_date` and `dev_mode` are passed down master → child pipeline → Job → notebook widget.
  `dev_mode=true` runs the whole chain on samples and writes to the `*_dev` tables (cheap test of the wiring);
  `ingestion_date=YYYY-MM-DD` reprocesses one Bronze partition (backfill). Both are **String** parameters: the
  notebooks compare the widget with the text `"true"`, and an ADF Bool would arrive as `"True"`.

## Databricks Jobs

| Job | Tasks | Definition |
|---|---|---|
| `silver_vehicle_listings` | `validate_landing_and_bronze` (sources=vehicle_listings) → 4 × `stage_<source>_source` in parallel → `vehicle_listings` | `orchestration/databricks_job_silver_vehicle_listings.yml` |
| `silver_service_events` | `validate_landing_and_bronze` (sources=service_book) → `service_events` | `orchestration/databricks_job_silver_service_events.yml` |

- Job parameters (`ingestion_date`, `dev_mode`, `sources`) reach every notebook task as widgets; a notebook
  without such a widget ignores it. `sources` tells `01_validate_landing_and_bronze` which landing/Bronze folders
  belong to this Job. The Databricks UI does not allow task-level parameters next to job parameters, so `sources`
  is a job parameter.
- Both Jobs run on the existing all-purpose cluster `used-cars-cluster` and have queueing on (a second run waits
  instead of overwriting the same tables in parallel).
- **Why the service book is a Job and not ADF Notebook activities:** `used-cars-cluster` is a dedicated
  (single user) cluster. A Notebook activity runs as the ADF managed identity and is rejected
  (`Single-user check failed`); a Job runs as its owner and is accepted. A new job cluster per activity would also
  work, but it adds ~5 minutes of cluster start per activity.

## ADF objects

All exported from ADF Studio (code view `{}`) into `adf/`:

| Object | File |
|---|---|
| Master pipeline | `adf/pipeline/pl_daily_master.json` |
| landing → Bronze copy | `adf/pipeline/pl_ingest_bronze_sources.json` |
| Silver vehicle listings (Job activity) | `adf/pipeline/pl_silver_vehicle_listings.json` |
| Silver service events (Job activity) | `adf/pipeline/pl_silver_service_events.json` |
| Databricks linked service | `adf/linkedService/ls_databricks.json` |
| Daily trigger | `adf/trigger/tr_daily_master.json` |

The datasets and the storage linked service of `pl_ingest_bronze_sources` are not exported yet (ADF Git
integration was deferred).

## Authentication (no secrets)

- **ADF → Databricks:** linked service `ls_databricks` with **Managed Service Identity**. ADF uses its own Azure
  identity, so there is no token or password to store. The linked service points to the workspace and to the
  existing cluster (`existingClusterId`); the Job activity runs each Job on the cluster defined in the Job.
- **Databricks → storage:** OAuth with the service principal whose credentials come from the Key Vault-backed
  secret scope `key-vault-secrets` (see `notebooks/00_config.py`).
- The exported JSON contains only names and resource ids (workspace URL, subscription and resource group path,
  cluster id), no credentials.

## Trigger and cost

The trigger `tr_daily_master` exists but is **Stopped**. Every run copies ~2.5 GB into a new Bronze partition and
keeps the cluster busy for ~10 minutes, and the subscription has limited credits. It will be started once the
mentor confirms the setup.

## Verified (2026-11-09, ADF Debug runs of `pl_daily_master`)

- `dev_mode=true`: all three steps succeeded; ADF created the Bronze partition `ingestion_date=2026-11-09` and the
  Silver Jobs read it (stage tables `2026-11-09`, 4 × 5,000 rows, `*_dev` tables).
- `dev_mode=false` (ingest 40 s, vehicle Job 4 min 10 s, service Job 2 min 43 s):
  - stage tables 4 × 106,970 rows from `2026-11-09`; `vehicle_listings` 427,880 rows, valid % First 94.20,
    Second 94.29, Third 94.46, Fourth 94.32; brands lookup 412,552 / listing URL 3,181 / unknown 12,147;
  - `service_events` 8,312,490 rows, 0 duplicates, 419,641 of 419,641 service-book vehicles (100%) present in the
    same day's `vehicle_listings`.
- The results equal the earlier manual runs, so moving the scheduling to ADF changed no data.

## Open questions for the mentor

- Is "ADF starts Databricks Jobs" the split he meant, and should the vehicle files keep being copied landing →
  Bronze by ADF (or by Databricks)?
- The per-table ADF pipelines (`pl_silver_vehicle_listings`, `pl_silver_service_events`) contain one Job activity
  each; the master could call the Jobs directly instead. Keep them (one pipeline per table) or simplify?
