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
| Exchange rates API → Bronze | ADF (`pl_ingest_exchange_rates`, Copy activity with an HTTP source) |
| Cleaning and combining (Silver), Gold | Databricks notebooks, grouped into Databricks Jobs |

ADF never processes data itself: it starts Databricks Jobs with the **Databricks Job activity**, waits for them
and fails when a Job fails. A Job keeps the task graph that belongs to Databricks (for example the four vehicle
sources in parallel), so the same Job can also be started by hand in Databricks.

## Daily master pipeline

```
tr_daily_master  (schedule trigger, every day 02:00 Europe/Belgrade – created, currently STOPPED)
└─ pl_daily_master                                   parameters: ingestion_date=latest, dev_mode=false
   ├─ ingest_bronze            Execute Pipeline pl_ingest_bronze_sources   6 × Copy: landing → bronze/<Source>/ingestion_date=…
   ├─ ingest_exchange_rates    Execute Pipeline pl_ingest_exchange_rates   Copy: Frankfurter API → bronze/ExchangeRates/…
   │                           (both start at once, no dependency)
   ├─ silver_vehicle_listings  Job activity  after ingest_bronze: Succeeded
   ├─ silver_service_events    Job activity  after silver_vehicle_listings: Succeeded
   ├─ silver_exchange_rates    Job activity  after silver_vehicle_listings: Succeeded
   │                                         and ingest_exchange_rates: Completed
   ├─ fx_ingest_failed         Fail activity after ingest_exchange_rates: Failed
   └─ gold                     Job activity  after silver_service_events and silver_exchange_rates: Succeeded
```

- **Jobs are started directly** with Databricks Job activities (mentor, 2026-11-10). The earlier one-activity
  wrapper pipelines `pl_silver_vehicle_listings` / `pl_silver_service_events` were deleted. The ingest pipelines
  stay separate pipelines, because they hold ADF copy logic (six Copy activities; history parameters) and can be
  run on their own.
- A Silver step starts only when its inputs are complete (`Succeeded`), and every Execute Pipeline activity has
  **Wait on completion** on, so a step never reads data that is still being written. If the vehicle step fails, the
  steps after it do not run and the master is marked failed. The Silver tables keep their previous (complete)
  version: every write is a Delta overwrite, which is atomic.
- `silver_service_events` and `silver_exchange_rates` run **after** `silver_vehicle_listings`: the coverage check
  of the service book and the priced view (row count, rate age against the listings' `ingestion_date`) must see
  the listings of the same day, not a table that is being rewritten.
- **The exchange rate API must not stop the day.** `silver_exchange_rates` waits for the copy with the condition
  *Completed* (succeeded or failed), so the listings, the service book and the priced view are processed even when
  the API is down; the view then uses the last delivered rates and `06` fails only when they are more than 5 days
  older than the listings.
- **But the failure must be visible.** ADF decides the pipeline status from its last ("leaf") activities; a failed
  activity whose failure is handled by a *Completed* dependency would leave the master green. The Fail activity
  `fx_ingest_failed` runs only when the copy failed and marks the whole master run as failed with the message
  "Exchange rate API copy failed, prices use the last available rates" (error code `FX_INGEST_FAILED`).
- The parameters `ingestion_date` and `dev_mode` are passed master → Job activity (`jobParameters`) → Job → notebook
  widget. Every Job activity must list both: a Job activity without them silently runs the Job with its own
  defaults (`dev_mode=false`, found in the first test of 2026-11-10). `dev_mode=true` runs the whole chain on
  samples and writes to the `*_dev` tables (cheap test of the wiring); `ingestion_date=YYYY-MM-DD` reprocesses one
  Bronze partition (backfill). Both are **String** parameters: the notebooks compare the widget with the text
  `"true"`, and an ADF Bool would arrive as `"True"`.
- **`pl_ingest_exchange_rates`:** linked service `ls_frankfurter_api` (HTTP, anonymous – a public API without a
  key, so nothing goes to Key Vault), source dataset HTTP + Binary, sink dataset Binary in
  `bronze/ExchangeRates/ingestion_date=<today>/`. Binary keeps the response byte for byte, like every Bronze
  copy. Parameters `history_from` / `history_to`:
  - empty = the daily call, a sliding window of the last 7 days, written as `rates_latest.json`. The source
    dataset's `relative_url` is
    `concat('v2/rates?base=EUR&quotes=USD,GBP,AUD&providers=ecb&from=', formatDateTime(addDays(utcNow(), -7), 'yyyy-MM-dd'), '&to=', formatDateTime(utcNow(), 'yyyy-MM-dd'))`.
    The window repeats the last working days, so a day missed during a short ADF outage, or lost when a second run
    on the same day overwrites the file, is delivered again (see `documentation/silver_data_model.md`);
  - filled = a one-time history call `rates_<from>_<to>.json`. Retry 3 × 60 s and a 10-minute timeout, because an external API can be
  briefly unavailable or hang.

## Databricks Jobs

| Job | Tasks | Definition |
|---|---|---|
| `silver_vehicle_listings` | `validate_landing_and_bronze` (sources=vehicle_listings) → 4 × `stage_<source>_source` and `brand_lookup` in parallel → `vehicle_listings` | `orchestration/databricks_job_silver_vehicle_listings.yml` |
| `silver_service_events` | `validate_landing_and_bronze` (sources=service_book) → `service_events` | `orchestration/databricks_job_silver_service_events.yml` |
| `silver_exchange_rates` | `exchange_rates` (05) → `vehicle_listings_priced` (06, creates the view) | `orchestration/databricks_job_silver_exchange_rates.yml` |
| `gold` | `dim_date`, `dim_vehicle`, `dim_small` (07, parallel) → `fact_listing`, `fact_service_event`, `fact_exchange_rate` (08, parallel) → `views` (08, Unity Catalog views `gold.<table>` and `vw_listing_current_price`) | `orchestration/databricks_job_gold.yml` |

- Job parameters (`ingestion_date`, `dev_mode`, `sources`, and `load_mode` in `silver_exchange_rates`) reach every
  notebook task as widgets; a notebook without such a widget ignores it. `sources` tells `01_validate_landing_and_bronze` which landing/Bronze folders
  belong to this Job. The Databricks UI does not allow task-level parameters next to job parameters, so `sources`
  is a job parameter.
- `load_mode` (`silver_exchange_rates` only, default `incremental`): `05_silver_exchange_rates` merges only the
  new Bronze partitions into `exchange_rates`. The ADF master does not pass it, so the daily run uses the default;
  a backfill or rebuild is started by hand from the Job UI with `load_mode=full` (see
  `documentation/silver_data_model.md`).
- All Jobs run on the existing all-purpose cluster `used-cars-cluster` and have queueing on (a second run waits
  instead of overwriting the same tables in parallel).
- **Why the service book is a Job and not ADF Notebook activities:** `used-cars-cluster` is a dedicated
  (single user) cluster. A Notebook activity runs as the ADF managed identity and is rejected
  (`Single-user check failed`); a Job runs as its owner and is accepted. A new job cluster per activity would also
  work, but it adds ~5 minutes of cluster start per activity.

## Logging and monitoring

- **Progress of a run:** ADF Monitor → Pipeline runs → `pl_daily_master` shows every activity with its status,
  duration and error. The output of a Job activity has `runPageUrl`, a link to the Databricks Job run. In
  Databricks, Jobs & Pipelines → Job → Runs shows the task graph of each run.
- **Inside a task:** every pipeline notebook logs with `get_logger("<notebook>")` from `00_config` (Python
  `logging`): one line per step with time, level and notebook, e.g.

  ```
  2026-11-12 02:14:03 INFO    [05_silver_exchange_rates] incremental load, watermark=2026-11-11: reading 2 of 3 partitions [...]
  2026-11-12 02:14:25 INFO    [05_silver_exchange_rates] Merged into abfss://.../exchange_rates: 3 rows inserted, 0 rows updated
  2026-11-12 02:20:41 WARNING [04_silver_service_events] 12 service_date values could not be parsed (...)
  ```

  The lines appear in the task output of the Job run (and in the notebook when it is run by hand). `WARNING` marks
  something to look at that did not stop the run; a problem that makes the data wrong stops the run with an
  assertion instead.
- The log lives with the run; it is not stored in a table. A persistent `run_log` table (rows and DQ counts per
  day) was discussed with the mentor and left for later.

## ADF objects

All exported from ADF Studio (code view `{}`) into `adf/`:

| Object | File |
|---|---|
| Master pipeline | `adf/pipeline/pl_daily_master.json` |
| landing → Bronze copy | `adf/pipeline/pl_ingest_bronze_sources.json` |
| Databricks linked service | `adf/linkedService/ls_databricks.json` |
| Exchange rate API → Bronze copy | `adf/pipeline/pl_ingest_exchange_rates.json` |
| Frankfurter API linked service (HTTP, anonymous) | `adf/linkedService/ls_frankfurter_api.json` |
| API source dataset (HTTP, Binary, `relative_url` parameter) | `adf/dataset/ds_frankfurter_rates.json` |
| Bronze sink dataset (Binary, `ExchangeRates/ingestion_date=<today>/<file_name>`) | `adf/dataset/ds_bronze_exchange_rates_sink.json` |
| Daily trigger | `adf/trigger/tr_daily_master.json` |

The datasets and the storage linked service of `pl_ingest_bronze_sources` are not exported yet (ADF Git
integration was deferred).

**Verified (2026-11-10, ADF Debug of `pl_ingest_exchange_rates`):** the history call (`history_from=2020-01-01`,
`history_to=2026-11-10`) wrote `rates_2020-01-01_2026-11-10.json` (333 KB, 2019-12-31 → 2026-11-09, working
days only, three currencies per day) and the daily call wrote `rates_latest.json` (190 bytes, 2026-11-09) into
`bronze/ExchangeRates/ingestion_date=2026-11-10/`; read and written byte counts are equal.

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
keeps the cluster busy for ~10–15 minutes, and the subscription has limited credits. It will be started once the
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

## Verified (2026-11-10, after adding the `brand_lookup` task)

- The lookup is copied into `bronze/Lookup/ingestion_date=…` (sink dataset `ds_bronze_lookup_sink` uses the same
  date expression as the other sources) and cleaned by the new task `brand_lookup` in `silver_vehicle_listings`.
- The `silver` container was emptied first (old tables, `*_dev` tables and the Delta history of all earlier test
  runs), then `pl_daily_master` ran with `dev_mode=true` and `dev_mode=false` (`ingestion_date=2026-11-10`).
- Full run: `brand_lookup` 23,756 models / 44 manufacturers; `vehicle_listings` 427,880 rows, the same valid %
  and brands (412,552 / 3,181 / 12,147); `service_events` 8,312,490 rows, 100% coverage. Nothing changed.

## Verified (2026-11-10, ADF Debug runs of the new `pl_daily_master`)

- `dev_mode=false`: all five activities succeeded (ingest 34 s / 23 s in parallel, vehicle Job 4 min 22 s, then
  service Job 3 min 43 s and exchange rate Job 2 min 27 s in parallel); `fx_ingest_failed` was skipped.
- `dev_mode=true` with a broken API URL (negative test; `ls_frankfurter_api` pointed to a wrong path in the debug
  session only, not published): `ingest_exchange_rates` failed after 4 min 25 s (1 + 3 retries),
  `fx_ingest_failed` failed with its message, the three Jobs still succeeded – `silver_exchange_rates` waited for
  the failed copy and built `vw_vehicle_listings_priced_dev` with the last delivered rates – and the master run was
  marked **Failed**.
- The first dev test had no `jobParameters` on the `silver_exchange_rates` activity, so that Job ran with
  `dev_mode=false`; fixed before the runs above.

## Verified (2026-11-13, ADF Debug of `pl_daily_master` with the `gold` Job activity, full data)

`ingestion_date=latest`, `dev_mode=false`, warm cluster; every activity succeeded, `fx_ingest_failed` did not run.

| Activity | Start | Duration |
|---|---|---|
| `ingest_bronze` | 11:21:36 | 39 s |
| `ingest_exchange_rates` | 11:21:36 | 23 s |
| `silver_vehicle_listings` | 11:22:15 | 4 min 10 s |
| `silver_service_events` | 11:26:27 | 3 min 7 s |
| `silver_exchange_rates` | 11:26:27 | 2 min 45 s |
| `gold` (Job ID 626529156910056) | 11:29:36 | 3 min 5 s |

The whole day, landing → Bronze → Silver → Gold star schema, takes **about 11 minutes** on a warm cluster (about
5 minutes more when the cluster has to start, as in the nightly trigger run).

## Open questions for the mentor

- Is "ADF starts Databricks Jobs" the split he meant, and should the vehicle files keep being copied landing →
  Bronze by ADF (or by Databricks)?
- ~~Keep the one-activity wrapper pipelines?~~ Answered on 2026-11-10: no, the master calls the Jobs directly
  (see "Daily master pipeline").
