# Used Vehicle Data Pipeline (Azure, Medallion architecture)

A production-style data pipeline for used vehicle data built on Azure for the Vega IT Data Engineering Internship.
Raw files from four listing source systems, a vehicle service book and a model-to-brand lookup, together with daily
exchange rates from a public API, are moved through **Bronze → Silver → Gold** layers into a **star schema** for
Business and Data Analysts, including a **Reliability Score (0–100)** per vehicle.

## Architecture

```
Azure Data Factory: pl_daily_master (daily trigger 02:00, Europe/Belgrade)

landing/ (6 source files) ─► ingest_bronze ─► silver_vehicle_listings ─┬─► silver_service_events ─┐
                                                                        │                          ├─► silver_dq ─► gold
Frankfurter API (ECB)     ─► ingest_exchange_rates ──(Completed)────────┴─► silver_exchange_rates ─┘
                                  └─(Failed)─► fx_ingest_failed (marks the day as failed)

ADLS Gen2: landing ─► bronze ─► silver ─► gold          ADF copies and schedules; Databricks Jobs process
```

| Layer | Storage | What happens |
|---|---|---|
| **Landing** | `landing/<Source>/` | Source files as delivered (manual upload, the "source systems"). |
| **Bronze** | `bronze/<Source>/ingestion_date=YYYY-MM-DD/` | Byte-for-byte copies made by ADF, partitioned by source and ingestion date. Exchange rates come from the Frankfurter API (official ECB rates) as raw JSON. |
| **Silver** | `silver/<table>` (Delta) | Cleaned, typed and standardized tables: one notebook per vehicle source, union into `vehicle_listings`, brand mapping, `service_events`, incremental `exchange_rates`, and the view `vw_vehicle_listings_priced` with prices in GBP, USD and AUD at the latest rates. Invalid values become null and are listed in `dq_issues`; rows are never deleted. |
| **Gold** | `gold/<table>` (Delta) | Star schema (fact constellation) with business-ready values, prices at the posting-date rates, the Reliability Score, and Unity Catalog views for analysts. |

**Technology:** Azure Data Lake Storage Gen2, Azure Data Factory (copying and scheduling), Azure Databricks
(Runtime 18, Spark 4.1, Delta Lake, Unity Catalog views), Azure Key Vault, Python/PySpark, pytest.

## Gold data model

Six dimensions shared by four fact tables (diagram source: [`documentation/gold_star_schema.dbml`](documentation/gold_star_schema.dbml),
paste it into https://dbdiagram.io).

| Table | Grain | Rows (full data) |
|---|---|---|
| `dim_date` | one day, 1950-01-01 → 2030-12-31 (role-playing: posting, service and rate date) | 29,586 |
| `dim_vehicle` | one vehicle (conformed: used by listings, service events and reliability) | 427,881 |
| `dim_location` / `dim_platform` / `dim_currency` | state + region / listing platform / currency | 427 / 5 / 5 |
| `dim_service_type` | service classification incl. the kind of failure (junk dimension) | 8 |
| `fact_listing` | one listing, with prices in EUR/GBP/USD/AUD at the posting-date rates | 427,880 |
| `fact_service_event` | one service event (cost, downtime, odometer) | 8,312,490 |
| `fact_exchange_rate` | one ECB working day × currency (EUR = 1 included) | ~7,000 |
| `fact_vehicle_reliability` | one vehicle: service summary, penalties and the Reliability Score | 427,880 |

Surrogate keys are deterministic (`xxhash64` of the natural key, `yyyyMMdd` for dates), every dimension has an
"Unknown" member (`-1`), and analysts query the tables by name, e.g.
`SELECT * FROM dbw_used_cars_strahinja.gold.fact_listing`, plus `gold.vw_listing_current_price` for prices at
today's rates. Details: [`documentation/gold_data_model.md`](documentation/gold_data_model.md).

## Reliability Score

Every vehicle starts at 100 and loses explainable points, each stored in its own column:

- **failures** (from the repair notes): engine failure 35, transmission 30, head gasket 20 per incident, plus 0.5 per day out of service;
- **minor body repairs:** 5 each;
- **skipped big services:** 5 each, at most 15;
- **late oil changes:** at most 10;
- **wear:** 1 point per 5 years of age and per 20,000 km, at most 10, raised up to ×1.5 when maintenance was neglected.

Vehicles with only regular maintenance score 100; vehicles without any service history get no score ("Unknown").
Categories: Excellent ≥ 90, Good ≥ 75, Fair ≥ 50, Poor. The profiling behind every weight, worked examples and the
verified distribution are in [`documentation/reliability_score.md`](documentation/reliability_score.md).

## Change Data Capture demo

A local demonstration of **log-based CDC** with **SCD Type 2 history** ([`documentation/cdc.md`](documentation/cdc.md)),
next to the daily file pipeline:

- A PostgreSQL database in Docker plays the operational database of a listing platform, filled with 80 % of a sample
  of the Fourth source.
- A simulator makes daily changes: new listings from the other 20 %, price drops, condition changes, sold cars.
- `cdc/cdc_reader.py` reads only the changes from the database's write-ahead log (replication slot + wal2json) into
  Bronze.
- Silver applies them with MERGE into one table with every version (`valid_from` / `valid_to` / `is_current`).
- Gold rebuilds an SCD2 `dim_vehicle` and a `fact_listing_event` from that history.

In the verified 3-day run, every day processed about 7 % of the rows a full load would read. The Silver current
state always equalled the source database, and a reader crash before confirming changed nothing.

```bash
.venv/bin/pip install -r cdc/requirements.txt
.venv/bin/python -m cdc.run_demo --days 3 --crash-test
```

## Repository structure

```
adf/                    ADF pipelines, linked services, datasets and trigger (JSON exported from ADF Studio)
cdc/                    CDC demo: PostgreSQL source (Docker), change simulator, WAL reader, local runner
documentation/          silver_data_model.md, gold_data_model.md, gold_star_schema.dbml,
                        reliability_score.md, orchestration.md, data_quality.md, cdc.md, cdc_gold_schema.dbml
notebooks/              Databricks notebooks (source format), numbered in execution order
  00_config.py          storage access through the Key Vault-backed secret scope, paths, sources, logger
  01_…                  landing vs Bronze validation
  02_… – 06_…           Silver: one stage per vehicle source, brand lookup, union, service events,
                        exchange rates (incremental MERGE), priced view, data quality checks (gate before Gold)
  07_… – 09_…           Gold: dimensions, facts, Unity Catalog views, Reliability Score
  10_… – 11_…           CDC demo: Silver version history (SCD2) and Gold SCD2 dimension + change facts
  utils/                shared helpers (cleaning, data quality, Delta writes, Gold keys and weights)
  profiling/            read-only profiling notebooks behind the cleaning rules and the score
orchestration/          Databricks Job definitions (YAML, "View as code" format)
tests/                  pytest suite that runs the notebooks locally on samples of the real files
```

## How it runs

1. **ADF `pl_daily_master`** (trigger `tr_daily_master`, daily at 02:00) copies the landing files into a new Bronze
   partition and, in parallel, calls the exchange rate API for the last 7 days (a sliding window that heals missed days).
2. It then starts the **Databricks Jobs** directly (no wrapper pipelines):
   - `silver_vehicle_listings`: validate → 4 source stages + brand lookup in parallel → union;
   - `silver_service_events`;
   - `silver_exchange_rates`: runs even when the API copy failed (it then uses the last rates), while a Fail activity marks the day as failed;
   - `silver_dq`: table-level data quality rules on all Silver tables; Gold starts only when no error rule failed;
   - `gold`: dimensions → facts and reliability → views.
3. The whole day (landing → Gold) takes about **11 minutes** on a warm cluster and about 14 minutes including
   cluster start.

Every notebook also runs on its own in Databricks. The widgets/Job parameters are:
- `ingestion_date`: `latest` or a Bronze date;
- `dev_mode`: `true` uses samples and writes `*_dev` tables;
- `load_mode` (exchange rates only): `incremental` or `full`.

Details, parameters and verified runs: [`documentation/orchestration.md`](documentation/orchestration.md).

## Security

No credentials are stored in code or in git:
- Databricks reads storage with a service principal whose secrets come from the **Key Vault-backed secret scope**
  `key-vault-secrets`;
- ADF reaches Databricks with its **managed identity**;
- the exchange rate API is public and needs no key.

## Data quality

Three levels (details: [`documentation/data_quality.md`](documentation/data_quality.md)):

- **per value:** an impossible value becomes null and is flagged in `dq_issues` / `is_valid`; rows are never deleted;
- **before a write:** every notebook checks its own output and stops the run when something is wrong;
- **whole Silver layer:** `06_silver_dq_checks` checks the Silver tables against thresholds every day (error rules
  such as duplicate keys or stale rates, warning rules such as twice the usual share of invalid values), appends
  every result to `silver.dq_results` and **gates Gold**: when an error rule fails, the Gold Job does not start.

The checks before a write cover:
- documented columns and unique keys;
- foreign keys in Gold, and row counts and cost totals equal to Silver;
- valid exchange rates and their age.

Notebooks log timestamped progress lines (`get_logger`) into the Job task output.

## Local tests

The notebooks run unchanged on a local SparkSession (PySpark 4.1 + delta-spark 4.1, matching Databricks Runtime 18,
ANSI mode on; Java 17 required). The tests use samples of the source files in `data/`, which are not in the
repository.

```bash
python -m venv .venv
.venv/bin/pip install pyspark==4.1.4 delta-spark==4.1.0 pandas pytest
.venv/bin/python -m pytest                          # fast set: everything except the tests marked slow
.venv/bin/python -m pytest -m "slow or not slow"    # whole suite (~10 minutes)
```

Tests marked `slow` are extra full pipeline runs whose notebooks a cheaper test already covers. The Silver of the
sample is built once per run (`sample_silver` fixture) and shared by the Gold, data quality and view tests.

## Open points

- The daily trigger is created but stopped until the setup is approved (shared subscription with limited credits).
- Tables are path-based Delta tables exposed through Unity Catalog views; registering them as tables needs a
  Unity Catalog External Location for the containers.
- The unit of the listings' odometer (km or miles) is not confirmed.
