# Incremental file load with watermarks and SCD Type 2 history

A local demonstration of how the pipeline loads a source that **grows every day**: only the new records are moved
through the layers, and Silver keeps every version of a listing. It runs next to the daily pipeline and does not
change it; everything runs on a laptop (local Spark), so it uses no Azure credits.

## Task (mentor, 2026-11-25)

- Take one of the source files and split it into **fifths**; every fifth gets a date one day later than the
  previous one.
- Every run moves the **next fifth** from the original file into a **new source file**, which is the input of
  Bronze.
- A **watermark** remembers how far the load has come in the original file.
- **SCD2:** Silver keeps the history (`valid_from`, `valid_to`, `is_current`); **Gold holds only the current state**.

Decisions with the user:
- every fifth brings **new listings plus changed versions** of listings published on earlier days, so the SCD2
  history has something to track;
- **no deletes**;
- the source is the Fourth file.

## Flow

```
data/FourthSourceSystem (106,970 listings)
   │  once: incremental/prepare_original.py
   ▼
landing/IncrementalSource/original/   "original file": 5 days, new listings + changed versions,
   │                                   every record with updated_at (its day) and change_seq (its order)
   │  12_incremental_extract_source    watermark original_to_source = last day copied
   ▼
landing/IncrementalSource/source/     "new source": grows by one day per run
   │  13_incremental_ingest_bronze     watermark source_to_bronze = last day loaded
   ▼
bronze/IncrementalListings/ingestion_date=<day>/
   │  14_silver_listings_history       watermark = highest change_seq already in the table
   ▼
silver/vehicle_listings_history       every version: valid_from / valid_to / is_current
   │  15_gold_incremental_dim_vehicle  full rebuild
   ▼
gold/incremental/dim_vehicle          current state only: one row per vehicle
```

## The original file

`build_original` in `incremental/prepare_original.py`:
1. Shuffles the Fourth listings (deterministic seed) and splits them into 5 fifths. Fifth *k* appears on day *k*,
   with `updated_at` = 2026-11-01 + (*k* − 1).
2. From day 2 on, every day also changes listings published on an **earlier** day:
   - ~3 % get a price drop of 3–15 % and a few more kilometres;
   - ~0.5 % get a worse condition or another title status.

   This is the same logic as the CDC simulator (`drop_price`, `next_attribute` in `cdc/simulate_changes.py`). A
   listing changes at most once per day.
3. `change_seq` numbers all records in the order they happen. It orders the versions of a listing, like the WAL
   position in the CDC demo.

## Watermarks

A watermark is how far a load has come. The two file loads keep theirs in a small Delta control table,
`bronze/_control/watermarks`, with the columns `pipeline`, `watermark_value` and `updated_at_run`
(`read_watermark` / `write_watermark` in `notebooks/utils/incremental_helpers.py`).

| Load | Watermark | A run takes |
|---|---|---|
| original → source (`12`) | `original_to_source`: last day copied | the next day after it (widget `days`, default 1) |
| source → Bronze (`13`) | `source_to_bronze`: last day loaded | all records with `updated_at` after it, one Bronze partition per day |
| Bronze → Silver (`14`) | highest `change_seq` in the Silver table (no extra state, like the exchange rates) | the Bronze days from the newest one in the table on, records after the watermark |

**Data first, watermark second.** A run writes the data and moves the watermark only afterwards. A crash in
between means the same day is loaded again (at-least-once), and the next layers absorb the repeat:
- Bronze overwrites the day's partition;
- Silver skips records whose `change_seq` it already has.

The opposite order could lose a day for good.

## Silver: SCD Type 2 (`14_silver_listings_history`)

The same model and code as the CDC demo (`notebooks/utils/scd2_helpers.py`):
- `silver/vehicle_listings_history` has the columns of `silver.vehicle_listings` plus `version_key`, `row_hash`,
  `change_op` (I = first record of a listing, U = a later one), `change_seq`, `valid_from` (the record's
  `updated_at`), `valid_to`, `valid_to_seq` and `is_current`;
- the records are cleaned with the same `clean_vehicle_listings` and `add_manufacturer` as the files;
- a record opens a new version only when a cleaned business value changed (e.g. a price drop on a price that is
  invalid anyway does not);
- MERGE on `version_key`, then the SCD2 rules are checked on the whole table: one current version per listing,
  consistent periods, no overlaps;
- the current state is `WHERE is_current`.

The table is read **pinned to its version at the start of the run** (`read_table_snapshot`). A plain read is lazy
and can run again after the MERGE. The first demo run caught this: every vehicle looked already known, so new
listings were labelled U instead of I.

## Gold: current state only (`15_gold_incremental_dim_vehicle`)

`gold/incremental/dim_vehicle` = the current Silver versions, one row per vehicle:
- the same business-ready attributes and key as the main `gold.dim_vehicle`, without `has_service_history`;
- the Unknown member −1;
- rebuilt on every run (`build_current_dim_vehicle`, shared with the CDC demo; diagram
  [`scd2_gold_schema.dbml`](scd2_gold_schema.dbml)).

Questions over time (price history, how often a listing changed) are asked on the Silver history.

## File or database: which approach when

| | Incremental file load (this) | Log-based CDC ([`cdc.md`](cdc.md)) |
|---|---|---|
| Source | files or exports that grow | the source's database |
| What is new | watermark on a date / sequence in the data | the database's write-ahead log (LSN) |
| Deletes | only if the file marks them | seen automatically |
| Needs | a reliable change date in the data | access to the database (replication slot) |

Both fill the same Silver SCD2 table model and the same current-only Gold dimension.

## How to run

```bash
.venv/bin/python -m incremental.run_demo           # 5 days on the full Fourth file; writes to incremental_data/
```

The runner:
1. resets `incremental_data/`;
2. builds the original file and the brand lookup;
3. runs `12 → 13 → 14 → 15` once per day and checks every day (`incremental/verify.py`):
   - both watermarks are on that day;
   - the source and Bronze hold exactly the days so far;
   - the current Silver state equals the latest record of every listing in the source, after the same cleaning;
   - the number of versions equals the number of records that changed a business value;
   - there is one I version per listing;
   - Gold has one row per current version;
4. at the end:
   - re-runs every step without a new day, and nothing may change;
   - simulates a crash of the Bronze load (watermark moved back), loads the day again, and Silver must not change.

The notebooks run unchanged on Databricks (`days`, `ingestion_date`, `dev_mode` widgets), with the original file
uploaded to `landing/IncrementalSource/original/`.

**Tests:** `tests/test_incremental.py` uses a small hand-made original file (12 listings, 3 days). It covers:
- the split and the changes;
- one day per run through every layer;
- the versions and the I/U labels;
- the current state;
- Gold;
- a re-run without a new day;
- a repeated Bronze load after a crash;
- a missing source.

## Verified run (2026-11-25, full Fourth file)

| Day | Records loaded (new / changed) | Records in the source | Silver versions | Current vehicles | Seconds |
|---|---|---|---|---|---|
| 2026-11-01 | 21,394 (21,394 / 0) | 21,394 | 21,394 | 21,394 | 44 |
| 2026-11-02 | 22,143 (21,394 / 749) | 43,537 | 43,537 | 42,788 | 48 |
| 2026-11-03 | 22,892 (21,394 / 1,498) | 66,429 | 66,428 | 64,182 | 51 |
| 2026-11-04 | 23,640 (21,394 / 2,246) | 90,069 | 90,067 | 85,576 | 58 |
| 2026-11-05 | 24,389 (21,394 / 2,995) | 114,458 | 114,455 | 106,970 | 68 |

- Every run loaded only that day's records (21–24 thousand), not the whole source (114 thousand on day 5).
- 3 changed records made no new version: their price drop was on a price that Silver treats as invalid (null), so
  no cleaned business value changed.
- The re-run without a new day changed nothing. The Bronze load repeated after a simulated crash left Silver
  unchanged.

**Local Spark note:** pyspark 4.1.4 cannot read the Delta checkpoint files that delta-spark 4.1.0 writes after 10
commits (`NoSuchMethodError`). The watermark table reached 10 commits in the demo. The local session and the tests
therefore create checkpoints only every 1,000 commits. Databricks is not affected.
