# Change Data Capture (CDC) demo with SCD Type 2 history

A local demonstration of how the pipeline would load listings if it could read the **database of a listing
platform** instead of receiving full files. It runs next to the daily pipeline and does not change it. Everything
runs on a laptop (Docker + local Spark), so it uses no Azure credits.

## Why CDC

The daily pipeline gets **full snapshots**: every file holds all listings, and Silver and Gold are overwritten
every day. That is simple, but it has three costs:

- **Work:** every row is processed every day, even though only a few percent changed.
- **No history:** an overwrite keeps only the latest state. A price drop, or the day a car was sold, is lost.
- **Deletes are invisible:** a listing that is missing from a file looks the same as a listing that was sold.

CDC captures **only the changes**: INSERT (new listing), UPDATE (new price, more kilometres, another condition)
and DELETE (sold).

| Method | How the changes are found | Fits when |
|---|---|---|
| Snapshot comparison | compare today's full file with yesterday's, key by key | the source only sends files |
| Timestamp / watermark | rows with `updated_at` after the last load | the source has a reliable `updated_at`; deletes are not seen |
| **Log-based** (this demo) | read the database's own write-ahead log | we may read the source database; sees every change, deletes included, in order |
| Triggers | the database writes every change into a change table | no log access; slows the source down |

The demo uses the **log-based** method. It is the most complete method and the one used in production with
Debezium.

## Architecture

```
data/FourthSourceSystem ─► sample of 20,000 listings, split: 80 % = database on day 0, 20 % = reserve of "new" listings

cdc/simulate_changes ─► PostgreSQL 16 (Docker, wal_level=logical, table listings, REPLICA IDENTITY FULL)
 (INSERT from the reserve,                │ write-ahead log (WAL)
  UPDATE price/km/condition,              ▼
  DELETE = sold)               cdc/cdc_reader ── replication slot + wal2json ──► bronze/ListingChanges/ingestion_date=…/*.json
                                                                                     │
                       notebooks/10_silver_listing_changes ──► silver/vehicle_listings_cdc   (every version, SCD2 columns)
                       notebooks/11_gold_cdc_vehicle_history ─► gold/cdc/dim_vehicle         (current state only)
```

The PostgreSQL database plays the **source system** (the platform's operational database), not our storage.
Its table holds the **raw** listing columns exactly as in the files. Cleaning stays the job of Silver, with the
same functions as the file pipeline.

## Source side: reading the write-ahead log

- **WAL:** PostgreSQL writes every change to its write-ahead log before it changes the table (so it can recover
  after a crash). The log therefore contains every change, in commit order.
- **Logical decoding** turns the binary WAL records into readable row changes. The output plugin
  **wal2json** (format version 2) gives one JSON row per change. Each row has the action (I/U/D), the commit time,
  the WAL position (LSN), the new row (`columns`) and the old row (`identity`).
- **`REPLICA IDENTITY FULL`** on the table makes PostgreSQL log the *whole* old row of an UPDATE / DELETE, not
  only the primary key. Every event therefore carries a complete **before** image.
- **Replication slot:** our bookmark in the WAL. PostgreSQL keeps every WAL record after the slot's confirmed
  position until we confirm it, so a reader that is down for a while loses nothing. The cost: a reader that never
  comes back makes the database keep WAL forever. In production, the slot lag is monitored
  (`cdc_reader.slot_lag_bytes()`).
- **LSN** (Log Sequence Number), e.g. `0/1A2B3C`, is the position in the WAL. As a number
  (`lsn_num` = high << 32 | low), it orders all changes and serves as the watermark.

**Initial snapshot (once).** The slot is created with `CREATE_REPLICATION_SLOT … EXPORT_SNAPSHOT`, and the table
is read inside that exported snapshot. The table copy and the slot therefore start at the same point in the
log: no change is missed and none is counted twice (Debezium does the same). Snapshot rows become events with
`op = "R"`.

**Every run afterwards** (`cdc_reader.read_changes`):
1. peek all new changes (`pg_logical_slot_peek_changes`);
2. write them as one NDJSON file into Bronze, under a temporary name and then renamed, so Silver never sees
   half a file;
3. **only then** confirm (`pg_replication_slot_advance`, up to the end of the last transaction).

A crash between steps 2 and 3 means that the same changes are delivered again (**at-least-once**). Silver must
therefore be idempotent (below).

**Event format** (`bronze/ListingChanges/ingestion_date=YYYY-MM-DD/changes_<from>_<to>_<written at>.json`):

```json
{"op": "U", "lsn": "0/D1A2680", "lsn_num": 219817600, "commit_ts": "2026-11-24T15:24:25.123456Z",
 "table": "listings", "before": {"id": "…", "price": 15000.0, "…": "…"}, "after": {"id": "…", "price": 13500.0, "…": "…"}}
```

**Pitfall found during the demo – TOAST:** PostgreSQL stores long values (here: the long listing descriptions)
outside the row (TOAST). An UPDATE that does not change such a value does not repeat it in the WAL, so
wal2json leaves the column out of the new row. The first run therefore showed 304 listings whose description
"disappeared" after a price change. The reader now takes a missing column of an UPDATE from the old row; it is
unchanged by definition. Debezium has the same gap and writes a placeholder value instead.

## Silver: one table with every version (`10_silver_listing_changes`)

`silver/vehicle_listings_cdc` has the columns of `silver.vehicle_listings` plus:

| Column | Meaning |
|---|---|
| `version_key` | one version: `xxhash64(vehicle_id, change_seq, change_op)` |
| `row_hash` | SHA-256 of the cleaned business columns (lineage and DQ summary excluded) |
| `change_op` | R / I / U: the change that opened the version |
| `change_seq` | order of that change in the source: here its WAL position (`lsn_num` of the event) |
| `valid_from` / `valid_to` | commit time of the opening / closing change (event time from the WAL, not the processing time) |
| `valid_to_seq` | `change_seq` of the closing change (an UPDATE or the DELETE) |
| `is_current` | `valid_to` is null; **the current state is `WHERE is_current`** |

There are no separate "SCD tables": a version table with `valid_from`, `valid_to` and `is_current` *is* SCD
Type 2. The incremental file load ([`incremental_load.md`](incremental_load.md)) fills a table with exactly the
same columns: both use `notebooks/utils/scd2_helpers.py`, only the way the changes arrive differs. The column is
called `change_seq`, not `lsn_num`, because for the file it is a record number, not a WAL position.

Steps of one run:
1. Read the Bronze partitions from the newest date already in the table up to `ingestion_date`.
2. **Idempotency:** drop the events up to the watermark (highest applied LSN: the newest change that opened or
   closed a version) and repeated events. The snapshot is applied only on the first run.
3. Clean every row image with the **same** `clean_vehicle_listings` and `add_manufacturer` as the file
   pipeline. A DELETE uses its before image.
4. Per listing, in WAL order:
   - an R / I / U opens a version when the cleaned business values changed;
   - a change that repeats the current values is skipped;
   - a D closes the open version without opening a new one, so the sale time stays visible.
5. MERGE on `version_key`: new versions are inserted, and the versions they close get `valid_to`.
6. Check the whole table:
   - unique keys;
   - at most one current version per listing;
   - `valid_to` empty exactly when the version is current;
   - no overlapping periods.

## Gold: current state only (`11_gold_cdc_vehicle_history`)

**Mentor's decision (2026-11-25):**
- Silver keeps the history (`valid_from`, `valid_to`, `is_current`);
- Gold holds **only the current state**.

`gold/cdc/dim_vehicle` = the current Silver versions (`WHERE is_current`), one row per vehicle that exists in the
source now:
- the same business-ready attributes as the main `gold.dim_vehicle` (`vehicle_attributes()` in `gold_helpers`);
- the same key `vehicle_key = xxhash64(vehicle_id)` and the Unknown member −1;
- no `has_service_history`, because the demo has no service book.

It is rebuilt on every run (SCD Type 1 in Gold), and a sold listing simply disappears from it.
Diagram: [`scd2_gold_schema.dbml`](scd2_gold_schema.dbml).

**What this means for analysts:** questions over time go to the Silver history, e.g. the price of a listing on a
given day, how often prices dropped, or when a listing disappeared. Gold answers "how is it now".

*Earlier version (2026-11-24/25), replaced by the decision above:* Gold had an SCD2 `dim_vehicle` with version keys
and a `fact_listing_event` with every change. It was removed to keep Gold simple and the same as the main star
schema.

## How to run

```bash
.venv/bin/pip install -r cdc/requirements.txt
# .env (gitignored) needs CDC_PG_USER and CDC_PG_PASSWORD for the local database
.venv/bin/python -m cdc.run_demo --days 3 --crash-test     # starts Docker if needed; writes to cdc_data/
```

For a presentation, `.venv/bin/python -m cdc.presentation` runs the same steps and prints a formatted report:
- every step and day;
- the checks and the crash test;
- a summary table;
- three real listings (price drop, sold, new) with their Silver history, Gold row and the SQL to look them up in
  the database.

The technical output goes to `cdc_data/presentation.log`.

The single steps also run on their own:
- `python -m cdc.load_initial`
- `python -m cdc.cdc_reader --snapshot`
- `python -m cdc.simulate_changes --day 1`
- `python -m cdc.cdc_reader [--crash-before-ack]`

The notebooks run unchanged on Databricks as well (`dev_mode`, `ingestion_date` widgets), once the change files
are in Bronze.

**Tests:**
- `tests/test_cdc_simulator.py`: WAL message parsing, TOAST fill, the simulator, the data split.
- `tests/test_cdc_pipeline.py`: hand-written change files. It covers every SCD2 rule, re-delivery, a partly
  applied file, a change at the snapshot LSN, two changes at the same commit time, the current-only Gold dimension and
  the diagram.
- The end-to-end test against PostgreSQL is opt-in, because it resets the demo database:
  `CDC_INTEGRATION=1 .venv/bin/python -m pytest -m cdc_integration`.

## Verified run (2026-11-24, `run_demo --days 3 --crash-test`)

After every day, `cdc/verify.py` checked:
- the **current Silver state equals the PostgreSQL table** row by row, after the same cleaning;
- the SCD2 rules hold;
- Gold holds exactly the current Silver versions;
- every listing the simulator deleted has no current version in Silver.

| Day | Changes read (I / U / D) | Listings in the source | Silver versions | Changes vs. full load |
|---|---|---|---|---|
| 0 | snapshot 16,000 | 16,000 | 16,000 | – |
| 1 | 1,127 (320 / 647 / 160) | 16,160 | 16,967 | 7.0 % |
| 2 | 1,147 (323 / 662 / 162) | 16,321 | 17,951 | 7.0 % |
| 3 | 1,160 (326 / 671 / 163) | 16,484 | 18,948 | 7.0 % |

- **Crash test on day 3:** the reader wrote the changes without confirming them, and the next read delivered the
  same 1,160 changes again. Silver applied nothing and the table stayed unchanged.
- A read with no new changes found 0 changes.
- WAL still held by the slot at the end: 34 KB.

- In the simulation, every "day" takes seconds; the logic is the same as with real days.
- After the switch to the current-only Gold (2026-11-25), the end-to-end test against PostgreSQL (2 days + crash
  test) passed again.

## In production

- **Capture:** Debezium does what `cdc_reader.py` does here, with the same slot, LSN, before/after images and
  snapshot. It publishes the events to Kafka, or on Azure to **Event Hubs** (Kafka-compatible). Databricks reads
  them with Structured Streaming (or Auto Loader from the landed files) and applies the same MERGE. Azure
  Database for PostgreSQL supports logical replication. Delta Lake Change Data Feed / Delta Live Tables
  `APPLY CHANGES … STORED AS SCD TYPE 2` give the same SCD2 result as a managed feature.
- **Operations:**
  - monitor the slot lag, so a stopped reader cannot fill the source disk;
  - use a schema registry or schema-change handling for new source columns;
  - partition the change files by date, and compact (OPTIMIZE) the Silver history.
- **Same-transaction changes:** changes inside one transaction share a commit time. The demo commits every
  change on its own. With shared times, versions of zero length would appear, and the order would come from the
  LSN.
