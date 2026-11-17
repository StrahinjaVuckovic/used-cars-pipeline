# Data quality

Data quality is checked on two levels.

| Level | Where | What it does |
|---|---|---|
| **Per value** | every Silver notebook (`dq_issues`, `is_valid`) | An impossible value (price 3.7 billion, year 2100, latitude 200) becomes null and its issue code is added to `dq_issues`; the row is kept. |
| **Before a write** | `assert`s in every Silver and Gold notebook | Stops a write that would break the table: wrong columns, duplicate keys, Silver rows ≠ Bronze rows, invalid exchange rates, missing foreign keys in Gold. |
| **Whole Silver layer, with thresholds and history** | `notebooks/06_silver_dq_checks.py` (Job `silver_dq`) | Checks the Silver tables as a whole against thresholds every day, appends every result to `silver/dq_results`, and **gates Gold**: when an error rule fails, the Gold Job does not start. |

This document describes the third level, added on the mentor's request (2026-11-16).

## How it works

- **Engine:** `notebooks/utils/dq_checks.py`, a small rule engine. A rule checks one number about a whole table (a
  share of flagged rows, a count of duplicate keys, the age of the latest exchange rate) against a threshold with
  `<=`, `>=` or `==`.
- **Profiles:** each big table is read once (one aggregation per table), and all its rules read their number from
  that profile, so the 8.3M service events are scanned once, not once per rule.
- **Severity:**
  - `error`: Gold would be wrong. The run fails **after** all results are stored, with a message that lists
    every failed error rule. The ADF master then does not start the Gold Job, so Gold keeps its last correct
    version.
  - `warning`: the source behaves differently than usual. Logged (`WARNING` line in the task output) and stored;
    the run goes on.
- **Thresholds** of the warnings are about twice the values verified on the full data, so normal day-to-day
  variation passes and a broken source is noticed.
- A ready-made framework (Great Expectations, Soda, Delta Live Tables expectations) was considered and not used:
  it needs extra libraries or another way to run the pipeline, and for a dozen tables a list of rules in a notebook
  does the same job and is easier to explain and test.

## Rules

### Error rules (Gold would be wrong)

| Table | Rule | Passes when | Why |
|---|---|---|---|
| vehicle_listings | `row_count_matches_stages` | rows = sum of the 4 stage tables | the union lost or duplicated rows |
| vehicle_listings | `vehicle_id_duplicates`, `vehicle_id_nulls` | 0 / 0 | `vehicle_id` is the key of `dim_vehicle` |
| vehicle_listings | `ingestion_dates_not_expected` | one ingestion date, the latest Bronze partition (or the requested date) | the table must be one consistent snapshot of today |
| service_events | `service_event_id_duplicates` | 0 | key of `fact_service_event` |
| service_events | `vehicles_in_listings_share` | ≥ 99% (verified: 100%) | service events need their vehicle in `dim_vehicle`; **warning in dev mode**, where the two samples do not match |
| brand_lookup | `model_normalized_duplicates` | 0 | the brand join would multiply listings |
| exchange_rates | `latest_rate_age_days` | ≤ 5 days before the listings' ingestion date | prices would use stale rates (5 days cover a long weekend) |
| exchange_rates | `latest_day_currencies` | 3 (USD, GBP, AUD) | a missing currency gives missing prices |
| vw_vehicle_listings_priced | `missing_converted_prices` | 0 | every listing with price and currency must be converted |

### Warning rules (the source behaves differently than usual)

| Table | Rule | Verified value (full data) | Threshold |
|---|---|---|---|
| vehicle_listings | `invalid_manufacture_year_share` | 2.51% | ≤ 5% |
| vehicle_listings | `invalid_latitude_share`, `invalid_longitude_share` | 2.33% | ≤ 5% |
| vehicle_listings | `invalid_vin_share` / `zero_odometer_share` | 0.37% / 0.46% | ≤ 2% |
| vehicle_listings | `invalid_odometer_share` / `invalid_price_share` / `invalid_currency_share` | 0.14% / 0.01% / 0% | ≤ 1% |
| vehicle_listings | `unparsed_posting_date_share` | 0% | = 0 (a new date format appeared) |
| vehicle_listings | `unknown_manufacturer_share` | 2.84% | ≤ 5% |
| vehicle_listings | `valid_rows_share` | ~94.3% | ≥ 90% |
| service_events | `invalid_service_date_share` | 2.75% | ≤ 5% |
| service_events | `invalid_odometer_km_share` | 3.38% | ≤ 7% |
| service_events | `non_monotonic_odometer_share` | 16.2% | ≤ 25% |
| service_events | `unparsed_service_date_share` | 0% | = 0 |
| service_events | `repairs_with_cost_and_downtime_share` | 100% | ≥ 99% (needed by the Reliability Score) |
| exchange_rates | `largest_rate_gap_days` | ≤ 3 (weekends) | ≤ 5 calendar days in the last 30 days |
| all four tables | `row_count` | – | ≥ 1 |
| all four tables | `row_count_change_pct` | ~0% | ≤ 20% against the previous run (from `dq_results`) |

## Results: `silver.dq_results`

Delta table `silver/dq_results` (`dq_results_dev` in dev mode), **appended** on every run, and the Unity Catalog
view `dbw_used_cars_strahinja.silver.dq_results`.

| Column | Meaning |
|---|---|
| `run_id`, `run_timestamp`, `ingestion_date` | one run of the checks |
| `table_name`, `rule_name`, `severity`, `description` | the rule |
| `metric_value`, `operator`, `threshold` | what was measured and against what |
| `status`, `message` | PASS / FAIL and a readable sentence, e.g. `vehicle_listings.invalid_manufacture_year_share = 6.10%, expected <= 5.00%: …` |

```sql
-- Latest run, failed rules first
SELECT severity, status, table_name, rule_name, message
FROM dbw_used_cars_strahinja.silver.dq_results
WHERE run_id = (SELECT max_by(run_id, run_timestamp) FROM dbw_used_cars_strahinja.silver.dq_results)
ORDER BY status DESC, severity;
```

The table is also the source for the planned daily e-mail / Teams notification (what failed and why).

## Gating Gold

```
silver_service_events ─┐
                        ├─(Succeeded)─► silver_dq ─(Succeeded)─► gold
silver_exchange_rates ─┘
```

When an error rule fails, `silver_dq` fails, the Gold Job does not start and the master run is Failed. The
Silver tables have already been rewritten at that point; what is protected is Gold, the layer the analysts use,
which keeps its previous correct version. The `assert`s inside the Silver notebooks still stop the worst cases
before a Silver write.

## Verified on the cluster (2026-11-16, Job `silver_dq`, ID 363052312939361, full data)

Profiles: vehicle_listings 427,880, service_events 8,312,490, brand_lookup 23,756, exchange_rates 5,286 rows.
**35 rules: 35 passed, 0 warnings, 0 errors** ("Silver is ready for Gold"). The measured values match the values
the thresholds were derived from: invalid manufacture year 2.51%, invalid latitude/longitude 2.33%, unknown
manufacturer 2.84%, valid rows 94.32%, service-book vehicles in the listings 100%, latest rate 3 days old, largest
rate gap 3 days (a weekend). The negative case (an error rule fails → results stored, run fails with the list of
failed rules) is covered by `tests/test_dq_checks.py`; the run on the cluster with `threshold_overrides` and the ADF
wiring are pending (see below).

**Pending:** the Databricks account ran out of credits right after this run ("exhausted your available credits"),
so the cluster cannot start. The `silver_dq` activity is already in `pl_daily_master` (published 2026-11-17, gold
depends on it), but the master has not run with it yet; that run and the negative run on the cluster wait until the
credits are restored.

## Alerting

The Job `silver_dq` has a Databricks **e-mail notification on failure** (`email_notifications.on_failure`, set
2026-11-17; the address is configured in the Databricks UI and not kept in git). An error rule failing makes the
notebook fail, so the e-mail arrives exactly when Silver is not good enough for Gold. The e-mail names the Job and
links to the run; the run's error message lists every failed rule with the measured value and the threshold, e.g.

```
1 data quality error rule(s) failed:
- service_events.vehicles_in_listings_share = 97.20%, expected >= 99.00%: service-book vehicles must exist in the listings (dim_vehicle)
```

Warnings do not fail the run and send no e-mail; they are in `silver.dq_results`. Not tested yet (no Databricks
credits): the test is the negative run with `threshold_overrides`. A daily summary or a Teams message with the
failed rules in the message itself would read `silver.dq_results` (e.g. from a Logic App) – a possible next step.

## Adding or changing a rule

Add one `dq_rule(table, rule, severity, description, metric, operator, threshold, unit)` to the `RULES` list in
`06_silver_dq_checks.py` (compute the number in the profile cell when it needs a new aggregation), add a line to
the tables above, and a test in `tests/test_dq_checks.py`. Thresholds can be changed for one run with the Job
parameter `threshold_overrides`, e.g. `{"vehicles_in_listings_share": 100.1}` to see the gate fail on purpose.
