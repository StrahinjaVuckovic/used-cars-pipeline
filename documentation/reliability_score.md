# Reliability Score (0–100)

> *"Explore the service book, identify important columns and based on that make an algorithm that scores cars
> based on their reliability. Highest scores = only regular maintenance; lowest scores = frequent non-maintenance
> issues."* – project brief, Gold layer

The score is stored per vehicle in `gold.fact_vehicle_reliability` (notebook
`notebooks/09_gold_fact_vehicle_reliability.py`, weights `RELIABILITY_WEIGHTS` in `notebooks/utils/gold_helpers.py`).
The exploration is reproducible with `notebooks/profiling/04_profile_reliability_inputs.py`.

## 1. What the service book contains

Profiled on the full data: 8,312,490 events of 419,641 vehicles.

| `service_type` | Events | What it is (`notes`, flags) | Cost / downtime | Role in the score |
|---|---|---|---|---|
| OIL_CHANGE | 7,667,397 | regular oil change, target every 15,000 km | none | maintenance; lateness = `oil_gap_penalty` |
| BIG_SERVICE | 498,285 | big service; 148,896 noted **"Skipped"** | none | maintenance; a skipped one = poor discipline |
| ENGINE_WORK | 26,651 | every row "Major engine inspection", INSPECTION, LOW, `is_maintenance` = 1, `is_repair` = 0 | none | **neutral** (see below) |
| BODY_WORK | 20,309 | every row "Minor exterior repair", not maintenance, MEDIUM | none | small issue |
| REPAIR | 99,848 | failures: transmission, engine overheat / head gasket, engine failure | **only here**: median 1,626 EUR, 6.6 days | main issue |

Facts per vehicle that shaped the algorithm:

- **A failure is recorded as two rows**: a diagnosis ("transmission failure; gearbox slipping") and a fix
  ("transmission replaced; …"), with equal counts per kind. Vehicles have exactly 2 or 4 repair rows, so an
  incident is `ceil(repair rows / 2)`. 33,285 vehicles (7.9%) had a failure: 16,646 one incident, 16,639 two.
  Downtime per incident: median 14.6 days.
- **Skipped big services:** 27.5% of the vehicles have at least one.
- **Late oil changes:** the source already computes `oil_gap_penalty` per oil change (> 0 when the gap exceeds the
  15,000 km target). 40% of the vehicles have some; per vehicle p75 = 4, p95 = 18, p99 = 46. The maximum, 84,048,
  comes from impossible odometer jumps.
- **The components are independent:** the correlation between repairs, body work, skipped services and late oil
  changes is about 0, so each adds its own information.
- **Histories are short and similar:** median 0.97 years and 17 events per vehicle, so normalizing per year or per
  km would not separate vehicles and is not done.
- **8,239 vehicles** of the listings have no service event at all.

### Is ENGINE_WORK an engine repair?

No. All 26,651 rows are identical: "Major engine inspection", `is_inspection` = 1, `is_maintenance` = 1,
`is_repair` = 0, severity LOW, no cost, no downtime. Engine repairs are recorded separately as REPAIR rows
("engine failure; engine replace recommended" → "engine replaced; timing belt and coolant renewed", "engine
overheat; head gasket suspected" → "head gasket replaced"; 22,097 vehicles). Vehicles with an engine inspection
fail as often as the others (7.79% vs 7.93%): of the 22,097 vehicles with an engine repair, 1,380 also had an
inspection, where chance alone gives about 1,410. The inspection happens at random times relative to failures
(−260 to +300 days). Penalizing it would penalize owners who have their engine checked; the weight is still a
constant (`engine_inspection`, default 0).

## 2. The algorithm

The score starts at 100 and loses points for explainable reasons:

```
reliability_score = clamp(0, 100,
      100
    − repair_penalty           = 25 × repair_incidents + 0.5 × repair_downtime_days
    − minor_repair_penalty     = 5 × BODY_WORK events + 0 × ENGINE_WORK events
    − skipped_service_penalty  = min(15, 5 × skipped big services)
    − oil_change_penalty       = min(10, 0.5 × oil_gap_penalty)          rows with a valid odometer only
)
```

Rounded to one decimal. Every component and penalty is a column of the table, so each score can be explained.

| Weight | Value | Why |
|---|---|---|
| Repair incident | 25 | The brief puts "frequent non-maintenance issues" at the bottom: one failure takes a vehicle out of the top categories, two put it in the lowest. |
| Repair downtime | 0.5 per day | Separates heavy failures (engine replaced, ~8.5 days per row) from light ones (head gasket, ~3.5). Downtime is chosen over cost because cost in EUR follows the kind of failure and adds nothing, while downtime is what hurts an owner (vehicle out of use). |
| Minor repair (BODY_WORK) | 5 | Not maintenance, but not a mechanical failure either. |
| Engine inspection | 0 | An inspection, not a failure (evidence above). |
| Skipped big service | 5, max 15 | Poor maintenance discipline lowers the score, but never as much as a failure. |
| Late oil change | 0.5 per point, max 10 | Uses the source's own lateness measure; the cap keeps impossible odometer jumps from destroying a score; rows with a non-monotonic odometer are ignored. |

The maintenance penalties are capped (15 + 10), so poor maintenance alone costs at most 25 points plus 5 per body
repair, while one failure costs 25 plus its downtime (at least ~27.5 on the data). The groups therefore barely
overlap: on the full data vehicles without a failure score 70–100, with one failure 34–72.4 (only the best
one-failure vehicles, with short downtime and perfect maintenance, reach the worst no-failure vehicles), and with
two failures 0–44.

**Categories:** Excellent ≥ 90, Good ≥ 75, Fair ≥ 50, Poor < 50. **Confidence** by the number of service events:
High ≥ 15, Medium ≥ 5, Low below. **Vehicles without service history** get no score (null), category and
confidence "Unknown" – no evidence is not the same as perfect reliability.

### Worked examples

| Vehicle | Service history | Penalties | Score |
|---|---|---|---|
| A | 20 oil changes on time, 2 big services | 0 | **100** (Excellent) |
| B | regular oil changes, one big service skipped, oil penalty points 8 | 5 + 4 | **91** (Excellent) |
| C | one transmission failure (diagnosis 7.0 + fix 8.0 days) | 25 + 7.5 | **67.5** (Fair) |
| D | engine failure and head gasket (2 incidents, 30 days), one body repair | 50 + 15 + 5 | **30** (Poor) |
| E | no service events | – | **null** (Unknown) |

## 3. Expected distribution

Simulated on the full local copy of the service book before implementation:

| Repair incidents | Vehicles | Score min / avg / max |
|---|---|---|
| 0 | 386,356 | 70 / 96.5 / 100 |
| 1 | 16,646 | 34 / 64.7 / 72 |
| 2 | 16,639 | 0.3 / 32.9 / 44 |

Categories: Excellent 349,594, Good 36,642, Fair 16,418, Poor 16,987, Unknown 8,239 (no history).

### Verified on the cluster (2026-11-16, full ADF master run, Job `gold` with the new task)

427,880 rows; repair cost total 178,572,463.59 EUR = Silver; no vehicle with an odd number of repair rows and no
service-book vehicle without a listing.

| Repair incidents | Vehicles | Score min / avg / max |
|---|---|---|
| 0 | 386,356 | 70 / 96.5 / 100 |
| 1 | 16,646 | 34 / 64.7 / 72.5 |
| 2 | 16,639 | 0.3 / 32.9 / 44.3 |

Categories: Excellent 349,594, Good 36,642, Fair 16,420, Poor 16,985, Unknown 8,239 – the simulation within a few
vehicles (Silver now keeps fractional downtime). The five lowest scores (0.3–4) are vehicles with two failures
(34–42 days of downtime), 3–4 skipped services and the maximum oil penalty. The average score per manufacturer is
almost flat (91.6–93.9 for the 15 largest), so in this data the brand barely affects reliability.

## 4. Limitations and how to extend

- The weights are a reasoned judgement, not learned from outcomes (there is no "breakdown after sale" data to
  fit them to). They are constants in one place and can be tuned with the business.
- Histories are about one year long; with multi-year histories the repair and lateness components should be
  divided by the years (or 10,000 km) of history.
- The ENGINE_WORK decision depends on the source labelling it an inspection; if inspections started to come with
  findings, they would get a weight.
- Silver change made for the score: `downtime_days` is kept as a double (it was an int, which cut the fraction
  of 90% of the values).
