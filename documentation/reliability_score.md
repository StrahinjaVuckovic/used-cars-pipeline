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
    − repair_penalty           = Σ penalty(kind) × incidents of that kind + 0.5 × repair_downtime_days
                                 kind: engine failure 35, transmission 30, head gasket 20, other 25
    − minor_repair_penalty     = 5 × BODY_WORK events + 0 × ENGINE_WORK events
    − skipped_service_penalty  = min(15, 5 × skipped big services)
    − oil_change_penalty       = min(10, 0.5 × oil_gap_penalty)          rows with a valid odometer only
)
```

Rounded to one decimal. Every component and penalty is a column of the table, so each score can be explained.

### Kind of failure

`severity` is HIGH for every repair, so it cannot tell failures apart, but the **notes** of each repair row name the
failure. Both rows of an incident (diagnosis and fix) name the same kind, and a vehicle with two failures had the
same failure twice (6 combinations, about 5,500 vehicles each).

| Kind | Notes (diagnosis → fix) | Repair rows | Cost / incident | Downtime / incident | Penalty |
|---|---|---|---|---|---|
| Engine failure | "engine failure; engine replace recommended" → "engine replaced; timing belt and coolant renewed" | 32,878 | 4,747 EUR | 17.0 days | **35** |
| Transmission | "transmission failure; gearbox slipping" → "transmission replaced; atf filled and adaptation performed" | 33,634 | 3,997 EUR | 15.5 days | **30** |
| Head gasket | "engine overheat; head gasket suspected; vehicle towed" → "head gasket replaced; coolant system flushed" | 33,336 | 1,999 EUR | 8.5 days | **20** |
| Other (unknown notes) | – | 0 | – | – | **25** + a warning in the log |

The kind is recognized by `failure_type` in `notebooks/utils/gold_helpers.py` with the patterns `transmission`,
`head gasket|overheat` and `engine fail|engine replace`, checked in this order ("engine overheat" is a head gasket
failure, not an engine failure). Only repair rows are matched against the text (~1% of the events). The kind is
also the column `failure_type` of `gold.dim_service_type`, so analysts can count, for example, transmission failures
per manufacturer.

Before the kind was used (version 1, verified 2026-11-16), every failure cost 25 and only the downtime separated
them: an engine failure took about 4 points more than a head gasket. Now the order follows the data (cost and
downtime), and the steps are rounded so they can be read and explained; the average penalty (~28) stays close to 25.

### Weights

| Weight | Value | Why |
|---|---|---|
| Failure | 35 / 30 / 20 (other 25) | The brief puts "frequent non-maintenance issues" at the bottom. One failure takes a vehicle out of the top categories, two put it in the lowest; a heavier failure takes more. |
| Repair downtime | 0.5 per day | Within a kind, a longer stay in the workshop means a worse failure; downtime is what hurts an owner (vehicle out of use). |
| Minor repair (BODY_WORK) | 5 | Not maintenance, but not a mechanical failure either. |
| Engine inspection | 0 | An inspection, not a failure (evidence above). |
| Skipped big service | 5, max 15 | Poor maintenance discipline lowers the score, but never as much as a heavy failure. |
| Late oil change | 0.5 per point, max 10 | Uses the source's own lateness measure; the cap keeps impossible odometer jumps from destroying a score; rows with a non-monotonic odometer are ignored. |

The maintenance penalties are capped (15 + 10), so poor maintenance alone costs at most 25 points plus 5 per body
repair. One head gasket failure (20 + ~4 for downtime) costs about as much, so **a vehicle with one repaired head
gasket and good maintenance (~77, Good) can rank above a vehicle without failures but with very poor maintenance
(70)**. This is intended: a repaired head gasket is a smaller problem than neglected maintenance at its worst.
Engine and transmission failures always rank below every vehicle without failures.

**Categories:** Excellent ≥ 90, Good ≥ 75, Fair ≥ 50, Poor < 50. **Confidence** by the number of service events:
High ≥ 15, Medium ≥ 5, Low below. **Vehicles without service history** get no score (null), category and
confidence "Unknown" – no evidence is not the same as perfect reliability.

### Worked examples

| Vehicle | Service history | Penalties | Score |
|---|---|---|---|
| A | 20 oil changes on time, 2 big services | 0 | **100** (Excellent) |
| B | regular oil changes, one big service skipped, oil penalty points 8 | 5 + 4 | **91** (Excellent) |
| C | one head gasket failure (diagnosis 5.0 + fix 3.5 days) | 20 + 4.25 | **75.8** (Good) |
| D | one transmission failure (diagnosis 7.0 + fix 8.5 days) | 30 + 7.75 | **62.3** (Fair) |
| E | engine failed twice (34 days in total), one body repair | 70 + 17 + 5 | **8** (Poor) |
| F | no service events | – | **null** (Unknown) |

## 3. Distribution

Simulated on the full local copy of the service book (version 2, with the kind of failure):

| Repair incidents | Vehicles | Score min / avg / max |
|---|---|---|
| 0 | 386,356 | 70 / 96.5 / 100 |
| 1 | 16,646 | 24 / 61.3 / 77.4 |
| 2 | 16,639 | 0 / 26.4 / 54.3 |

One failure, by kind: engine failure 24–60 (average 52.9), transmission 32–65 (58.8), head gasket 45–77 (72.3).
Categories: Excellent 349,594, Good 39,141, Fair 15,506, Poor 15,400, Unknown 8,239 (no history).

### Version 1 verified on the cluster (2026-11-16, flat 25 per failure)

427,880 rows; repair cost total 178,572,463.59 EUR = Silver; no vehicle with an odd number of repair rows and no
service-book vehicle without a listing. By incidents: 0 → 70 / 96.5 / 100, 1 → 34 / 64.7 / 72.5,
2 → 0.3 / 32.9 / 44.3. Categories: Excellent 349,594, Good 36,642, Fair 16,420, Poor 16,985, Unknown 8,239 (the
simulation within a few vehicles; Silver now keeps fractional downtime). The average score per manufacturer is
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
