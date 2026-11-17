"""
Tests for the Reliability Score (notebooks/09_gold_fact_vehicle_reliability.py) on hand-made service events.

The score is 100 minus explainable penalties (documentation/reliability_score.md):
25 per repair incident + 0.5 per repair downtime day, 5 per BODY_WORK, 5 per skipped big service (max 15),
0.5 per oil-gap penalty point (max 10, valid odometer only); ENGINE_WORK (an inspection) is neutral.
Version 3 adds wear: 0.2 per year of age at posting + 0.05 per 1,000 km, at most 10, times 1.0-1.5 for neglect.
"""

import datetime

from decimal import Decimal

import pytest

from notebook_utils import run_notebook_cells_until
from test_gold import gold_namespace

NOTEBOOK = "09_gold_fact_vehicle_reliability.py"
EVENT_SCHEMA = (
    "vehicle_id STRING, service_type STRING, is_repair BOOLEAN, cost_eur DECIMAL(10,2), downtime_days DOUBLE, "
    "is_skipped_service BOOLEAN, oil_gap_penalty DOUBLE, is_odometer_monotonic BOOLEAN, notes STRING, odometer_km INT"
)
# The real notes of the three kinds of failure: (diagnosis, fix).
ENGINE_FAILURE = ("engine failure; engine replace recommended", "engine replaced; timing belt and coolant renewed")
TRANSMISSION = ("transmission failure; gearbox slipping", "transmission replaced; atf filled and adaptation performed")
HEAD_GASKET = ("engine overheat; head gasket suspected; vehicle towed", "head gasket replaced; coolant system flushed")


@pytest.fixture(scope="module")
def score(spark, tmp_path_factory):
    """Run the notebook's function cells; returns a function {vehicle_id: [events]} -> {vehicle_id: row}."""
    tmp = tmp_path_factory.mktemp("reliability")
    namespace = run_notebook_cells_until(NOTEBOOK, gold_namespace(spark, tmp, tmp, tmp), "service_events_df = read_required_table")

    def run(vehicles: dict, listings: dict = None) -> dict:
        """`listings`: {vehicle_id: (manufacture_year, posting_date)} for the age; omitted = no listing data."""
        rows = [(vid, *event) for vid, events in vehicles.items() for event in events]
        events_df = spark.createDataFrame(rows, EVENT_SCHEMA)
        key = namespace["surrogate_key"]
        dim = spark.createDataFrame([(vid,) for vid in vehicles], "vehicle_id STRING").select(
            key("vehicle_id").alias("vehicle_key"), "vehicle_id")
        dim = namespace["with_unknown_member"](dim, "vehicle_key")
        listings_df = None
        if listings is not None:
            listings_df = spark.createDataFrame(
                [(vid, year, posted) for vid, (year, posted) in listings.items()],
                "vehicle_id STRING, manufacture_year INT, posting_date DATE",
            )
        result = namespace["score_vehicles"](namespace["vehicle_service_summary"](events_df), dim, listings_df)
        return {r.vehicle_id: r for r in result.join(dim, "vehicle_key").collect()}

    return run


def oil(penalty=0.0, monotonic=True, km=None):
    return ("OIL_CHANGE", False, None, None, False, penalty, monotonic, None, km)


def big_service(skipped=False):
    return ("BIG_SERVICE", False, None, None, skipped, 0.0, True, "Skipped" if skipped else None, None)


def repair(cost="1500.00", downtime=7.5, notes=None):
    """A repair row; without notes its kind is "other" (penalty 25 per incident)."""
    return ("REPAIR", True, Decimal(cost), downtime, False, 0.0, True, notes, None)


def failure(kind, downtime=0.0):
    """One incident of a known kind: its diagnosis row and its fix row."""
    return [repair(downtime=downtime, notes=kind[0]), repair(downtime=downtime, notes=kind[1])]


BODY_WORK = ("BODY_WORK", False, None, None, False, 0.0, True, "Minor exterior repair", None)
ENGINE_WORK = ("ENGINE_WORK", False, None, None, False, 0.0, True, "Major engine inspection", None)


def test_only_regular_maintenance_scores_100(score):
    row = score({"clean": [oil()] * 20 + [big_service()] * 2})["clean"]
    assert (row.reliability_score, row.reliability_category, row.score_confidence) == (100.0, "Excellent", "High")
    assert (row.service_events, row.maintenance_events, row.repair_incidents) == (22, 22, 0)
    assert (row.repair_penalty, row.minor_repair_penalty, row.skipped_service_penalty, row.oil_change_penalty) == (0, 0, 0, 0)


def test_one_repair_incident_is_two_rows(score):
    # Diagnosis (7.5 days) + fix (7.5 days): 1 incident, 15 days -> 100 - 25 - 7.5 = 67.5.
    row = score({"one": [oil()] * 10 + [repair("1000.00", 7.5), repair("2000.00", 7.5)]})["one"]
    assert (row.repair_incidents, row.repair_downtime_days, row.repair_cost_eur) == (1, 15.0, Decimal("3000.00"))
    assert (row.repair_penalty, row.reliability_score, row.reliability_category) == (32.5, 67.5, "Fair")


def test_two_incidents_are_poor(score):
    row = score({"two": [oil()] * 10 + [repair(downtime=7.0)] * 4})["two"]
    assert row.repair_incidents == 2
    assert row.reliability_score == 100 - 50 - 14  # 36.0
    assert row.reliability_category == "Poor"


def test_maintenance_penalties_are_capped_below_a_failure(score):
    rows = score({
        "worst_maintenance": [oil(penalty=100.0)] * 3 + [big_service(skipped=True)] * 5 + [BODY_WORK],
        "best_repair": [oil()] * 10 + [repair(downtime=0.0)] * 2,
    })
    assert rows["worst_maintenance"].reliability_score == 100 - 15 - 10 - 5  # 70.0, the floor without repairs
    assert rows["best_repair"].reliability_score == 75.0
    # All maintenance penalties together (capped, 30 with one body repair) take about as much as one failure.
    assert 100 - rows["worst_maintenance"].reliability_score <= 25 + 5


def test_minor_repair_costs_five_points_and_engine_inspection_nothing(score):
    rows = score({"body": [oil()] * 5 + [BODY_WORK], "engine": [oil()] * 5 + [ENGINE_WORK]})
    assert (rows["body"].minor_repairs, rows["body"].reliability_score) == (1, 95.0)
    assert rows["engine"].reliability_score == 100.0
    assert rows["engine"].maintenance_events == 6  # an engine inspection is maintenance


def test_skipped_services_are_capped(score):
    rows = score({"one": [big_service(skipped=True)] + [oil()] * 5, "many": [big_service(skipped=True)] * 7 + [oil()] * 5})
    assert (rows["one"].skipped_service_penalty, rows["one"].reliability_score) == (5.0, 95.0)
    assert (rows["many"].skipped_services, rows["many"].skipped_service_penalty) == (7, 15.0)
    assert rows["many"].maintenance_events == 5  # a skipped service was not performed


def test_oil_gap_penalty_is_capped_and_ignores_invalid_odometers(score):
    rows = score({
        "late": [oil(penalty=4.0)] * 2 + [oil()] * 3,                        # 8 points -> 4
        "very_late": [oil(penalty=46.0)] + [oil()] * 4,                       # 23 -> capped 10
        "bad_odometer": [oil(penalty=84000.0, monotonic=False)] + [oil()] * 4,  # not trusted -> 0
    })
    assert (rows["late"].oil_gap_penalty_total, rows["late"].oil_change_penalty, rows["late"].reliability_score) == (8.0, 4.0, 96.0)
    assert (rows["very_late"].oil_change_penalty, rows["very_late"].reliability_score) == (10.0, 90.0)
    assert (rows["bad_odometer"].oil_gap_penalty_total, rows["bad_odometer"].reliability_score) == (0.0, 100.0)


def test_score_never_goes_below_zero(score):
    row = score({"wreck": [repair(downtime=30.0)] * 8 + [BODY_WORK] * 3 + [big_service(skipped=True)] * 4})["wreck"]
    assert (row.reliability_score, row.reliability_category) == (0.0, "Poor")


def test_vehicle_without_service_history_has_no_score(score, spark):
    rows = score({"serviced": [oil()] * 3, "no_history": []})
    row = rows["no_history"]
    assert row.reliability_score is None
    assert (row.reliability_category, row.score_confidence) == ("Unknown", "Unknown")
    assert (row.service_events, row.repair_incidents, row.repair_penalty) == (0, 0, None)
    assert rows["serviced"].score_confidence == "Low"


@pytest.mark.parametrize(
    "events, expected_score, category",
    [
        ([oil()] * 5 + [BODY_WORK] * 2, 90.0, "Excellent"),                          # 90 is Excellent
        ([oil()] * 5 + [big_service(skipped=True)] * 3 + [BODY_WORK] * 2, 75.0, "Good"),  # 75 is Good
        ([oil()] * 5 + [repair(downtime=25.0)] * 2, 50.0, "Fair"),                      # 1 incident: 100 - 25 - 0.5 x 50 = 50 is Fair
    ],
)
def test_category_bounds(score, events, expected_score, category):
    row = score({"v": events})["v"]
    assert (row.reliability_score, row.reliability_category) == (expected_score, category)


@pytest.mark.parametrize("events, confidence", [(4, "Low"), (5, "Medium"), (14, "Medium"), (15, "High")])
def test_confidence_grows_with_the_number_of_events(score, events, confidence):
    assert score({"v": [oil()] * events})["v"].score_confidence == confidence


def test_profiling_notebook_runs_on_the_sample(spark, bronze_sample, tmp_path):
    """notebooks/profiling/04_profile_reliability_inputs.py reads silver.service_events and writes nothing."""
    from notebook_utils import load_config_and_utils, run_notebook
    from test_service_events import run_service_events_notebook

    silver = tmp_path / "silver"
    run_service_events_notebook(spark, bronze_sample, silver)
    namespace = load_config_and_utils(spark, bronze_path=str(bronze_sample), silver_path=str(silver),
                                      widget_values={"dev_mode": "false"})
    run_notebook("profiling/04_profile_reliability_inputs.py", namespace)
    assert len(namespace["display"].results) == 7  # every report cell ran (no listings here: the age table is skipped)
    assert sorted(p.name for p in silver.iterdir()) == ["service_events"]


# ---------- kind of failure ----------


@pytest.mark.parametrize(
    "kind, column, penalty",
    [(ENGINE_FAILURE, "engine_failure_incidents", 35.0), (TRANSMISSION, "transmission_incidents", 30.0),
     (HEAD_GASKET, "head_gasket_incidents", 20.0)],
)
def test_penalty_depends_on_the_kind_of_failure(score, kind, column, penalty):
    row = score({"v": [oil()] * 10 + failure(kind, downtime=5.0)})["v"]
    assert (row.repair_incidents, row[column]) == (1, 1)
    assert row.repair_penalty == penalty + 0.5 * 10  # two rows of 5 days
    assert row.reliability_score == 100 - penalty - 5.0


def test_engine_overheat_is_a_head_gasket_failure_not_an_engine_failure(score):
    row = score({"v": [oil()] * 5 + failure(HEAD_GASKET)})["v"]
    assert (row.head_gasket_incidents, row.engine_failure_incidents) == (1, 0)


def test_unknown_notes_count_as_other_with_the_default_penalty(score):
    row = score({"v": [oil()] * 5 + [repair(downtime=0.0, notes="brake disc cracked"), repair(downtime=0.0, notes="brakes replaced")]})["v"]
    assert (row.other_failure_incidents, row.repair_incidents, row.repair_penalty) == (1, 1, 25.0)


def test_two_failures_of_the_same_kind(score):
    row = score({"v": [oil()] * 10 + failure(ENGINE_FAILURE, downtime=8.5) * 2})["v"]
    assert (row.engine_failure_incidents, row.repair_incidents) == (2, 2)
    assert row.reliability_score == 100 - 70 - 17  # 13.0


def test_failure_kind_ranks_the_score(score):
    rows = score({kind: [oil()] * 10 + failure(notes) for kind, notes in
                  [("engine", ENGINE_FAILURE), ("transmission", TRANSMISSION), ("gasket", HEAD_GASKET)]})
    assert rows["engine"].reliability_score < rows["transmission"].reliability_score < rows["gasket"].reliability_score


# ---------- wear: age and mileage (version 3) ----------

POSTED = datetime.date(2022, 6, 1)


def test_age_points_are_taken_at_the_posting_date(score):
    rows = score({"v10": [oil()] * 5, "v0": [oil()] * 5, "future": [oil()] * 5, "unknown": [oil()] * 5},
                 listings={"v10": (2012, POSTED), "v0": (2022, POSTED), "future": (2023, POSTED), "unknown": (None, POSTED)})
    assert (rows["v10"].vehicle_age_years, rows["v10"].age_points, rows["v10"].reliability_score) == (10, 2.0, 98.0)
    assert (rows["v0"].vehicle_age_years, rows["v0"].age_points) == (0, 0.0)
    assert (rows["future"].vehicle_age_years, rows["future"].age_points) == (0, 0.0)  # next year's model
    assert (rows["unknown"].vehicle_age_years, rows["unknown"].age_points, rows["unknown"].reliability_score) == (None, 0.0, 100.0)


def test_mileage_points_use_the_highest_valid_service_odometer(score):
    rows = score({
        "km": [oil(km=50_000), oil(km=150_000)],                      # 150,000 km -> 7.5
        "bad_odometer": [oil(km=40_000), oil(km=900_000, monotonic=False)],  # the jump is ignored -> 40,000 km -> 2
        "no_km": [oil()] * 3,
    })
    assert (rows["km"].max_odometer_km, rows["km"].mileage_points, rows["km"].reliability_score) == (150_000, 7.5, 92.5)
    assert (rows["bad_odometer"].max_odometer_km, rows["bad_odometer"].mileage_points) == (40_000, 2.0)
    assert (rows["no_km"].max_odometer_km, rows["no_km"].mileage_points) == (None, 0.0)


def test_wear_is_capped_at_10(score):
    row = score({"old": [oil(km=400_000)] * 3}, listings={"old": (1980, POSTED)})["old"]
    assert (row.age_points, row.mileage_points) == (8.4, 20.0)
    assert (row.wear_multiplier, row.wear_penalty, row.reliability_score) == (1.0, 10.0, 90.0)


def test_neglect_raises_the_wear_penalty_up_to_one_and_a_half(score):
    rows = score({
        "careful": [oil(km=100_000)] * 3,                                              # wear 5, x1.0
        "one_skip": [oil(km=100_000)] * 3 + [big_service(skipped=True)],                # x(1 + 0.5 x 5/25) = 1.1
        "neglected": [oil(penalty=40.0, km=100_000)] + [big_service(skipped=True)] * 4,  # x1.5
    })
    assert (rows["careful"].wear_multiplier, rows["careful"].wear_penalty) == (1.0, 5.0)
    assert (rows["one_skip"].wear_multiplier, rows["one_skip"].wear_penalty) == (1.1, 5.5)
    assert (rows["one_skip"].reliability_score) == 100 - 5 - 5.5
    assert (rows["neglected"].wear_multiplier, rows["neglected"].wear_penalty) == (1.5, 7.5)
    assert rows["neglected"].reliability_score == 100 - 15 - 10 - 7.5


def test_worked_example_from_the_documentation(score):
    """12 years, 150,000 km, one skipped service, no failure: 100 - 5 - 9.9 x 1.1 = 84.1."""
    row = score({"v": [oil(km=150_000)] * 10 + [big_service(skipped=True)]}, listings={"v": (2010, POSTED)})["v"]
    assert (row.age_points, row.mileage_points, row.wear_penalty, row.reliability_score) == (2.4, 7.5, 10.89, 84.1)


def test_vehicle_without_history_still_has_no_score_with_age(score):
    rows = score({"serviced": [oil()] * 3, "no_history": []}, listings={"serviced": (2010, POSTED), "no_history": (2000, POSTED)})
    row = rows["no_history"]
    assert (row.reliability_score, row.wear_penalty, row.vehicle_age_years) == (None, None, 22)
