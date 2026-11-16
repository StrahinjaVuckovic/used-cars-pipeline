"""
Tests for the Reliability Score (notebooks/09_gold_fact_vehicle_reliability.py) on hand-made service events.

The score is 100 minus explainable penalties (documentation/reliability_score.md):
25 per repair incident + 0.5 per repair downtime day, 5 per BODY_WORK, 5 per skipped big service (max 15),
0.5 per oil-gap penalty point (max 10, valid odometer only); ENGINE_WORK (an inspection) is neutral.
"""

from decimal import Decimal

import pytest

from notebook_utils import run_notebook_cells_until
from test_gold import gold_namespace

NOTEBOOK = "09_gold_fact_vehicle_reliability.py"
EVENT_SCHEMA = (
    "vehicle_id STRING, service_type STRING, is_repair BOOLEAN, cost_eur DECIMAL(10,2), downtime_days DOUBLE, "
    "is_skipped_service BOOLEAN, oil_gap_penalty DOUBLE, is_odometer_monotonic BOOLEAN, notes STRING"
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

    def run(vehicles: dict) -> dict:
        rows = [(vid, *event) for vid, events in vehicles.items() for event in events]
        events_df = spark.createDataFrame(rows, EVENT_SCHEMA)
        key = namespace["surrogate_key"]
        dim = spark.createDataFrame([(vid,) for vid in vehicles], "vehicle_id STRING").select(
            key("vehicle_id").alias("vehicle_key"), "vehicle_id")
        dim = namespace["with_unknown_member"](dim, "vehicle_key")
        result = namespace["score_vehicles"](namespace["vehicle_service_summary"](events_df), dim)
        return {r.vehicle_id: r for r in result.join(dim, "vehicle_key").collect()}

    return run


def oil(penalty=0.0, monotonic=True):
    return ("OIL_CHANGE", False, None, None, False, penalty, monotonic, None)


def big_service(skipped=False):
    return ("BIG_SERVICE", False, None, None, skipped, 0.0, True, "Skipped" if skipped else None)


def repair(cost="1500.00", downtime=7.5, notes=None):
    """A repair row; without notes its kind is "other" (penalty 25 per incident)."""
    return ("REPAIR", True, Decimal(cost), downtime, False, 0.0, True, notes)


def failure(kind, downtime=0.0):
    """One incident of a known kind: its diagnosis row and its fix row."""
    return [repair(downtime=downtime, notes=kind[0]), repair(downtime=downtime, notes=kind[1])]


BODY_WORK = ("BODY_WORK", False, None, None, False, 0.0, True, "Minor exterior repair")
ENGINE_WORK = ("ENGINE_WORK", False, None, None, False, 0.0, True, "Major engine inspection")


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
    assert len(namespace["display"].results) == 6  # every report cell ran
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
