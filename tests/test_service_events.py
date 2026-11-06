"""Tests for notebooks/04_silver_service_events.py (full notebook runs on crafted and real samples)."""

import datetime
from decimal import Decimal

import pytest

from conftest import SAMPLE_ROWS, catalog_database_names, write_service_book_partition
from notebook_utils import load_config_and_utils, run_notebook

NOTEBOOK = "04_silver_service_events.py"

EXPECTED_DTYPES = {
    "service_event_id": "string", "vehicle_id": "string", "service_date": "date", "odometer_km": "int",
    "event_type": "string", "service_type": "string", "maintenance_kind": "string", "severity": "string",
    "is_maintenance": "boolean", "is_repair": "boolean", "is_inspection": "boolean", "is_oil_change": "boolean",
    "interval_km_target": "int", "prev_odometer_km": "int", "km_since_prev": "int",
    "is_odometer_monotonic": "boolean", "oil_gap_km": "int", "oil_gap_penalty": "double",
    "cost_eur": "decimal(10,2)", "downtime_days": "int", "notes": "string", "is_skipped_service": "boolean",
    "ingestion_date": "date", "source_file_path": "string", "silver_processed_at": "timestamp",
    "dq_issues": "array<string>", "is_valid": "boolean",
}

HEADER = (
    "car_id,service_date,odometer_km,event_type,maintenance_kind,severity,root_cause,warranty_flag,"
    "is_maintenance,is_repair,is_inspection,is_tyre,is_oil_change,interval_km_target,prev_odometer_km,"
    "km_since_prev,odometer_monotonic_flag,oil_gap_km,oil_gap_penalty_piece,cost_eur,downtime_days,"
    "service_type_raw,notes_raw,desc_all"
)

# Crafted ServiceBook rows covering the cleaning rules (same layout as the real file).
EDGE_CASE_ROWS = [
    # normal oil change + an exact duplicate of it
    "c1,2006-01-23,10026.0,MAINTENANCE,OIL/OIL_FILTER,LOW,UNKNOWN,False,1,0,0,0,1,15000.0,,,1,,0.0,,,Oil Change,,oil change",
    "c1,2006-01-23,10026.0,MAINTENANCE,OIL/OIL_FILTER,LOW,UNKNOWN,False,1,0,0,0,1,15000.0,,,1,,0.0,,,Oil Change,,oil change",
    # skipped big service
    "c1,2007-03-01,25000.0,INSPECTION,,LOW,UNKNOWN,False,1,0,1,0,0,,10026.0,14974.0,1,,0.0,,,Big Service,Skipped,big service skipped",
    # repair with cost and downtime
    "c2,2010-05-05,120000.0,REPAIR,,HIGH,UNKNOWN,False,0,1,0,0,0,,100000.0,20000.0,1,,0.0,1234.56,5.0,repair,engine failure,repair engine failure",
    # impossible dates
    "c2,1900-01-01,50000.0,MAINTENANCE,OIL/OIL_FILTER,LOW,UNKNOWN,False,1,0,0,0,1,15000.0,,,1,,0.0,,,Oil Change,,oil change",
    "c2,2101-02-01,60000.0,MAINTENANCE,OIL/OIL_FILTER,LOW,UNKNOWN,False,1,0,0,0,1,15000.0,,,1,,0.0,,,Oil Change,,oil change",
    # impossible odometer
    "c3,2011-01-01,9999958.0,MAINTENANCE,OIL/OIL_FILTER,LOW,UNKNOWN,False,1,0,0,0,1,15000.0,,,1,,0.0,,,Oil Change,,oil change",
    # odometer went backwards
    "c3,2012-01-01,5000.0,MAINTENANCE,OIL/OIL_FILTER,LOW,UNKNOWN,False,1,0,0,0,1,15000.0,20000.0,-15000.0,0,-15000.0,0.0,,,Oil Change,,oil change",
    # same car/date/odometer/event as the next row but a different service type -> different events
    "c4,2013-01-01,30000.0,UNKNOWN,,MEDIUM,UNKNOWN,False,0,0,0,0,0,,,,1,,0.0,,,Body Work,,body work",
    "c4,2013-01-01,30000.0,UNKNOWN,,MEDIUM,UNKNOWN,False,0,0,0,0,0,,,,1,,0.0,,,Engine Work,,engine work",
    # impossible calendar date: must be flagged, not stop the notebook
    "c5,2012-02-30,40000.0,MAINTENANCE,OIL/OIL_FILTER,LOW,UNKNOWN,False,1,0,0,0,1,15000.0,,,1,,0.0,,,Oil Change,,oil change",
    # two real repairs on the same day (same car/date/odometer/event type, different cost and odometer chain),
    # the pattern found 117 times in the real ServiceBook: both must be kept
    "c6,2015-06-01,44473.0,REPAIR,,HIGH,UNKNOWN,False,0,1,0,0,0,,41925.0,2548.0,1,,0.0,1065.80,8.0,repair,,repair",
    "c6,2015-06-01,44473.0,REPAIR,,HIGH,UNKNOWN,False,0,1,0,0,0,,44473.0,0.0,1,,0.0,1525.59,4.0,repair,,repair",
]


def run_service_events_notebook(spark, bronze_path, silver_path):
    databases_before = catalog_database_names(spark)
    namespace = load_config_and_utils(
        spark,
        bronze_path=str(bronze_path),
        silver_path=str(silver_path),
        widget_values={"ingestion_date": "latest", "dev_mode": "false"},
    )
    run_notebook(NOTEBOOK, namespace)  # the notebook's own asserts run here
    assert catalog_database_names(spark) == databases_before, "Notebook created a database in the catalog"
    result = spark.read.format("delta").load(f"{silver_path}/service_events").cache()
    result.count()
    return namespace, result


@pytest.fixture(scope="module")
def edge_case_result(spark, tmp_path_factory):
    bronze = tmp_path_factory.mktemp("bronze-edge")
    write_service_book_partition(bronze, "\n".join([HEADER, *EDGE_CASE_ROWS]) + "\n")
    namespace, result = run_service_events_notebook(spark, bronze, tmp_path_factory.mktemp("silver-edge"))
    return namespace, result


def rows_of(result, vehicle_id):
    return sorted(result.filter(f"vehicle_id = '{vehicle_id}'").collect(), key=lambda r: r.service_type)


def test_schema_matches_documentation(edge_case_result):
    namespace, result = edge_case_result
    assert result.columns == namespace["SERVICE_EVENTS_COLUMNS"]
    assert dict(result.dtypes) == EXPECTED_DTYPES


def test_dropped_columns_are_gone(edge_case_result):
    _, result = edge_case_result
    for column in ["root_cause", "warranty_flag", "is_tyre", "desc_all", "car_id", "service_type_raw", "notes_raw"]:
        assert column not in result.columns


def test_exact_duplicates_removed_and_rows_kept(edge_case_result):
    _, result = edge_case_result
    assert result.count() == len(EDGE_CASE_ROWS) - 1  # only the exact duplicate is removed
    assert result.select("service_event_id").distinct().count() == result.count()


def test_two_repairs_on_the_same_day_are_both_kept(edge_case_result):
    _, result = edge_case_result
    rows = result.filter("vehicle_id = 'c6'").collect()
    assert sorted(str(r.cost_eur) for r in rows) == ["1065.80", "1525.59"]
    assert len({r.service_event_id for r in rows}) == 2


def test_different_service_type_is_a_different_event(edge_case_result):
    _, result = edge_case_result
    assert [r.service_type for r in rows_of(result, "c4")] == ["BODY_WORK", "ENGINE_WORK"]


def test_normal_event_is_typed_and_valid(edge_case_result):
    _, result = edge_case_result
    row = result.filter("vehicle_id = 'c1' AND service_type = 'OIL_CHANGE'").first()
    assert row.service_date == datetime.date(2006, 1, 23)
    assert row.odometer_km == 10026
    assert row.interval_km_target == 15000
    assert row.is_maintenance is True and row.is_oil_change is True and row.is_repair is False
    assert row.maintenance_kind == "OIL/OIL_FILTER" and row.severity == "LOW"
    assert row.notes is None and row.is_skipped_service is False
    assert row.dq_issues == [] and row.is_valid is True
    assert row.ingestion_date == datetime.date(2026, 11, 3)


def test_skipped_service(edge_case_result):
    _, result = edge_case_result
    row = result.filter("service_type = 'BIG_SERVICE'").first()
    assert row.notes == "Skipped" and row.is_skipped_service is True
    assert row.is_inspection is True and row.maintenance_kind is None


def test_repair_cost_and_downtime(edge_case_result):
    _, result = edge_case_result
    row = result.filter("event_type = 'REPAIR' AND vehicle_id = 'c2'").first()
    assert row.cost_eur == Decimal("1234.56") and row.downtime_days == 5
    assert row.severity == "HIGH" and row.is_repair is True


@pytest.mark.parametrize("odometer_km", [50000, 60000])
def test_impossible_service_dates_are_nulled_and_flagged(edge_case_result, odometer_km):
    _, result = edge_case_result
    row = result.filter(f"vehicle_id = 'c2' AND odometer_km = {odometer_km}").first()
    assert row.service_date is None
    assert row.dq_issues == ["invalid_service_date"] and row.is_valid is False


def test_impossible_odometer_is_nulled_and_flagged(edge_case_result):
    _, result = edge_case_result
    row = result.filter("vehicle_id = 'c3' AND service_date = DATE'2011-01-01'").first()
    assert row.odometer_km is None and row.dq_issues == ["invalid_odometer_km"]


def test_non_monotonic_odometer_is_flagged_and_values_kept(edge_case_result):
    _, result = edge_case_result
    row = result.filter("vehicle_id = 'c3' AND service_date = DATE'2012-01-01'").first()
    assert row.is_odometer_monotonic is False
    assert row.km_since_prev == -15000 and row.oil_gap_km == -15000
    assert row.dq_issues == ["non_monotonic_odometer"]


def test_unparsed_service_date_is_flagged_and_notebook_continues(edge_case_result):
    namespace, result = edge_case_result
    row = result.filter("vehicle_id = 'c5'").first()
    assert row.service_date is None
    assert row.dq_issues == ["unparsed_service_date"] and row.is_valid is False
    assert namespace["unparsed_dates"] == 1


def test_service_event_id_is_stable_across_runs(spark, edge_case_result, tmp_path):
    _, first_result = edge_case_result
    first_ids = {r.service_event_id for r in first_result.select("service_event_id").collect()}

    bronze = tmp_path / "bronze"
    write_service_book_partition(bronze, "\n".join([HEADER, *EDGE_CASE_ROWS]) + "\n")
    _, second_result = run_service_events_notebook(spark, bronze, tmp_path / "silver")
    assert {r.service_event_id for r in second_result.select("service_event_id").collect()} == first_ids


def test_real_sample(spark, bronze_sample, tmp_path):
    namespace, result = run_service_events_notebook(spark, bronze_sample, tmp_path / "silver")
    assert dict(result.dtypes) == EXPECTED_DTYPES
    assert result.count() <= SAMPLE_ROWS
    assert result.filter("service_date IS NULL AND NOT array_contains(dq_issues, 'invalid_service_date')").count() == 0
    assert set(r.event_type for r in result.select("event_type").distinct().collect()) <= {
        "MAINTENANCE", "INSPECTION", "REPAIR", "UNKNOWN",
    }
    assert len(namespace["display"].results) >= 4


def test_coverage_against_vehicle_listings(spark, tmp_path, capsys):
    """When a vehicle_listings Delta table exists at the Silver path, the notebook reports the match percentage."""
    silver = tmp_path / "silver"
    spark.createDataFrame([("c1",), ("c2",)], "vehicle_id STRING").write.format("delta").save(str(silver / "vehicle_listings"))
    bronze = tmp_path / "bronze"
    write_service_book_partition(bronze, "\n".join([HEADER, *EDGE_CASE_ROWS]) + "\n")
    run_service_events_notebook(spark, bronze, silver)
    assert "2 of 6 service-book vehicles (33.33%) exist in" in capsys.readouterr().out


def test_service_date_after_the_ingestion_date_is_invalid(spark, tmp_path):
    """Idempotency: "in the future" is relative to the Bronze ingestion date, not to the day of the run."""
    rows = [
        "c1,2026-11-03,10026.0,MAINTENANCE,OIL/OIL_FILTER,LOW,UNKNOWN,False,1,0,0,0,1,15000.0,,,1,,0.0,,,Oil Change,,oil change",
        "c1,2026-11-04,10500.0,MAINTENANCE,OIL/OIL_FILTER,LOW,UNKNOWN,False,1,0,0,0,1,15000.0,,,1,,0.0,,,Oil Change,,oil change",
    ]
    bronze = tmp_path / "bronze"
    write_service_book_partition(bronze, "\n".join([HEADER, *rows]) + "\n")  # partition 2026-11-03
    _, result = run_service_events_notebook(spark, bronze, tmp_path / "silver")
    by_odometer = {r.odometer_km: r for r in result.collect()}
    assert by_odometer[10026].dq_issues == []  # on the ingestion date itself: valid
    assert by_odometer[10500].service_date is None
    assert by_odometer[10500].dq_issues == ["invalid_service_date"]
