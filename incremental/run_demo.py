"""
The incremental file load demo, end to end (documentation/incremental_load.md):

  once     build the original file (Fourth source in fifths, day 1..5, with changed versions) + the brand lookup
  per day  12 original -> source (watermark) -> 13 source -> Bronze (watermark) -> 14 Silver SCD2 -> 15 Gold current
           -> verify
  at end   re-run every step without a new day (nothing may change) and a "crash" of the Bronze load (data
           written, watermark not moved): the same day is loaded again and Silver stays the same

Usage:  .venv/bin/python -m incremental.run_demo [--days 5]
Writes only to incremental_data/ (gitignored). Needs Java 17 (local Spark), not Docker.
"""

import argparse
import json
import shutil
import time

from cdc import local_pipeline
from incremental import prepare_original, settings, verify

STEPS = [
    "12_incremental_extract_source.py",
    "13_incremental_ingest_bronze.py",
    "14_silver_listings_history.py",
    "15_gold_incremental_dim_vehicle.py",
]


def reset_demo_folder() -> None:
    assert settings.DEMO_DIR.name == "incremental_data" and settings.DEMO_DIR.parent == settings.PROJECT_ROOT
    shutil.rmtree(settings.DEMO_DIR, ignore_errors=True)


def run_day(spark, paths: dict) -> dict:
    """The four steps of one daily run; returns the exit value of every notebook (None = it did its work)."""
    return {step: local_pipeline.run(spark, step, **paths).get("notebook_exit_value") for step in STEPS}


def history_count(spark) -> int:
    return spark.read.format("delta").load(str(settings.SILVER_DIR / "vehicle_listings_history")).count()


def main(days: int = settings.DAYS, spark=None) -> list:
    own_session = spark is None
    reset_demo_folder()
    summary = prepare_original.main()
    all_days = [day for day, _, _ in summary][:days]

    spark = spark or local_pipeline.spark_session()
    paths = verify.demo_paths()
    local_pipeline.build_brand_lookup(spark, bronze=settings.BRONZE_DIR, silver=settings.SILVER_DIR)

    report = []
    for number in range(1, days + 1):
        started = time.time()
        exits = run_day(spark, paths)
        assert all(value is None for value in exits.values()), f"day {number}: a step did nothing: {exits}"
        row = verify.verify_day(spark, all_days[:number], paths)
        report.append({**row, "seconds": round(time.time() - started, 1)})

    # Re-run without a new day: nothing may change.
    versions = history_count(spark)
    exits = run_day(spark, paths)
    assert exits[STEPS[0]] == "nothing to extract" and exits[STEPS[1]] == "nothing to ingest", exits
    assert exits[STEPS[2]] == "nothing to apply" and history_count(spark) == versions, exits

    # Crash of the Bronze load after writing the data: move the watermark back and load the last day again.
    ns = local_pipeline.namespace(spark, **paths)
    ns["write_watermark"](ns["SOURCE_TO_BRONZE"], all_days[days - 2] if days > 1 else "0000-00-00")
    exits = {step: local_pipeline.run(spark, step, **paths).get("notebook_exit_value") for step in STEPS[1:3]}
    assert exits[STEPS[1]] is None, "the last day was not loaded again"
    assert exits[STEPS[2]] == "nothing to apply" and history_count(spark) == versions, "Silver applied a day twice"

    print("\nDay        | records loaded | records in source | Silver versions | current vehicles | seconds")
    for row in report:
        print(f"{row['day']} | {row['records_loaded']:>14} | {row['source_records']:>17} | "
              f"{row['silver_versions']:>15} | {row['current_vehicles']:>16} | {row['seconds']:>7}")
    print("Re-run without a new day: nothing changed. Bronze load repeated after a crash: Silver unchanged.")
    (settings.DEMO_DIR / "demo_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    if own_session:
        spark.stop()
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--days", type=int, default=settings.DAYS)
    main(parser.parse_args().days)
