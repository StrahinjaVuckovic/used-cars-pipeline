"""
The whole CDC demo, end to end (documentation/cdc.md):

  day 0  load the source database (80 % of a sample), create the slot + initial snapshot, Silver, Gold, verify
  day N  simulator changes the database -> reader lands the WAL changes in Bronze -> Silver MERGE -> Gold -> verify

With --crash-test the reader "dies" on the last day after writing the changes but before confirming them; the
next read delivers the same changes again and Silver must not change (at-least-once + idempotent Silver).

Usage:  .venv/bin/python -m cdc.run_demo --days 3 --crash-test
Needs Docker (the source database) and Java 17 (local Spark). Writes only to cdc_data/ (gitignored).
"""

import argparse
import json
import shutil
import subprocess
import time

from cdc import cdc_reader, db, load_initial, local_pipeline, settings, simulate_changes, verify


def ensure_database() -> None:
    if db.is_available():
        return
    print("Starting the source database (docker compose)...")
    subprocess.run(
        ["docker", "compose", "--env-file", str(settings.PROJECT_ROOT / ".env"), "up", "-d", "--build", "--wait"],
        cwd=settings.PROJECT_ROOT / "cdc", check=True,
    )


def reset_demo_folder() -> None:
    assert settings.DEMO_DIR.name == "cdc_data" and settings.DEMO_DIR.parent == settings.PROJECT_ROOT
    shutil.rmtree(settings.DEMO_DIR, ignore_errors=True)


def history_size(spark) -> int:
    return spark.read.format("delta").load(str(settings.SILVER_DIR / "vehicle_listings_cdc")).count()


def main(days: int, crash_test: bool, spark=None) -> list:
    own_session = spark is None
    ensure_database()
    reset_demo_folder()
    load_initial.main()
    cdc_reader.create_slot_and_snapshot()

    spark = spark or local_pipeline.spark_session()
    local_pipeline.build_brand_lookup(spark)
    started = time.time()
    local_pipeline.run_pipeline(spark)
    report = [{"day": 0, "changes": 0, **verify.verify(spark, expected_sold=0),
               "seconds": round(time.time() - started, 1)}]

    sold_total = 0
    for day in range(1, days + 1):
        changes = simulate_changes.main(day)
        sold_total += changes["deletes"]
        started = time.time()
        crash = None
        if crash_test and day == days:
            first = cdc_reader.read_changes(crash_before_ack=True)
            local_pipeline.run_pipeline(spark)
            versions_before = history_size(spark)
            again = cdc_reader.read_changes()  # the same changes are delivered again
            rerun = local_pipeline.run_pipeline(spark)
            assert again["events"] == first["events"], "the unconfirmed changes were not delivered again"
            assert rerun["silver_exit"] == "nothing to apply", "Silver applied a re-delivered change twice"
            assert history_size(spark) == versions_before
            crash = f"re-delivered {again['events']} changes, Silver unchanged"
            read = first
        else:
            read = cdc_reader.read_changes()
            local_pipeline.run_pipeline(spark)
        repeat = cdc_reader.read_changes()  # nothing new: a second read must find no changes
        assert repeat["events"] == 0
        report.append({
            "day": day, "changes": read["events"], **verify.verify(spark, expected_sold=sold_total),
            "seconds": round(time.time() - started, 1), **({"crash_test": crash} if crash else {}),
        })

    print("\nDay | changes read | listings in source | Silver versions | Gold current vehicles | sold")
    for row in report:
        print(f"{row['day']:>3} | {row['changes']:>12} | {row['source_listings']:>18} | {row['silver_versions']:>15} "
              f"| {row['gold_current_vehicles']:>21} | {row['sold_listings']}")
    for row in report[1:]:
        share = 100 * row["changes"] / row["source_listings"]
        print(f"Day {row['day']}: {row['changes']} changes processed instead of {row['source_listings']} rows "
              f"of a full load ({share:.1f} %)")
    print(f"WAL still held by the slot: {cdc_reader.slot_lag_bytes()} bytes")
    (settings.DEMO_DIR / "demo_report.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    if own_session:
        spark.stop()
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--crash-test", action="store_true")
    arguments = parser.parse_args()
    main(arguments.days, arguments.crash_test)
