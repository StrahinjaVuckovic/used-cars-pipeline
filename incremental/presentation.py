"""
The incremental file load as a readable report for a presentation (documentation/incremental_load.md).

Runs the same steps as `incremental.run_demo` (original file in five days, then per day: original -> source ->
Bronze -> Silver SCD2 -> Gold current state, re-run without a new day, crash of the Bronze load), but writes all
technical output (Spark, notebook logs) to incremental_data/presentation.log and prints a formatted English report:
the original file, the watermarks before and after every step, the checks, a summary table, one real listing with
its records in the source, its Silver history and its Gold row, the watermark table and where to look at the files.

Usage:  .venv/bin/python -m incremental.presentation [--days 5]
"""

import argparse
import os
import sys
import time

from pyspark.sql import functions as F

from cdc import local_pipeline, report_format
from cdc.report_format import banner, check, line, n, section, shown, table
from incremental import prepare_original, run_demo, settings, verify

LOG_FILE = settings.DEMO_DIR / "presentation.log"
EXTRACT, INGEST, SILVER, GOLD = run_demo.STEPS


def quiet():
    return report_format.quiet(LOG_FILE)


def relative(path) -> str:
    return os.path.relpath(path, settings.PROJECT_ROOT)


def run_step(spark, notebook: str, paths: dict) -> dict:
    with quiet():
        return local_pipeline.run(spark, notebook, **paths)


def watermarks(spark, paths: dict) -> tuple:
    with quiet():
        ns = local_pipeline.namespace(spark, **paths)
        return ns["read_watermark"](ns["ORIGINAL_TO_SOURCE"]), ns["read_watermark"](ns["SOURCE_TO_BRONZE"])


def history_df(spark):
    return spark.read.format("delta").load(str(settings.SILVER_DIR / "vehicle_listings_history"))


def bronze_count(spark, day: str) -> int:
    path = settings.BRONZE_DIR / "IncrementalListings" / f"ingestion_date={day}"
    return spark.read.json(str(path)).count()


# ---------- example ----------


def show_example(spark, paths: dict) -> None:
    history = history_df(spark)
    vehicle_id = (
        history.groupBy("vehicle_id").count().join(history.filter("is_current"), "vehicle_id")
        .orderBy(F.desc("count"), "vehicle_id").first().vehicle_id
    )
    section(f"Example: a listing that changed over the days  (vehicle_id {vehicle_id})")

    with quiet():
        ns = local_pipeline.namespace(spark, **paths)
        source = ns["read_records"](ns["source_path"]()).filter(F.col("id") == vehicle_id).orderBy("change_seq").collect()
    print("  1) Its records in the source file (what arrived, day by day):")
    table(["updated_at", "change_seq", "price", "odometer", "condition", "title_status"],
          [[r.updated_at, r.change_seq, shown(r.price), shown(r.odometer), shown(r.condition), shown(r.title_status)]
           for r in source])

    rows = history.filter(F.col("vehicle_id") == vehicle_id).orderBy("change_seq").collect()
    print("\n  2) Silver history (vehicle_listings_history, every version):")
    table(["change", "change_seq", "price", "odometer", "condition", "valid_from", "valid_to", "is_current"],
          [[r.change_op, r.change_seq, shown(r.price), shown(r.odometer), shown(r.condition),
            r.valid_from.strftime("%Y-%m-%d"), r.valid_to.strftime("%Y-%m-%d") if r.valid_to else "-", r.is_current]
           for r in rows])

    gold = spark.read.format("delta").load(str(settings.GOLD_DIR / "incremental" / "dim_vehicle")) \
        .filter(F.col("vehicle_id") == vehicle_id).collect()
    print("\n  3) Gold dim_vehicle (current state only):")
    table(["vehicle_key", "manufacturer", "model", "year", "condition", "title_status"],
          [[g.vehicle_key, g.manufacturer, g.model, shown(g.manufacture_year), g.condition, g.title_status]
           for g in gold])


# ---------- the report ----------


def main(days: int = settings.DAYS) -> None:
    run_demo.reset_demo_folder()
    saved_stderr = report_format.redirect_stderr_to_log(LOG_FILE)
    started = time.time()
    try:
        banner("INCREMENTAL FILE LOAD - watermarks, SCD Type 2 history in Silver, current state in Gold")
        print(" Task: split a file into fifths (one day each); every run moves the NEXT day from the original file")
        print(" into a new source file that feeds Bronze. Watermarks remember how far every load has come.")
        print(f" Technical output (Spark, notebook logs) goes to {relative(LOG_FILE)}")

        section("STEP 1  The original file")
        with quiet():
            summary = prepare_original.main()
        all_days = [day for day, _, _ in summary][:days]
        line("Source:", "data/FourthSourceSystem, 106,970 listings, shuffled and split into 5 fifths")
        line("Changes:", "from day 2 on, ~3 % of the earlier listings drop their price, ~0.5 % change condition")
        line("File:", relative(settings.ORIGINAL_FILE))
        table(["day (updated_at)", "new listings", "changed listings", "records"],
              [[day, n(new), n(changed), n(new + changed)] for day, new, changed in summary])
        print(f"  Every record also has change_seq (1 .. {n(sum(a + b for _, a, b in summary))}): the order in which"
              " things happened.")

        with quiet():
            spark = local_pipeline.spark_session()
            local_pipeline.build_brand_lookup(spark, bronze=settings.BRONZE_DIR, silver=settings.SILVER_DIR)
        paths = verify.demo_paths()

        rows = []
        for number, day in enumerate(all_days, start=1):
            section(f"DAY {number}  ({day})")
            before = watermarks(spark, paths)

            extract = run_step(spark, EXTRACT, paths)
            line("Extract:", f"watermark original_to_source {before[0] or 'none (first run)'} -> next day {day}: "
                             f"{n(extract.get('records', 0))} records appended to the source file")
            ingest = run_step(spark, INGEST, paths)
            loaded = bronze_count(spark, day)
            line("Ingest:", f"watermark source_to_bronze {before[1] or 'none (first run)'}: {n(loaded)} new records "
                            f"-> Bronze partition ingestion_date={day}")
            silver = run_step(spark, SILVER, paths)
            by_op = silver.get("by_op", {})
            line("Silver:", f"read {n(silver.get('read_count', 0))} Bronze records, {n(silver.get('new_count', 0))} new "
                            f"after the watermark ({n(by_op.get('I', 0))} new listings, {n(by_op.get('U', 0))} changes): "
                            f"{n(silver.get('inserted', 0))} new versions, {n(silver.get('closed', 0))} versions closed")
            gold = run_step(spark, GOLD, paths)
            line("Gold:", f"{n(gold.get('vehicles', 0))} current vehicles")
            after = watermarks(spark, paths)
            line("Watermarks:", f"original_to_source = {after[0]}, source_to_bronze = {after[1]}")

            with quiet():
                result = verify.verify_day(spark, all_days[:number], paths)
            check(f"Both watermarks moved to {day}")
            check(f"Source file and Bronze hold exactly the days {all_days[0]} .. {day}")
            check("Silver current state == the latest record of every listing in the source (same cleaning)")
            check(f"Silver has exactly one version per real change ({n(result['silver_versions'])} versions)")
            check("SCD2 rules: one current version per listing, no overlapping periods, one I version per listing")
            check(f"Gold dim_vehicle == current Silver state ({n(result['current_vehicles'])} vehicles)")
            rows.append([day, n(result["records_loaded"]), n(result["source_records"]), n(result["silver_versions"]),
                         n(result["current_vehicles"])])

        section("RE-RUN WITHOUT A NEW DAY")
        versions = history_df(spark).count()
        exits = [run_step(spark, step, paths).get("notebook_exit_value") for step in (EXTRACT, INGEST, SILVER)]
        line("Extract:", f"{exits[0]} (the watermark is on the last day of the original file)")
        line("Ingest:", f"{exits[1]} (no record after the watermark)")
        line("Silver:", f"{exits[2]}")
        check(f"Nothing changed (Silver still has {n(versions)} versions)",
              exits == ["nothing to extract", "nothing to ingest", "nothing to apply"]
              and history_df(spark).count() == versions)

        section("CRASH OF THE BRONZE LOAD (data written, watermark not moved)")
        with quiet():
            ns = local_pipeline.namespace(spark, **paths)
            ns["write_watermark"](ns["SOURCE_TO_BRONZE"], all_days[-2])
        line("Simulated:", f"watermark source_to_bronze set back to {all_days[-2]}, as if the run had crashed "
                           "before moving it")
        reloaded = run_step(spark, INGEST, paths).get("notebook_exit_value")
        line("Ingest:", f"loads {all_days[-1]} again ({n(bronze_count(spark, all_days[-1]))} records); the partition is "
                        "overwritten, not doubled")
        repeat = run_step(spark, SILVER, paths).get("notebook_exit_value")
        line("Silver:", f"{repeat}: it already has these records (by change_seq)")
        check(f"Nothing applied twice (Silver still has {n(versions)} versions)",
              reloaded is None and repeat == "nothing to apply" and history_df(spark).count() == versions)

        section("SUMMARY")
        table(["day", "records loaded", "records in source", "Silver versions", "current vehicles (Gold)"], rows)
        print("\n  - Every run read only ONE new day from the original file, not the whole file.")
        print("  - Silver versions > vehicles: Silver keeps every price and condition a listing had (the history).")
        print("  - Gold has one row per vehicle: only the current state (mentor's decision).")

        banner("REAL EXAMPLE FROM THIS RUN")
        show_example(spark, paths)

        banner("THE WATERMARK TABLE AND THE FILES")
        with quiet():
            marks = ns["read_delta_table"](ns["watermarks_path"]()).orderBy("pipeline").collect()
        print(f"  Watermark table: {relative(settings.BRONZE_DIR / '_control' / 'watermarks')} (Delta)")
        table(["pipeline", "watermark_value", "updated_at_run"],
              [[m.pipeline, m.watermark_value, m.updated_at_run.strftime("%Y-%m-%d %H:%M:%S")] for m in marks])
        print("\n  Look at the files yourself:")
        print(f"  ls {relative(settings.ORIGINAL_FILE.parent)}            # the original file (all 5 days)")
        print(f"  ls {relative(settings.LANDING_DIR / 'IncrementalSource' / 'source')}              # the new source: one file per day")
        print(f"  ls {relative(settings.BRONZE_DIR / 'IncrementalListings')}               # one Bronze partition per day")
        print(f"\n Finished in {time.time() - started:.0f} s. All checks passed.")
        spark.stop()
    except AssertionError as error:
        print(f"\n  [FAILED] {error}\n  Details: {LOG_FILE}")
        raise SystemExit(1)
    finally:
        report_format.restore_stderr(saved_stderr)
        sys.stdout.flush()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--days", type=int, default=settings.DAYS)
    main(parser.parse_args().days)
