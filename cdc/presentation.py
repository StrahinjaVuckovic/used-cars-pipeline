"""
The CDC demo as a readable report for a presentation (documentation/cdc.md).

Runs the same steps as `cdc.run_demo` (source database, replication slot + snapshot, simulated days, WAL reader,
Silver SCD2, Gold current state, checks, reader crash test), but writes all technical output (Spark, notebook logs)
to cdc_data/presentation.log and prints a formatted English report: what happened on every day, the checks, a
summary table, and three real listings (price drop, sold, new) with their Silver history, their Gold row and the
SQL to look them up in the source database (e.g. in DBeaver).

Usage:  .venv/bin/python -m cdc.presentation [--days 3]
"""

import argparse
import os
import sys
import time

from pyspark.sql import Window
from pyspark.sql import functions as F

from cdc import cdc_reader, db, load_initial, local_pipeline, report_format, run_demo, settings, simulate_changes, verify
from cdc.report_format import banner, check, line, n, section, shown, table

LOG_FILE = settings.DEMO_DIR / "presentation.log"


def quiet():
    return report_format.quiet(LOG_FILE)


# ---------- pipeline steps with their numbers ----------


def run_silver_and_gold(spark) -> dict:
    with quiet():
        silver = local_pipeline.run(spark, "10_silver_listing_changes.py")
        local_pipeline.run(spark, "11_gold_cdc_vehicle_history.py")
    return {
        "exit": silver.get("notebook_exit_value"),
        "new_events": silver.get("new_count", 0),
        "inserted": silver.get("inserted", 0),
        "closed": silver.get("closed", 0),
        "skipped": silver.get("skipped_changes", 0),
    }


def verified(spark, expected_sold: int) -> dict:
    with quiet():
        return verify.verify(spark, expected_sold=expected_sold)


def print_checks(result: dict, second_read: int = None) -> None:
    check(f"Silver current state == PostgreSQL table, row by row ({n(result['source_listings'])} listings)")
    check("SCD2 rules: one current version per listing, no overlapping periods")
    check(f"Gold dim_vehicle == current Silver state ({n(result['gold_current_vehicles'])} vehicles)")
    check(f"Every sold listing has no current version ({n(result['sold_listings'])} sold so far)")
    if second_read is not None:
        check(f"A second read right after finds {second_read} new changes (the slot was confirmed)", second_read == 0)


def ops(by_op: dict) -> str:
    names = {"I": "inserts", "U": "updates", "D": "deletes", "R": "snapshot"}
    return ", ".join(f"{n(by_op[k])} {names[k]}" for k in ("I", "U", "D", "R") if k in by_op)


# ---------- examples ----------


def history_df(spark):
    return spark.read.format("delta").load(str(settings.SILVER_DIR / "vehicle_listings_cdc"))


def gold_df(spark):
    return spark.read.format("delta").load(str(settings.GOLD_DIR / "cdc" / "dim_vehicle"))


def pick_examples(spark) -> dict:
    history = history_df(spark)
    versions = history.groupBy("vehicle_id").agg(
        F.count("*").alias("versions"), F.max(F.col("is_current").cast("int")).alias("has_current"),
    )
    price_dropped = (
        history.filter("change_op = 'U'").select("vehicle_id").distinct()
        .join(versions.filter("has_current = 1"), "vehicle_id")
        .orderBy(F.desc("versions"), "vehicle_id").first().vehicle_id
    )
    sold = versions.filter("has_current = 0").orderBy(F.desc("versions"), "vehicle_id").first().vehicle_id
    new = (
        history.filter("change_op = 'I' AND is_current").orderBy("vehicle_id").first().vehicle_id
    )
    return {"price drop": price_dropped, "sold": sold, "new listing": new}


def show_example(spark, title: str, vehicle_id: str, source_rows: dict) -> None:
    section(f"{title}  (vehicle_id {vehicle_id})")
    rows = (
        history_df(spark).filter(F.col("vehicle_id") == vehicle_id)
        .withColumn("version", F.row_number().over(Window.orderBy("change_seq")))
        .orderBy("change_seq").collect()
    )
    print("  Silver history (vehicle_listings_cdc, every version):")
    table(
        ["version", "change", "price", "odometer", "condition", "valid_from", "valid_to", "is_current"],
        [[r.version, r.change_op, shown(r.price), shown(r.odometer), shown(r.condition),
          r.valid_from.strftime("%H:%M:%S.%f")[:-3], r.valid_to.strftime("%H:%M:%S.%f")[:-3] if r.valid_to else "-",
          r.is_current] for r in rows],
    )
    gold = gold_df(spark).filter(F.col("vehicle_id") == vehicle_id).collect()
    print("\n  Gold dim_vehicle (current state only):")
    if gold:
        g = gold[0]
        table(["vehicle_key", "manufacturer", "model", "year", "condition", "title_status"],
              [[g.vehicle_key, g.manufacturer, g.model, shown(g.manufacture_year), g.condition, g.title_status]])
    else:
        sold_at = rows[-1].valid_to.strftime("%H:%M:%S.%f")[:-3]
        print(f"  (no row: the listing was sold at {sold_at}; the DELETE closed its last version in Silver)")
    source = source_rows.get(vehicle_id)
    print("\n  PostgreSQL right now:")
    if source:
        print(f"  price {source['price']}, odometer {source['odometer']}, condition {source['condition']}")
    else:
        print("  (no row: the listing was deleted in the source)")
    print(f"\n  Check it yourself:  SELECT id, price, odometer, condition FROM listings WHERE id = '{vehicle_id}';")


# ---------- the report ----------


def main(days: int = 3) -> None:
    run_demo.reset_demo_folder()
    saved_stderr = report_format.redirect_stderr_to_log(LOG_FILE)
    started = time.time()
    try:
        banner("CDC DEMO - log-based change data capture with SCD Type 2 history in Silver")
        print(" Source: PostgreSQL in Docker plays the database of a listing platform.")
        print(" The pipeline reads ONLY the changes from the database's write-ahead log (WAL), not the whole table.")
        print(f" Technical output (Spark, notebook logs) goes to {LOG_FILE.relative_to(settings.PROJECT_ROOT)}")

        section("STEP 1  Source system")
        with quiet():
            run_demo.ensure_database()
            loaded = load_initial.main()
        line("Sample:", f"{n(settings.SAMPLE_SIZE)} listings from data/FourthSourceSystem")
        line("Database:", f"{n(loaded['initial_rows'])} listings loaded into table 'listings' (state on day 0)")
        line("Reserve:", f"{n(loaded['reserve_rows'])} listings kept aside, inserted later as new listings")

        section("STEP 2  Replication slot and initial snapshot")
        with quiet():
            snapshot = cdc_reader.create_slot_and_snapshot()
        line("Slot:", f"'{db.SLOT_NAME}' created at WAL position {snapshot['lsn']} (our bookmark in the log)")
        line("Snapshot:", f"{n(snapshot['events'])} rows written to Bronze as events with op = R")
        line("Bronze file:", os.path.relpath(snapshot["file"], settings.PROJECT_ROOT))

        section("STEP 3  Initial load: Bronze -> Silver -> Gold")
        with quiet():
            spark = local_pipeline.spark_session()
            local_pipeline.build_brand_lookup(spark)
        applied = run_silver_and_gold(spark)
        result = verified(spark, expected_sold=0)
        line("Silver:", f"{n(applied['inserted'])} versions in vehicle_listings_cdc (all current)")
        line("Gold:", f"{n(result['gold_current_vehicles'])} vehicles in dim_vehicle")
        print_checks(result)
        summary = [[0, "snapshot", n(result["source_listings"]), n(result["silver_versions"]),
                    n(result["gold_current_vehicles"]), 0, "-"]]

        sold_total = 0
        for day in range(1, days + 1):
            crash_day = day == days
            section(f"DAY {day}" + ("  (with a simulated reader crash)" if crash_day else ""))
            with quiet():
                changes = simulate_changes.main(day)
            sold_total += changes["deletes"]
            line("Users:", f"{n(changes['inserts'])} new listings, {n(changes['updates'])} updates of "
                           f"{n(changes['updated_vehicles'])} listings ({n(changes['attribute_updates'])} condition / "
                           f"title changes, the rest price drops), {n(changes['deletes'])} sold")
            with quiet():
                read = cdc_reader.read_changes(crash_before_ack=crash_day)
            line("CDC reader:", f"{n(read['events'])} changes read from the WAL ({ops(read['by_op'])})")
            line("Bronze file:", os.path.relpath(read["file"], settings.PROJECT_ROOT))
            line("Slot:", "NOT confirmed - the reader crashes here" if crash_day else "confirmed after the file was written")
            applied = run_silver_and_gold(spark)
            line("Silver:", f"{n(applied['new_events'])} new changes applied: {n(applied['inserted'])} new versions, "
                            f"{n(applied['closed'])} versions closed, {n(applied['skipped'])} without a new value")
            if crash_day:
                versions_before = history_df(spark).count()
                with quiet():
                    again = cdc_reader.read_changes()
                line("Restart:", f"the reader runs again and gets the SAME {n(again['events'])} changes "
                                 f"(at-least-once delivery); slot confirmed now")
                rerun = run_silver_and_gold(spark)
                unchanged = rerun["exit"] == "nothing to apply" and history_df(spark).count() == versions_before
                line("Silver:", "recognises them by their WAL position and applies nothing")
                check(f"No change applied twice (Silver still has {n(versions_before)} versions)", unchanged)
            with quiet():
                second = cdc_reader.read_changes()["events"]
            result = verified(spark, expected_sold=sold_total)
            line("Gold:", f"{n(result['gold_current_vehicles'])} current vehicles")
            print_checks(result, second)
            share = 100 * read["events"] / result["source_listings"]
            summary.append([day, n(read["events"]), n(result["source_listings"]), n(result["silver_versions"]),
                            n(result["gold_current_vehicles"]), n(sold_total), f"{share:.1f} %"])

        section("SUMMARY")
        table(["day", "changes read", "listings in source", "Silver versions", "Gold vehicles", "sold (total)",
               "work vs. full load"], summary)
        print("\n  - Gold vehicles = listings in the source every day: Gold holds only the current state.")
        print("  - Silver versions > listings: Silver also keeps the old prices and the sold listings (the history).")
        print(f"  - WAL still held by the slot: {n(cdc_reader.slot_lag_bytes())} bytes (everything was confirmed).")

        conn = db.connect()
        try:
            source_rows = {r["id"]: r for r in db.fetch_rows(conn, ["id", "price", "odometer", "condition"])}
            listings = db.row_count(conn)
        finally:
            conn.close()
        banner("REAL EXAMPLES FROM THIS RUN")
        for title, vehicle_id in pick_examples(spark).items():
            show_example(spark, f"Example: {title}", vehicle_id, source_rows)

        banner("LOOK IT UP IN THE SOURCE DATABASE (DBeaver, database listings_source)")
        print(f"  SELECT count(*) FROM listings;                     -- {n(listings)} listings now")
        print("  SELECT slot_name, confirmed_flush_lsn FROM pg_replication_slots;   -- our bookmark in the WAL")
        print("  Changes not read yet (none right now; make an UPDATE first to see one):")
        print("  SELECT lsn, data FROM pg_logical_slot_peek_changes('listings_cdc', NULL, NULL,")
        print("         'format-version', '2', 'include-timestamp', '1', 'include-lsn', '1');")
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
    parser.add_argument("--days", type=int, default=3)
    main(parser.parse_args().days)
