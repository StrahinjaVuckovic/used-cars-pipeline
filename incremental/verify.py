"""
Check of the incremental file load after a day (documentation/incremental_load.md):

- both watermarks point to the day; the source and Bronze hold exactly the days loaded so far;
- Silver: the current versions equal the latest record of every listing in the source (same cleaning, same hash
  of the business columns); the number of versions equals the number of records that changed a business value;
  the SCD2 rules hold;
- Gold: one row per current Silver version.
"""

from pyspark.sql import Window
from pyspark.sql import functions as F

from cdc import local_pipeline
from incremental import settings


def demo_paths() -> dict:
    return {"landing": settings.LANDING_DIR, "bronze": settings.BRONZE_DIR, "silver": settings.SILVER_DIR,
            "gold": settings.GOLD_DIR}


def expected_versions(ns: dict, source_df, lookup_df):
    """(current state, number of versions) the source should give: clean every record, count business changes."""
    raw = source_df.withColumn("source_file_path", F.lit("source")).withColumn("_seq", F.struct("change_seq"))
    cleaned = ns["add_manufacturer"](
        ns["clean_vehicle_listings"](raw, ns["INCREMENTAL_SOURCE_SYSTEM"], "2026-12-31", carry_columns=["_seq"]),
        lookup_df,
    ).select("vehicle_id", F.col("_seq.change_seq").alias("change_seq"),
             ns["content_hash"](ns["SCD2_BUSINESS_COLUMNS"]).alias("row_hash"))
    order = Window.partitionBy("vehicle_id").orderBy("change_seq")
    flagged = cleaned.withColumn("_previous", F.lag("row_hash").over(order)).withColumn(
        "_last", F.lead("change_seq").over(order).isNull()
    )
    versions = flagged.filter(F.col("_previous").isNull() | (F.col("_previous") != F.col("row_hash"))).count()
    current = flagged.filter("_last").select("vehicle_id", "row_hash")
    return current, versions


def verify_day(spark, days_loaded: list, paths: dict = None) -> dict:
    paths = paths or demo_paths()
    ns = local_pipeline.namespace(spark, **paths)
    last_day = days_loaded[-1]

    for name in (ns["ORIGINAL_TO_SOURCE"], ns["SOURCE_TO_BRONZE"]):
        assert ns["read_watermark"](name) == last_day, f"watermark {name} = {ns['read_watermark'](name)}, not {last_day}"
    source_df = ns["read_records"](ns["source_path"]())
    source_days = sorted(r.updated_at for r in source_df.select("updated_at").distinct().collect())
    assert source_days == days_loaded, f"source has days {source_days}, expected {days_loaded}"
    bronze_days = ns["list_ingestion_dates"](ns["INCREMENTAL_BRONZE_FOLDER"])
    assert bronze_days == days_loaded, f"Bronze has days {bronze_days}, expected {days_loaded}"

    history = ns["read_delta_table"](ns["history_silver_path"](False))
    counts = ns["check_scd2_versions"](history, "vehicle_listings_history")
    lookup = ns["read_delta_table"](ns["brand_lookup_path"](False)).select("model_normalized", "manufacturer")
    expected_current, expected_count = expected_versions(ns, source_df, lookup)
    current = history.filter("is_current").select("vehicle_id", "row_hash")
    differing = expected_current.exceptAll(current).count() + current.exceptAll(expected_current).count()
    assert differing == 0, f"Silver current state differs from the latest source records in {differing} rows"
    assert counts["versions"] == expected_count, f"Silver has {counts['versions']} versions, expected {expected_count}"
    inserts = history.filter("change_op = 'I'").count()
    assert inserts == counts["vehicles"], f"Silver: {inserts} first versions (I) for {counts['vehicles']} listings"

    dim = ns["read_delta_table"](ns["incremental_gold_path"]("dim_vehicle", False)).filter("vehicle_key <> -1")
    assert dim.count() == counts["current_versions"], "Gold dim_vehicle is not the current Silver state"

    source_records = source_df.count()
    day_records = source_df.filter(F.col("updated_at") == last_day).count()
    print(f"Verified {last_day}: watermarks, source and Bronze days, Silver current state == source, "
          f"{counts['versions']} versions, Gold {dim.count()} current vehicles")
    return {"day": last_day, "records_loaded": day_records, "source_records": source_records,
            "silver_versions": counts["versions"], "current_vehicles": counts["current_versions"]}
