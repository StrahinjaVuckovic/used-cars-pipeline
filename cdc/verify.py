"""
End-to-end check of the CDC demo after a run: the lakehouse must hold exactly the state of the source database.

- Silver: the current versions (`WHERE is_current`) equal the PostgreSQL table, row by row, after the same
  cleaning (same ids, same hash of the business columns); the SCD2 rules hold (one current version per listing,
  no overlapping periods).
- Gold: dim_vehicle holds exactly the current Silver versions (one row per vehicle); every listing the simulator
  deleted has no current version in Silver.
"""

from datetime import date

from pyspark.sql import functions as F

from cdc import db, local_pipeline, settings


def source_state(spark, ns: dict, rows: list):
    """The source table cleaned like the change events: (vehicle_id, row_hash) per listing."""
    schema = ", ".join(f"`{c}` {ns['CDC_RAW_TYPES'].get(c, 'STRING')}" for c in db.COLUMN_NAMES)
    raw_df = spark.createDataFrame([tuple(r[c] for c in db.COLUMN_NAMES) for r in rows], schema)
    cleaned = ns["clean_vehicle_listings"](
        raw_df.withColumn("source_file_path", F.lit("postgres")), ns["CDC_SOURCE_SYSTEM"], date.today().isoformat()
    )
    lookup = ns["read_delta_table"](ns["brand_lookup_path"](False)).select("model_normalized", "manufacturer")
    return ns["add_manufacturer"](cleaned, lookup).select(
        "vehicle_id", ns["content_hash"](ns["SCD2_BUSINESS_COLUMNS"]).alias("row_hash")
    )


def verify(spark, expected_sold: int, silver=settings.SILVER_DIR, gold=settings.GOLD_DIR) -> dict:
    ns = local_pipeline.namespace(spark, silver=silver, gold=gold)
    conn = db.connect()
    try:
        rows = db.fetch_rows(conn)
    finally:
        conn.close()

    history = ns["read_delta_table"](ns["cdc_silver_path"](False))
    counts = ns["check_scd2_versions"](history, "vehicle_listings_cdc")
    current = history.filter("is_current").select("vehicle_id", "row_hash")
    source = source_state(spark, ns, rows)
    only_in_source = source.exceptAll(current).count()
    only_in_silver = current.exceptAll(source).count()
    assert only_in_source == 0 and only_in_silver == 0, (
        f"Silver current state differs from the source: {only_in_source} rows only in PostgreSQL, "
        f"{only_in_silver} only in Silver"
    )

    dim = ns["read_delta_table"](ns["cdc_gold_path"]("dim_vehicle", False)).filter("vehicle_key <> -1")
    attributes = ["vehicle_id", *[c for c in ns["CURRENT_DIM_VEHICLE_COLUMNS"] if c not in ("vehicle_key", "vehicle_id")]]
    silver_attributes = history.filter("is_current").select("vehicle_id", *ns["vehicle_attributes"]()).select(*attributes)
    differing = silver_attributes.exceptAll(dim.select(*attributes)).count() + dim.select(*attributes).exceptAll(silver_attributes).count()
    assert differing == 0, f"dim_vehicle: {differing} rows differ from the current Silver versions"

    sold = history.join(history.filter("is_current").select("vehicle_id"), "vehicle_id", "left_anti") \
        .select("vehicle_id").distinct().count()
    assert sold == expected_sold, f"Silver: {sold} listings without a current version, the simulator deleted {expected_sold}"

    result = {
        "source_listings": len(rows),
        "silver_versions": counts["versions"],
        "silver_current": counts["current_versions"],
        "gold_current_vehicles": dim.count(),
        "sold_listings": sold,
    }
    print(f"Verified: Silver current state == PostgreSQL ({len(rows)} listings), SCD2 rules hold, Gold = current state")
    return result
