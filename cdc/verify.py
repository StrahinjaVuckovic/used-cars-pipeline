"""
End-to-end check of the CDC demo after a run: the lakehouse must hold exactly the state of the source database.

- Silver: the current versions (`WHERE is_current`) equal the PostgreSQL table, row by row, after the same
  cleaning (same ids, same hash of the business columns); the SCD2 rules hold (one current version per listing,
  no overlapping periods).
- Gold: the current versions of dim_vehicle have the current attributes of Silver; one Sold event per DELETE the
  simulator made; every event is linked to a vehicle version.
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
        "vehicle_id", ns["content_hash"](ns["CDC_BUSINESS_COLUMNS"]).alias("row_hash")
    )


def verify(spark, expected_sold: int, silver=settings.SILVER_DIR, gold=settings.GOLD_DIR) -> dict:
    ns = local_pipeline.namespace(spark, silver=silver, gold=gold)
    conn = db.connect()
    try:
        rows = db.fetch_rows(conn)
    finally:
        conn.close()

    history = ns["read_delta_table"](ns["cdc_silver_path"](False))
    counts = ns["check_cdc_versions"](history, "vehicle_listings_cdc")
    current = history.filter("is_current").select("vehicle_id", "row_hash")
    source = source_state(spark, ns, rows)
    only_in_source = source.exceptAll(current).count()
    only_in_silver = current.exceptAll(source).count()
    assert only_in_source == 0 and only_in_silver == 0, (
        f"Silver current state differs from the source: {only_in_source} rows only in PostgreSQL, "
        f"{only_in_silver} only in Silver"
    )

    dim = ns["read_delta_table"](ns["cdc_gold_path"]("dim_vehicle", False)).filter("vehicle_version_key <> -1")
    fact = ns["read_delta_table"](ns["cdc_gold_path"]("fact_listing_event", False))
    attributes = ns["CDC_DIM_ATTRIBUTES"]
    silver_attributes = history.filter("is_current").select(
        "vehicle_id", *ns["vehicle_attributes"]()
    ).select("vehicle_id", ns["content_hash"](attributes).alias("h"))
    gold_attributes = dim.filter("is_current").select("vehicle_id", ns["content_hash"](attributes).alias("h"))
    differing = silver_attributes.exceptAll(gold_attributes).count() + gold_attributes.exceptAll(silver_attributes).count()
    assert differing == 0, f"dim_vehicle: {differing} current versions differ from the Silver attributes"

    sold = fact.filter("event_type = 'Sold'").count()
    assert sold == expected_sold, f"fact_listing_event: {sold} Sold events, the simulator deleted {expected_sold}"
    assert fact.filter("vehicle_version_key = -1").count() == 0, "fact_listing_event: events without a version"

    result = {
        "source_listings": len(rows),
        "silver_versions": counts["versions"],
        "silver_current": counts["current_versions"],
        "gold_vehicle_versions": dim.count(),
        "gold_vehicles_with_several_versions": dim.groupBy("vehicle_id").count().filter("count > 1").count(),
        "gold_events": dict(fact.groupBy("event_type").count().collect()),
    }
    print(f"Verified: Silver current state == PostgreSQL ({len(rows)} listings), SCD2 rules hold, Gold consistent")
    return result
