"""
Build the "original file" of the incremental load demo (documentation/incremental_load.md), once.

The Fourth source is shuffled (deterministic) and split into fifths; fifth k appears on day k
(`updated_at` = START_DATE + k - 1). From day 2 on, every day also brings changed versions of listings that were
published on an earlier day: a price drop (~3 %) or a worse condition / another title status (~0.5 %). A listing
changes at most once per day and there are no deletes.

`change_seq` numbers the records in the order they happen: it orders the changes of one listing (like the WAL
position in the CDC demo) and is the key Silver uses to skip records it already applied.

Usage:  .venv/bin/python -m incremental.prepare_original
"""

import json
import random
from datetime import timedelta

from cdc.load_initial import raw_row
from cdc.simulate_changes import drop_price, next_attribute
from incremental import settings


def split_into_days(rows: list, days: int) -> list:
    """`days` consecutive slices of (almost) equal size."""
    return [rows[round(i * len(rows) / days):round((i + 1) * len(rows) / days)] for i in range(days)]


def build_original(rows: list, days: int = settings.DAYS, seed: int = settings.SEED,
                   start_date=settings.START_DATE, rates: dict = settings.CHANGE_RATES) -> list:
    """All records of the original file in the order they happen, each with `updated_at` and `change_seq`."""
    rng = random.Random(seed)
    shuffled = list(rows)
    rng.shuffle(shuffled)

    records, published = [], {}  # published: id -> latest record of that listing
    for day_number, fifth in enumerate(split_into_days(shuffled, days)):
        day = (start_date + timedelta(days=day_number)).isoformat()
        # Changes of listings published on earlier days (chosen before today's new listings arrive).
        earlier = sorted(published)
        n_price = round(len(earlier) * rates["price_drop"])
        n_attribute = round(len(earlier) * rates["attribute_change"])
        chosen = rng.sample(earlier, min(len(earlier), n_price + n_attribute))
        for position, vehicle_id in enumerate(chosen):
            record = dict(published[vehicle_id])
            if position < n_price:
                record["price"], record["odometer"] = drop_price(record["price"], record["odometer"], rng)
            else:
                record.update(next_attribute(record))
            record["updated_at"] = day
            records.append(record)
            published[vehicle_id] = record
        for row in fifth:
            record = {**row, "updated_at": day}
            records.append(record)
            published[row["id"]] = record

    for change_seq, record in enumerate(records, start=1):
        record["change_seq"] = change_seq
    return records


def read_source_rows(path=settings.SOURCE_FILE) -> list:
    with open(path, encoding="utf-8") as source:
        return [raw_row(json.loads(line)) for line in source]


def write_original(records: list, path=settings.ORIGINAL_FILE) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as target:
        for record in records:
            target.write(json.dumps(record) + "\n")


def summarize(records: list) -> list:
    """Records per day: (day, new listings, changed listings)."""
    seen, summary = set(), {}
    for record in records:
        counts = summary.setdefault(record["updated_at"], [0, 0])
        if record["id"] in seen:
            counts[1] += 1
        else:
            counts[0] += 1
            seen.add(record["id"])
    return [(day, new, changed) for day, (new, changed) in sorted(summary.items())]


def main() -> list:
    records = build_original(read_source_rows())
    write_original(records)
    summary = summarize(records)
    for day, new, changed in summary:
        print(f"{day}: {new} new listings, {changed} changed listings")
    print(f"Original file: {len(records)} records -> {settings.ORIGINAL_FILE}")
    return summary


if __name__ == "__main__":
    main()
