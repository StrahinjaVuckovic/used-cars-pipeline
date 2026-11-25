"""
The users of the listing platform: one call = one simulated day of changes in the source database.

Per day (shares of the listings currently in the database):
- ~2 % new listings (INSERT), taken from the reserve (real listings kept aside by load_initial);
- ~3 % price drops of 3–15 % with a few more kilometres (UPDATE); a fifth of them drop the price twice the
  same day, to show that the order of changes matters;
- ~0.5 % changes of a vehicle attribute (condition one step worse, or title status) (UPDATE) – these create a
  new version of the vehicle in the Gold dimension, price changes do not;
- ~1 % sold vehicles (DELETE).

Every change is its own transaction, like separate user actions, so every change has its own commit time.
`plan_changes` only decides what happens (pure function, tested without a database); `apply_changes` runs it.

Usage:  .venv/bin/python -m cdc.simulate_changes --day 1
"""

import argparse
import json
import random

from cdc import db, settings

RATES = {
    "insert": 0.02,
    "price_drop": 0.03,
    "second_price_drop": 0.2,   # share of the price drops that happen twice the same day
    "attribute_change": 0.005,
    "delete": 0.01,
}
CONDITION_STEPS = ["new", "like new", "excellent", "good", "fair", "salvage"]


def next_attribute(row: dict) -> dict:
    """An attribute change: the condition one step worse when possible, otherwise the title status."""
    condition = (row.get("condition") or "").strip().lower()
    if condition in CONDITION_STEPS[:-1]:
        return {"condition": CONDITION_STEPS[CONDITION_STEPS.index(condition) + 1]}
    return {"title_status": "clean" if (row.get("title_status") or "").lower() == "rebuilt" else "rebuilt"}


def plan_changes(current: dict, reserve: list, day: int, seed: int = settings.SEED, rates: dict = RATES) -> list:
    """
    The changes of one day as a list of operations, in the order they happen:
    {"op": "insert", "row": {...}} | {"op": "update", "id": ..., "set": {...}} | {"op": "delete", "id": ...}.

    `current` maps the id of every listing in the database to its row (at least price, odometer, condition,
    title_status); `reserve` holds the listings not used yet. Deterministic for (seed, day).
    """
    rng = random.Random(f"{seed}-{day}")
    ids = sorted(current)
    n_delete = round(len(ids) * rates["delete"])
    n_price = round(len(ids) * rates["price_drop"])
    n_attribute = round(len(ids) * rates["attribute_change"])
    n_insert = min(len(reserve), round(len(ids) * rates["insert"]))

    # Every vehicle gets at most one kind of change per day.
    chosen = rng.sample(ids, min(len(ids), n_delete + n_price + n_attribute))
    deleted = chosen[:n_delete]
    price_dropped = chosen[n_delete:n_delete + n_price]
    attribute_changed = chosen[n_delete + n_price:]

    groups = []  # the operations of one vehicle, in their order
    for vehicle_id in price_dropped:
        price, odometer = current[vehicle_id].get("price"), current[vehicle_id].get("odometer")
        group = []
        for _ in range(2 if rng.random() < rates["second_price_drop"] else 1):
            if price is not None:
                price = float(round(price * (1 - rng.uniform(0.03, 0.15))))
            if odometer is not None:
                odometer = float(odometer + rng.randint(50, 1500))
            group.append({"op": "update", "id": vehicle_id, "set": {"price": price, "odometer": odometer}})
        groups.append(group)
    groups += [[{"op": "update", "id": v, "set": next_attribute(current[v])}] for v in attribute_changed]
    groups += [[{"op": "delete", "id": v}] for v in deleted]
    groups += [[{"op": "insert", "row": row}] for row in reserve[:n_insert]]

    # Interleave the vehicles like real users, keeping the order inside one vehicle.
    keyed = []
    for group in groups:
        keys = sorted(rng.random() for _ in group)
        keyed += list(zip(keys, range(len(keyed), len(keyed) + len(group)), group))
    return [operation for _, _, operation in sorted(keyed)]


def summarize(operations: list) -> dict:
    updates = [o for o in operations if o["op"] == "update"]
    return {
        "inserts": sum(o["op"] == "insert" for o in operations),
        "updates": len(updates),
        "updated_vehicles": len({o["id"] for o in updates}),
        "price_updates": sum("price" in o["set"] for o in updates),
        "attribute_updates": sum("price" not in o["set"] for o in updates),
        "deletes": sum(o["op"] == "delete" for o in operations),
        "deleted_ids": sorted(o["id"] for o in operations if o["op"] == "delete"),
    }


def apply_changes(conn, operations: list) -> None:
    """Run every operation as its own transaction (the connection is in autocommit mode)."""
    with conn.cursor() as cur:
        for operation in operations:
            if operation["op"] == "insert":
                db.insert_rows(conn, [operation["row"]])
            elif operation["op"] == "update":
                assignments = ", ".join(f"{db.quoted(c)} = %s" for c in operation["set"])
                cur.execute(
                    f"UPDATE {db.TABLE} SET {assignments} WHERE id = %s",
                    [*operation["set"].values(), operation["id"]],
                )
            else:
                cur.execute(f"DELETE FROM {db.TABLE} WHERE id = %s", (operation["id"],))


def read_state() -> dict:
    return json.loads(settings.STATE_FILE.read_text(encoding="utf-8"))


def read_reserve(offset: int) -> list:
    with open(settings.RESERVE_FILE, encoding="utf-8") as source:
        return [json.loads(line) for index, line in enumerate(source) if index >= offset]


def main(day: int, seed: int = settings.SEED) -> dict:
    state = read_state()
    conn = db.connect()
    try:
        current = {r["id"]: r for r in db.fetch_rows(conn, ["id", "price", "odometer", "condition", "title_status"])}
        operations = plan_changes(current, read_reserve(state["reserve_offset"]), day, seed)
        apply_changes(conn, operations)
        rows_after = db.row_count(conn)
    finally:
        conn.close()

    summary = {"day": day, **summarize(operations), "rows_after": rows_after}
    state["reserve_offset"] += summary["inserts"]
    state["days"].append(summary)
    settings.STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")
    print(
        f"Day {day}: {summary['inserts']} new, {summary['updates']} updates of {summary['updated_vehicles']} "
        f"listings ({summary['attribute_updates']} attribute changes), {summary['deletes']} sold; "
        f"{rows_after} listings in the source"
    )
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--day", type=int, required=True)
    parser.add_argument("--seed", type=int, default=settings.SEED)
    arguments = parser.parse_args()
    main(arguments.day, arguments.seed)
