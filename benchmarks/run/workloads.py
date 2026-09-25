"""The workloads: each one's schema, row generator, load size, and
pgbench scripts, in one place.

    workloads.py <name> --ddl            the schema
    workloads.py <name> <first> <last>   multi-row INSERTs for those ids

Every row is derived from its id alone, so a row is reproducible and
parallel load streams need no coordination. The payloads are random
letters and digits, hard to compress on purpose: the disk numbers
should be about the rows, not about lz4.

A workload's pgbench scripts are strings here; `script_path` writes
one into the run's output dir when pgbench needs it, so a run keeps a
copy of exactly what it ran. `reads` are the read-only scripts the
readers run beside the load, `mix` the full workload, both with
weights. `:rows` is the count the load reached, `:run` the workload
run number and `:n` a per-client counter starting at 0, all passed by
pgbench.py.
"""

import argparse
import random
import string
import sys
from pathlib import Path

GIB = 2**30
ALPHABET = (string.ascii_letters + string.digits).encode()
ROWS_PER_STMT = 200


# ---------------------------------------------------------------------------
# big: 1 KiB rows past the page cache, point updates and reads
# ---------------------------------------------------------------------------

BIG_DDL = """\
CREATE TABLE big (id BIGINT PRIMARY KEY, n BIGINT, payload TEXT);
"""

BIG_INSERT = "INSERT INTO big (id, n, payload) VALUES"


def big_row(rid: int) -> str:
    r = random.Random(rid)
    payload = bytes(r.choices(ALPHABET, k=1000)).decode()
    return f"({rid}, 0, '{payload}')"


# A point update on a zipfian id: a few rows are very hot.
BIG_UPDATE = """\
\\set id random_zipfian(1, :rows, 1.1)
UPDATE big SET n = n + 1 WHERE id = :id;
"""

# A point read on the same distribution.
BIG_SELECT = """\
\\set id random_zipfian(1, :rows, 1.1)
SELECT n FROM big WHERE id = :id;
"""

BIG = {
    "ddl": BIG_DDL,
    "insert": BIG_INSERT,
    "row": big_row,
    "load": {"first": 1, "last": 700_000_000, "streams": 16,
             "chunk": 100_000, "stop_bytes": 600 * GIB},
    "vars": {},
    "scripts": {"update": BIG_UPDATE, "select": BIG_SELECT},
    "reads": [("select", 1)],
    "mix": [("update", 3), ("select", 1)],
}


# ---------------------------------------------------------------------------
# orders: a table with three secondary indexes
# ---------------------------------------------------------------------------
# Ours, not a standard: a plain OLTP table of the shape an application
# has. Every indexed value is random per row while `id` is sequential,
# so a load lands each insert at a random place in three indexes.
# Cardinalities: 8M customers (~100 orders each), 200k SKUs (~4,000
# each), 730 days (~1.1M each); the note is 600 random characters.

ORDERS_CUSTOMERS = 8_000_000
ORDERS_SKUS = 200_000
ORDERS_DAYS = 730

ORDERS_DDL = """\
CREATE TABLE orders (
  id BIGINT PRIMARY KEY,
  customer_id BIGINT,
  sku BIGINT,
  day INT,
  amount BIGINT,
  note TEXT
);
CREATE INDEX orders_customer ON orders (customer_id);
CREATE INDEX orders_sku ON orders (sku);
CREATE INDEX orders_day ON orders (day);
"""

ORDERS_INSERT = ("INSERT INTO orders (id, customer_id, sku, day, amount, "
                 "note) VALUES")


def orders_row(rid: int) -> str:
    r = random.Random(rid)
    customer = r.randrange(1, ORDERS_CUSTOMERS + 1)
    sku = r.randrange(1, ORDERS_SKUS + 1)
    day = r.randrange(1, ORDERS_DAYS + 1)
    amount = r.randrange(100, 100_000)
    note = bytes(r.choices(ALPHABET, k=600)).decode()
    return f"({rid}, {customer}, {sku}, {day}, {amount}, '{note}')"


# One customer's orders. Zipfian over customers, a few hot ones.
ORDERS_CUSTOMER = """\
\\set c random_zipfian(1, :customers, 1.05)
SELECT id, sku, day, amount FROM orders WHERE customer_id = :c LIMIT 100;
"""

# One SKU on one day: an index intersection, about five rows.
ORDERS_SKU_DAY = """\
\\set s random(1, :skus)
\\set d random(1, :days)
SELECT id, customer_id, amount FROM orders WHERE sku = :s AND day = :d;
"""

# How many orders a customer placed on a day.
ORDERS_CUSTOMER_DAY_COUNT = """\
\\set c random(1, :customers)
\\set d random(1, :days)
SELECT count(*) FROM orders WHERE customer_id = :c AND day = :d;
"""

# A new order: four index entries. Ids continue past the loaded range,
# a block of 100M per client and 10M of it per workload run, so no run
# repeats an id another inserted.
ORDERS_NEW = """\
\\set n :n + 1
\\set id :rows + (:client_id * 100000000) + (:run * 10000000) + :n
\\set c random(1, :customers)
\\set s random(1, :skus)
\\set d random(1, :days)
\\set a random(100, 100000)
INSERT INTO orders (id, customer_id, sku, day, amount, note)
  VALUES (:id, :c, :s, :d, :a,
          'new order note for the workload phase, shorter than a loaded one');
"""

# Move an order to another day: one index entry moves.
ORDERS_RESCHEDULE = """\
\\set id random(1, :rows)
\\set d random(1, :days)
UPDATE orders SET day = :d WHERE id = :id;
"""

# Update an unindexed column: no index entry moves.
ORDERS_AMOUNT = """\
\\set id random(1, :rows)
\\set a random(100, 100000)
UPDATE orders SET amount = :a WHERE id = :id;
"""

# Delete an order: four index entries removed. Repeats on an already
# deleted id are no-ops, as in any such workload.
ORDERS_DELETE = """\
\\set id random(1, :rows)
DELETE FROM orders WHERE id = :id;
"""

ORDERS = {
    "ddl": ORDERS_DDL,
    "insert": ORDERS_INSERT,
    "row": orders_row,
    "load": {"first": 1, "last": 700_000_000, "streams": 16,
             "chunk": 100_000, "stop_bytes": 500 * GIB},
    "vars": {"customers": ORDERS_CUSTOMERS, "skus": ORDERS_SKUS,
             "days": ORDERS_DAYS},
    "scripts": {
        "customer": ORDERS_CUSTOMER,
        "sku_day": ORDERS_SKU_DAY,
        "customer_day_count": ORDERS_CUSTOMER_DAY_COUNT,
        "insert": ORDERS_NEW,
        "reschedule": ORDERS_RESCHEDULE,
        "amount": ORDERS_AMOUNT,
        "delete": ORDERS_DELETE,
    },
    "reads": [("customer", 35), ("sku_day", 20), ("customer_day_count", 10)],
    "mix": [("customer", 35), ("sku_day", 20), ("customer_day_count", 10),
            ("insert", 25), ("reschedule", 5), ("amount", 3), ("delete", 2)],
}

WORKLOADS = {"big": BIG, "orders": ORDERS}


def script_path(run, script: str) -> Path:
    """The script's file in the run's output dir, written on first use."""
    d = run.out / "scripts"
    d.mkdir(parents=True, exist_ok=True)
    path = d / f"{script}.sql"
    if not path.exists():
        path.write_text(WORKLOADS[run.env["workload"]["name"]]["scripts"]
                        [script])
    return path


def emit(name: str, first: int, last: int, out=sys.stdout):
    """Multi-row INSERTs for ids [first, last], to pipe through psql."""
    w = WORKLOADS[name]
    rid = first
    while rid <= last:
        hi = min(rid + ROWS_PER_STMT - 1, last)
        out.write(w["insert"] + "\n")
        out.write(",\n".join(w["row"](i) for i in range(rid, hi + 1)))
        out.write(";\n")
        rid = hi + 1


def main():
    ap = argparse.ArgumentParser(
        description="Print a workload's schema, or INSERTs for a range of "
                    "ids to pipe through psql.",
        epilog="examples: workloads.py orders --ddl | psql \"$CONN\"; "
               "workloads.py big 1 100000 | psql \"$CONN\"")
    ap.add_argument("workload", choices=sorted(WORKLOADS),
                    help="which workload")
    ap.add_argument("--ddl", action="store_true",
                    help="print the CREATE TABLE and index statements")
    ap.add_argument("first", nargs="?", type=int,
                    help="first id of the rows to print")
    ap.add_argument("last", nargs="?", type=int,
                    help="last id, inclusive")
    a = ap.parse_args()
    if a.ddl:
        sys.stdout.write(WORKLOADS[a.workload]["ddl"])
    elif a.first is None or a.last is None:
        ap.error("give --ddl, or a first and last id")
    else:
        emit(a.workload, a.first, a.last)


if __name__ == "__main__":
    main()
