# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

"""The workloads: each one's schema, row generator, load size, and
pgbench scripts, in one place.

    workloads.py <name> --ddl            the schema
    workloads.py <name> <first> <last>   multi-row INSERTs for those ids
    workloads.py <name> --ratio          how well its rows compress

Every row is derived from its id alone, so a row is reproducible and
parallel load streams need no coordination.

How well the rows compress is part of a workload, and stated: a
server that compresses its blocks writes, compacts and scans fewer
bytes for it, which is a real difference on real data and none on
random bytes. `blended` is a payload that compresses about as
production rows do; `text` is one that does not.

A workload's pgbench scripts are strings here; `script_path` writes
one into the run's output dir when pgbench needs it, so a run keeps a
copy of exactly what it ran. `mix` is the full workload, scripts with
weights. `reads` are the reader groups that run beside the load: each
a name, a client count, its read-only scripts with weights, and
optionally `sample`, the fraction of its transactions whose latency is
logged (fast scripts would otherwise log gigabytes). A group is one
pgbench, so queries that differ by orders of magnitude get a group
each and neither starves the other of clients.

pgbench.py passes the variables: `:rows` is the count the load
reached, `:run` the workload run number, `:n` a per-client counter
starting at 0, and `:first`, `:streams`, `:slice`, `:per` say which
ids exist while the load is still running (`pgbench.id_vars`).
"""

import argparse
import base64
import itertools
import random
import struct
import sys
from pathlib import Path

GIB = 2**30
# Payload text: random bytes from the row's own generator, base64'd.
# Six bits of entropy per character, the same on either server, so
# on-disk sizes compare; and 8 us a row against 75 for choosing each
# character, which is what kept 16 load streams to 100k rows/s.
def text(r: random.Random, chars: int) -> str:
    return base64.b64encode(r.randbytes(chars * 3 // 4)).decode()
ROWS_PER_STMT = 200

# A blended payload: part random, part words, as a production row is
# part ids, hashes and tokens, which do not compress, and part names,
# enumerated values and prose, which do. The two dials are how much is
# random and how many words there are to choose from; fewer words or
# less randomness compress better.
#
# Set so that rows compress 2x under zstd, the conservative end of what
# production tables report (1.5x under lz4, 2x to 3x under zstd), and
# not the 3x and up a plain word list gives. Measured on 64 KiB blocks
# of rows as below (`--ratio` for zstd; lz4 with the vendored library):
# zstd level 1 2.01x, lz4 1.51x. Words alone from this vocabulary are
# 2.8x and 1.9x; a 2,000-word one, 3.6x and 2.4x.
BLEND_RANDOM = 0.24
BLEND_WORDS = 20_000
# The words are drawn once into a text the payloads take slices of: a
# slice costs a microsecond where drawing a row's words would cost
# fifty. Far larger than a block, so slices in one block rarely overlap
# and the ratio comes from the words, not from repeated slices.
BLEND_TEXT_BYTES = 2 << 20
_blend_text = None


def blend_text() -> str:
    """Words of a made-up vocabulary, the common ones far more often
    (frequency 1/rank), separated by spaces. The same text every time:
    its generators are seeded."""
    global _blend_text
    if _blend_text is None:
        r = random.Random(7)
        cons, vowels = "bcdfghjklmnprstvwz", "aeiou"
        syllables = [c + v for c in cons for v in vowels] + \
                    [c + v + d for c in cons for v in vowels for d in "nrst"]
        words = set()
        while len(words) < BLEND_WORDS:
            words.add("".join(r.choice(syllables)
                              for _ in range(r.randint(1, 4))))
        words = sorted(words)
        r.shuffle(words)
        cum = list(itertools.accumulate(1 / (rank + 1)
                                        for rank in range(BLEND_WORDS)))
        out, size = [], 0
        while size < BLEND_TEXT_BYTES:
            batch = r.choices(words, cum_weights=cum, k=4096)
            out += batch
            size += sum(len(w) + 1 for w in batch)
        _blend_text = " ".join(out)[:BLEND_TEXT_BYTES]
    return _blend_text


def blended(r: random.Random, chars: int) -> str:
    """`chars` characters: the random part, then a slice of the text."""
    random_chars = int(chars * BLEND_RANDOM) // 4 * 4
    words = blend_text()
    off = r.randrange(len(words) - chars)
    return text(r, random_chars) + words[off:off + chars - random_chars]


# ---------------------------------------------------------------------------
# big: 1 KiB rows past the page cache, point updates, reads and scans.
# The payload is blended: rows that compress 2x under zstd.
# ---------------------------------------------------------------------------

# `g` is what the scans filter on: BIG_GROUPS values, each on about one
# row in BIG_GROUPS wherever it is in the table, and no index, so
# `WHERE g = v` reads every row whichever value is asked for.
BIG_GROUPS = 1000

BIG_DDL = """\
CREATE TABLE big (id BIGINT PRIMARY KEY, n BIGINT, g BIGINT, payload TEXT);
"""

BIG_INSERT = "INSERT INTO big (id, n, g, payload) VALUES"


def big_row(rid: int) -> str:
    r = random.Random(rid)
    payload = blended(r, 1000)
    g = r.randrange(1, BIG_GROUPS + 1)
    return f"({rid}, 0, {g}, '{payload}')"


# A point update on a zipfian id: a few rows are very hot.
BIG_UPDATE = """\
\\set id random_zipfian(1, :rows, 1.1)
UPDATE big SET n = n + 1 WHERE id = :id;
"""

# A point read of a row that exists: a slice of the load, then a
# zipfian offset into what that slice has loaded, so each slice has a
# few very hot rows and a long cold tail.
BIG_SELECT = """\
\\set s random(0, :streams - 1)
\\set k random_zipfian(1, :per, 1.1)
\\set id :first + :s * :slice + :k - 1
SELECT n FROM big WHERE id = :id;
"""

# A scan: no index on g, so every row is read to count the one in
# BIG_GROUPS that matches. Its latency grows with the table.
BIG_SCAN = """\
\\set g random(1, :groups)
SELECT count(*) FROM big WHERE g = :g;
"""

BIG = {
    "ddl": BIG_DDL,
    "insert": BIG_INSERT,
    "row": big_row,
    # By rows, not by bytes on disk: the same rows for either server.
    "load": {"first": 1, "last": 600_000_000, "streams": 16,
             "chunk": 100_000, "stop_bytes": None, "stop_secs": None},
    "vars": {"groups": BIG_GROUPS},
    "scripts": {"update": BIG_UPDATE, "select": BIG_SELECT,
                "scan": BIG_SCAN},
    "reads": [
        {"name": "point", "clients": 8, "scripts": [("select", 1)],
         "sample": 0.01},
        {"name": "scan", "clients": 1, "scripts": [("scan", 1)]},
    ],
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
    note = text(r, 600)
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
             "chunk": 100_000, "stop_bytes": 500 * GIB,
             "stop_secs": None},
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
    "reads": [
        {"name": "reads", "clients": 8, "sample": 0.1,
         "scripts": [("customer", 35), ("sku_day", 20),
                     ("customer_day_count", 10)]},
    ],
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


def ratio(name: str, rows: int = 16_000):
    """Print how well the workload's rows compress under zstd level 1,
    in 64 KiB blocks as a block-compressing server would see them: a
    row here is its id as a key, then its SQL tuple."""
    from compression import zstd
    w = WORKLOADS[name]
    blob = b"".join(struct.pack(">Q", rid) + w["row"](rid).encode()
                    for rid in range(1, rows + 1))
    block = 64 << 10
    packed = sum(len(zstd.compress(blob[off:off + block], level=1))
                 for off in range(0, len(blob), block))
    print(f"{name}: {rows} rows, {len(blob) / rows:.0f} bytes each, "
          f"zstd level 1 in 64 KiB blocks: {len(blob) / packed:.2f}x")


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
    ap.add_argument("--ratio", action="store_true",
                    help="print how well the rows compress under zstd")
    ap.add_argument("first", nargs="?", type=int,
                    help="first id of the rows to print")
    ap.add_argument("last", nargs="?", type=int,
                    help="last id, inclusive")
    a = ap.parse_args()
    if a.ddl:
        sys.stdout.write(WORKLOADS[a.workload]["ddl"])
    elif a.ratio:
        ratio(a.workload)
    elif a.first is None or a.last is None:
        ap.error("give --ddl, --ratio, or a first and last id")
    else:
        emit(a.workload, a.first, a.last)


if __name__ == "__main__":
    main()
