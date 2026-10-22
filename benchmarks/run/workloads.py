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
pgbench: each client draws its next script by weight, and latency is
reported per script. Queries in one group share its clients, so a
slow script lowers the rate of a fast one; a second group keeps them
apart.

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
# orders: a table with four secondary indexes
# ---------------------------------------------------------------------------
# Ours, not a standard: a plain OLTP table of the shape an application
# has. Every indexed value is random per row while `id` is sequential,
# so a load lands each insert at a random place in four indexes.
# Cardinalities: 8M customers (~75 orders each), 200k SKUs (~3,000
# each), 730 days (~820k each), 99,900 amounts (~6,000 each); the
# note is 1000 blended characters,
# which compress 2x under zstd.

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
CREATE INDEX orders_amount ON orders (amount);
"""

ORDERS_INSERT = ("INSERT INTO orders (id, customer_id, sku, day, amount, "
                 "note) VALUES")


def orders_row(rid: int) -> str:
    r = random.Random(rid)
    customer = r.randrange(1, ORDERS_CUSTOMERS + 1)
    sku = r.randrange(1, ORDERS_SKUS + 1)
    day = r.randrange(1, ORDERS_DAYS + 1)
    amount = r.randrange(100, 100_000)
    note = blended(r, 1000)
    return f"({rid}, {customer}, {sku}, {day}, {amount}, '{note}')"


# The id of a row that exists, scrambled zipfian as in YCSB: a zipfian
# rank over the whole table, scattered by `permute` so the hot rows are
# spread over every slice, old and new, and not the first ones loaded.
# The seed is fixed, so a rank is the same row in every pgbench of a
# run. While that row is not loaded yet, `% :per` folds the rank onto
# a loaded row of the same slice. 1.01 is the nearest pgbench has to
# YCSB's 0.99: about 80% of draws go to 1% of the rows.
ORDERS_ID = """\
\\set total :streams * :slice
\\set r random_zipfian(1, :total, 1.01)
\\set p permute(:r - 1, :total, 7)
\\set s :p % :streams
\\set k (:p / :streams) % :per
\\set id :first + :s * :slice + :k
"""

# One order by its id: the primary key index, then the row.
ORDERS_SELECT = ORDERS_ID + """\
SELECT amount FROM orders WHERE id = :id;
"""

# One customer's orders. Zipfian over customers, a few hot ones.
ORDERS_CUSTOMER = """\
\\set c random_zipfian(1, :customers, 1.05)
SELECT id, sku, day, amount FROM orders WHERE customer_id = :c LIMIT 100;
"""

# One SKU on one day: an index intersection, about four rows.
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

# A new order: five index entries. Ids continue past the loaded range,
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

# Change an order's amount: one index entry moves.
ORDERS_AMOUNT = """\
\\set id random(1, :rows)
\\set a random(100, 100000)
UPDATE orders SET amount = :a WHERE id = :id;
"""

# Delete an order: five index entries removed. Repeats on an already
# deleted id are no-ops, as in any such workload.
ORDERS_DELETE = """\
\\set id random(1, :rows)
DELETE FROM orders WHERE id = :id;
"""

ORDERS = {
    "ddl": ORDERS_DDL,
    "insert": ORDERS_INSERT,
    "row": orders_row,
    # Whether a row is loaded: a killed load resumes from the answers.
    "exists": "SELECT id FROM orders WHERE id = {id};",
    # By rows, not by bytes on disk: the same rows for either server.
    # `create` false loads a further id range into the table as it is.
    "load": {"first": 1, "last": 600_000_000, "streams": 16,
             "chunk": 100_000, "stop_bytes": None, "stop_secs": None,
             "create": True, "after_secs": 1800},
    "vars": {"customers": ORDERS_CUSTOMERS, "skus": ORDERS_SKUS,
             "days": ORDERS_DAYS},
    "scripts": {
        "select": ORDERS_SELECT,
        "customer": ORDERS_CUSTOMER,
        "sku_day": ORDERS_SKU_DAY,
        "customer_day_count": ORDERS_CUSTOMER_DAY_COUNT,
        "insert": ORDERS_NEW,
        "reschedule": ORDERS_RESCHEDULE,
        "amount": ORDERS_AMOUNT,
        "delete": ORDERS_DELETE,
    },
    # Beside the load: each client rotates between a point read by id
    # and the index queries, half its transactions each. Latency is
    # reported per script.
    "reads": [
        {"name": "reads", "clients": 8, "sample": 0.1,
         "scripts": [("select", 65), ("customer", 35), ("sku_day", 20),
                     ("customer_day_count", 10)]},
    ],
    "mix": [("customer", 35), ("sku_day", 20), ("customer_day_count", 10),
            ("insert", 25), ("reschedule", 5), ("amount", 3), ("delete", 2)],
}

# ---------------------------------------------------------------------------
# bank: pgbench's accounts, tellers and branches, for the long run
# ---------------------------------------------------------------------------
# The TPC-B-like transaction pgbench ships, on tables of its shape,
# with two changes. The branch and the teller are the account's own,
# not drawn apart from it, so for every branch the accounts, the
# tellers and the branch row sum to the same number, and so does the
# whole bank. And every 100 accounts have a group row that takes the
# same delta: a group's accounts and its row are 101 point reads, so
# benchmarks/longrun checks groups in one snapshot all the time,
# where a scan of the accounts takes hours. history carries a bucket
# column so old rows can be deleted by equality (the only predicate
# the server takes today).

BANK_ACCOUNTS_PER_BRANCH = 100_000
BANK_TELLERS_PER_BRANCH = 10
BANK_ACCOUNTS_PER_GROUP = 100
BANK_FILLER = 500

BANK_DDL = """\
CREATE TABLE pgbench_accounts (
  aid BIGINT PRIMARY KEY,
  bid BIGINT,
  abalance BIGINT,
  filler TEXT
);
CREATE TABLE pgbench_branches (
  bid BIGINT PRIMARY KEY,
  bbalance BIGINT
);
CREATE TABLE pgbench_groups (
  gid BIGINT PRIMARY KEY,
  gbalance BIGINT
);
CREATE TABLE pgbench_tellers (
  tid BIGINT PRIMARY KEY,
  bid BIGINT,
  tbalance BIGINT
);
CREATE TABLE pgbench_history (
  tid BIGINT,
  bid BIGINT,
  aid BIGINT,
  delta BIGINT,
  mtime TIMESTAMP,
  bucket BIGINT
);
CREATE INDEX pgbench_history_bucket ON pgbench_history (bucket);
"""

BANK_INSERT = "INSERT INTO pgbench_accounts (aid, bid, abalance, filler) VALUES"


def bank_row(aid: int) -> str:
    r = random.Random(aid)
    bid = (aid - 1) // BANK_ACCOUNTS_PER_BRANCH + 1
    return f"({aid}, {bid}, 0, '{blended(r, BANK_FILLER)}')"


def bank_branches(accounts: int) -> int:
    return (accounts + BANK_ACCOUNTS_PER_BRANCH - 1) // \
        BANK_ACCOUNTS_PER_BRANCH


def bank_groups(accounts: int) -> int:
    return (accounts + BANK_ACCOUNTS_PER_GROUP - 1) // \
        BANK_ACCOUNTS_PER_GROUP


# `:run` is the pgbench start count and `:n` this client's transaction
# count, so a bucket never repeats across starts; the long run deletes
# a start's buckets once it is two starts old.
BANK_TPCB = """\
\\set aid random(1, :accounts)
\\set gid (:aid - 1) / :accounts_per_group + 1
\\set bid (:aid - 1) / :accounts_per_branch + 1
\\set tid (:bid - 1) * :tellers_per_branch + random(1, :tellers_per_branch)
\\set delta random(-5000, 5000)
\\set n :n + 1
\\set bucket :run * 100000 + :n / 10000
BEGIN;
UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;
SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
UPDATE pgbench_groups SET gbalance = gbalance + :delta WHERE gid = :gid;
UPDATE pgbench_tellers SET tbalance = tbalance + :delta WHERE tid = :tid;
UPDATE pgbench_branches SET bbalance = bbalance + :delta WHERE bid = :bid;
INSERT INTO pgbench_history (tid, bid, aid, delta, mtime, bucket)
  VALUES (:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP, :bucket);
END;
"""

BANK_READ = """\
\\set aid random(1, :accounts)
SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
"""

BANK = {
    "ddl": BANK_DDL,
    "insert": BANK_INSERT,
    "row": bank_row,
    "exists": "SELECT aid FROM pgbench_accounts WHERE aid = {id};",
    # The load is the accounts only; longrun adds the groups, branches
    # and tellers for the count it loaded. load.last is the scale:
    # about 600 bytes a row.
    "load": {"first": 1, "last": 10_000_000, "streams": 8,
             "chunk": 50_000, "stop_bytes": None, "stop_secs": None,
             "create": True, "after_secs": 0},
    "vars": {"accounts_per_branch": BANK_ACCOUNTS_PER_BRANCH,
             "tellers_per_branch": BANK_TELLERS_PER_BRANCH,
             "accounts_per_group": BANK_ACCOUNTS_PER_GROUP},
    "scripts": {"tpcb": BANK_TPCB, "read": BANK_READ},
    "reads": [],
    "mix": [("tpcb", 90), ("read", 10)],
}

WORKLOADS = {"orders": ORDERS, "bank": BANK}


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
               "workloads.py orders 1 100000 | psql \"$CONN\"")
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
