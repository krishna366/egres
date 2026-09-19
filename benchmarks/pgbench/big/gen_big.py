#!/usr/bin/env python3
"""Emit SQL that loads the `big` table: rows of about 1 KiB, so a few
hundred million of them make a dataset several times larger than RAM.

    python3 gen_big.py <first_id> <last_id> [--rows-per-stmt N] [--create]

prints one CREATE TABLE (with --create) and multi-row INSERTs covering
ids [first_id, last_id], to be piped through psql. Run several streams
over disjoint id ranges in parallel; ids are the primary key.

Payload: 1000 printable bytes from a fast per-row PRNG, seeded by the
id, so the content is reproducible and incompressible enough to defeat
block compression the way real text does not: keep the numbers honest
about disk, not about lz4.
"""
import sys, random, string

ALPHABET = (string.ascii_letters + string.digits).encode()

def payload(rid: int) -> str:
    r = random.Random(rid)
    return bytes(r.choices(ALPHABET, k=1000)).decode()

def main(argv):
    args = [a for a in argv if not a.startswith("--")]
    first, last = int(args[0]), int(args[1])
    rows_per_stmt = 200
    if "--rows-per-stmt" in argv:
        rows_per_stmt = int(argv[argv.index("--rows-per-stmt") + 1])
    out = sys.stdout
    if "--create" in argv:
        out.write("CREATE TABLE big (id BIGINT PRIMARY KEY, n BIGINT, payload TEXT);\n")
    rid = first
    while rid <= last:
        hi = min(rid + rows_per_stmt - 1, last)
        out.write("INSERT INTO big (id, n, payload) VALUES\n")
        out.write(",\n".join(f"({i}, 0, '{payload(i)}')" for i in range(rid, hi + 1)))
        out.write(";\n")
        rid = hi + 1

if __name__ == "__main__":
    main(sys.argv[1:])
