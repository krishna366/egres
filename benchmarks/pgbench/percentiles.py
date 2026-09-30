#!/usr/bin/env python3
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

"""Print p50/p99/p99.9 transaction latency from pgbench -l logs.

Usage: percentiles.py <log-prefix>

pgbench -l --log-prefix=P writes P.<pid> for the first thread and
P.<pid>.<n> for the rest, so every file matching P.* belongs to one
run.  A line is:

    client_id transaction_no time script_no time_epoch time_us \
[schedule_lag [retries]]

`time` is the transaction latency in microseconds.  A transaction that
failed logs the literal `failed` (or `serialization`/`deadlock`) in
that field; those lines carry no latency and are skipped.

Prints `p50_ms p99_ms p999_ms n` on one line.  n is the number of
latency samples found, and is 0 when the run logged nothing.
"""
import glob
import math
import sys


def nearest_rank(sorted_us, pct):
    """Nearest-rank percentile: the ceil(n*pct/100)-th smallest value."""
    n = len(sorted_us)
    rank = math.ceil(n * pct / 100.0)
    if rank < 1:
        rank = 1
    if rank > n:
        rank = n
    return sorted_us[rank - 1]


def main():
    if len(sys.argv) != 2:
        sys.stderr.write("usage: percentiles.py <log-prefix>\n")
        return 2
    prefix = sys.argv[1]
    lat_us = []
    for path in sorted(glob.glob(prefix + ".*")):
        with open(path, "r", errors="replace") as fh:
            for line in fh:
                parts = line.split()
                if len(parts) < 3:
                    continue
                try:
                    lat_us.append(int(parts[2]))
                except ValueError:
                    # Failed transaction, or a `progress:` style line.
                    continue
    if not lat_us:
        print("0.000 0.000 0.000 0")
        return 0
    lat_us.sort()
    p50 = nearest_rank(lat_us, 50.0) / 1000.0
    p99 = nearest_rank(lat_us, 99.0) / 1000.0
    p999 = nearest_rank(lat_us, 99.9) / 1000.0
    print("%.3f %.3f %.3f %d" % (p50, p99, p999, len(lat_us)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
