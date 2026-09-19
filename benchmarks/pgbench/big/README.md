# big: a dataset larger than RAM

`gen_big.py` loads a table of ~1 KiB rows; `big-update.sql` and
`big-select.sql` are the pgbench scripts (zipfian over `:rows`, pass
`-D rows=N`). One loader stream:

    python3 gen_big.py 1 1000000 --create | psql "$CONN" -X -q -v ON_ERROR_STOP=1

Several streams in parallel cover disjoint ranges; only the first
passes `--create`. Workload, three updates per select:

    pgbench -n -c 64 -j 16 -T 3600 -P 60 -D rows=500000000 \
      -f big-update.sql@3 -f big-select.sql@1 "$CONN"
