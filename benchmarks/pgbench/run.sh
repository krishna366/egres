#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# One runner for both Egres and Postgres pgbench numbers.  Every
# measured run is preceded by a warmup of the same shape, so cache
# fill and connection setup stay out of the measurement, and one TSV
# row per measured run is appended to <out>/results.tsv.
#
# A run that fails is recorded with an empty tps and the first error
# line in `notes`; the rest of the matrix still runs.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

LABEL=""
CONN=""
INIT="none"
SCALE=100
DURATION=60
WARMUP=10
CLIENTS="1 8 32 64"
MODES="simple"
WORKLOADS="select-only tpcb-like hot-update"
OUT="$SCRIPT_DIR/results"
PGBIN="${PGBIN:-/home/mrjn/.local/opt/pg18/bin}"
ADMIN=""

usage() {
  cat <<'EOF'
usage: run.sh --label <name> --conn "<conninfo>" [options]

  --label NAME       Name for the system under test (required).
  --conn CONNINFO    libpq conninfo string (required).  For Egres:
                     host=127.0.0.1 port=3210 dbname=egres
                     sslmode=disable
  --init MODE        egres | postgres | none        (default none)
  --scale N          pgbench scale factor           (default 100)
  --duration S       seconds per measured run       (default 60)
  --warmup S         warmup seconds, 0 skips it     (default 10)
  --clients "LIST"   client counts          (default "1 8 32 64")
  --modes "LIST"     simple | extended | prepared   (default simple)
  --workloads "LIST" select-only tpcb-like hot-update
  --out DIR          results directory
  --pgbin DIR        directory holding pgbench and psql
  --admin HOST:PORT  Egres admin port (SQL port + 2000).  When set,
                     every measured run waits until the LSM is quiet:
                     no L0 tables, no immutable memtable, no flush or
                     compaction in flight.  Reads then merge L1+ only.
EOF
}

die() {
  echo "run.sh: $*" >&2
  exit 1
}

while [ $# -gt 0 ]; do
  case "$1" in
    --label) LABEL="${2:-}"; shift 2 ;;
    --conn) CONN="${2:-}"; shift 2 ;;
    --init) INIT="${2:-}"; shift 2 ;;
    --scale) SCALE="${2:-}"; shift 2 ;;
    --duration) DURATION="${2:-}"; shift 2 ;;
    --warmup) WARMUP="${2:-}"; shift 2 ;;
    --clients) CLIENTS="${2:-}"; shift 2 ;;
    --modes) MODES="${2:-}"; shift 2 ;;
    --workloads) WORKLOADS="${2:-}"; shift 2 ;;
    --out) OUT="${2:-}"; shift 2 ;;
    --pgbin) PGBIN="${2:-}"; shift 2 ;;
    --admin) ADMIN="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; die "unknown argument: $1" ;;
  esac
done

[ -n "$LABEL" ] || { usage >&2; die "--label is required"; }
[ -n "$CONN" ] || { usage >&2; die "--conn is required"; }

case "$INIT" in
  egres|postgres|none) ;;
  *) die "--init must be egres, postgres or none" ;;
esac

PGBENCH="$PGBIN/pgbench"
PSQL="$PGBIN/psql"
[ -x "$PGBENCH" ] || die "no pgbench at $PGBENCH (set --pgbin)"
[ -x "$PSQL" ] || PSQL="psql"

# Validate the whole matrix before spending time on any of it.
for w in $WORKLOADS; do
  case "$w" in
    select-only|tpcb-like|hot-update) ;;
    *) die "unknown workload: $w" ;;
  esac
done
for m in $MODES; do
  case "$m" in
    simple|extended|prepared) ;;
    *) die "unknown query mode: $m" ;;
  esac
done
for c in $CLIENTS; do
  case "$c" in
    ''|*[!0-9]*) die "client count must be a positive integer: $c" ;;
  esac
  [ "$c" -ge 1 ] || die "client count must be >= 1: $c"
done

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

LOGDIR="$OUT/$LABEL"
mkdir -p "$LOGDIR"
TSV="$OUT/results.tsv"
HEADER=$'date_utc\tlabel\tworkload\tmode\tclients\tjobs\tscale'
HEADER+=$'\tduration_s\ttps\tlat_avg_ms\tlat_stddev_ms\tp50_ms'
HEADER+=$'\tp99_ms\tp999_ms\ttxns\tfailed\tnotes'
[ -s "$TSV" ] || printf '%s\n' "$HEADER" > "$TSV"

# Per-invocation copy of the rows, printed as a table at the end.
RUNTSV="$TMP/invocation.tsv"
printf '%s\n' "$HEADER" > "$RUNTSV"

# Wait until the Egres LSM has nothing left to flush or compact, so a
# read run measures the steady shape (L1 and below) and not a merge
# across L0 tables left behind by the previous write run.  Polls the
# admin port's `zdb forest` dump; gives up after 10 minutes.
wait_quiet() {
  [ -n "$ADMIN" ] || return 0
  local host="${ADMIN%%:*}" port="${ADMIN##*:}" i dump
  for i in $(seq 1 600); do
    dump="$(python3 - "$host" "$port" <<'PY' || true
import socket, sys
s = socket.create_connection((sys.argv[1], int(sys.argv[2])), timeout=5)
s.sendall(b"zdb forest\n")
out = b""
try:
    while True:
        b = s.recv(65536)
        if not b:
            break
        out += b
        # The admin server keeps the connection open after the dump;
        # a short quiet gap after the first bytes marks its end.
        s.settimeout(0.3)
except socket.timeout:
    pass
print(out.decode(errors="replace"))
PY
)"
    if ! grep -q 'immutable=[1-9]\|flush_pending: [1-9]\|^ *L0:' <<<"$dump" \
       && grep -q 'in-flight: (none)' <<<"$dump"; then
      [ "$i" -eq 1 ] || echo "lsm quiet after ${i}s"
      return 0
    fi
    sleep 1
  done
  echo "lsm still busy after 600s; measuring anyway" >&2
}

# Collapse a free-text message into something safe for one TSV cell.
clean() {
  printf '%s' "$1" | tr '\t\n' '  ' | cut -c1-160
}

# First line of a pgbench log that looks like a failure report.
first_error() {
  grep -m1 -E 'pgbench:|ERROR|FATAL|could not' "$1" 2>/dev/null || true
}

# Pull the first number out of a matching pgbench summary line.
grab_num() {
  grep -m1 -E "$2" "$1" 2>/dev/null \
    | grep -o -E '[0-9]+\.?[0-9]*' | head -1 || true
}

init_db() {
  local rows started finished elapsed rate
  rows=$(( SCALE + 10 * SCALE + 100000 * SCALE ))
  started="$(date +%s.%N)"
  case "$INIT" in
    postgres)
      "$PGBENCH" -i -s "$SCALE" "$CONN"
      ;;
    egres)
      # Egres cannot run `pgbench -i`, so the data comes from
      # generated SQL piped through psql.  ON_ERROR_STOP turns a
      # partial load into a hard failure instead of a short table.
      python3 "$SCRIPT_DIR/init_egres.py" "$SCALE" \
        | "$PSQL" "$CONN" -X -q -v ON_ERROR_STOP=1
      ;;
  esac
  finished="$(date +%s.%N)"
  elapsed="$(awk -v a="$started" -v b="$finished" \
    'BEGIN { printf "%.2f", b - a }')"
  rate="$(awk -v r="$rows" -v e="$elapsed" \
    'BEGIN { printf "%.0f", (e > 0 ? r / e : 0) }')"
  {
    echo "label:      $LABEL"
    echo "init:       $INIT"
    echo "scale:      $SCALE"
    echo "rows:       $rows"
    echo "seconds:    $elapsed"
    echo "rows_per_s: $rate"
    echo "date_utc:   $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  } > "$OUT/$LABEL-init.txt"
  echo "init $INIT scale $SCALE: $rows rows in ${elapsed}s, $rate rows/s"
}

[ "$INIT" = "none" ] || init_db

# Workload name -> pgbench arguments.  Custom-file runs need -s so
# that :scale is right; built-ins read the scale out of
# pgbench_branches and reject a redundant -s.
workload_args() {
  case "$1" in
    select-only) WL_ARGS=(-b select-only) ;;
    tpcb-like) WL_ARGS=(-b tpcb-like) ;;
    hot-update)
      WL_ARGS=(-f "$SCRIPT_DIR/hot-update.sql" -s "$SCALE") ;;
  esac
}

for workload in $WORKLOADS; do
  workload_args "$workload"
  for mode in $MODES; do
    for clients in $CLIENTS; do
      jobs="$clients"
      [ "$jobs" -le 16 ] || jobs=16
      base="$workload-$mode-c$clients"
      outlog="$LOGDIR/$base.log"
      warmlog="$LOGDIR/$base.warmup.log"
      lprefix="$TMP/$base/log"
      mkdir -p "$TMP/$base"
      notes=""

      if [ "$WARMUP" -gt 0 ]; then
        echo "warmup  $LABEL $base (${WARMUP}s)"
        rc=0
        "$PGBENCH" "$CONN" -n -M "$mode" -c "$clients" -j "$jobs" \
          -T "$WARMUP" -P 10 "${WL_ARGS[@]}" \
          > "$warmlog" 2>&1 || rc=$?
        [ "$rc" -eq 0 ] || notes="warmup rc=$rc; "
      fi

      wait_quiet
      echo "measure $LABEL $base (${DURATION}s, j=$jobs)"
      rc=0
      "$PGBENCH" "$CONN" -n -M "$mode" -c "$clients" -j "$jobs" \
        -T "$DURATION" -P 10 -l --sampling-rate=0.1 \
        --log-prefix="$lprefix" "${WL_ARGS[@]}" \
        > "$outlog" 2>&1 || rc=$?

      tps=""; lat_avg=""; lat_sd=""; txns=""; failed=""
      p50=""; p99=""; p999=""; nsamp=0

      if [ "$rc" -eq 0 ]; then
        tps="$(grep -m1 '^tps = ' "$outlog" \
          | sed -E 's/^tps = ([0-9.]+).*/\1/' || true)"
        lat_avg="$(grab_num "$outlog" '^latency average =')"
        lat_sd="$(grab_num "$outlog" '^latency stddev =')"
        txns="$(grab_num "$outlog" \
          '^number of transactions actually processed:')"
        failed="$(grab_num "$outlog" \
          '^number of failed transactions:')"
        read -r p50 p99 p999 nsamp <<<"$(python3 \
          "$SCRIPT_DIR/percentiles.py" "$lprefix")"
        if [ "${nsamp:-0}" -eq 0 ]; then
          p50=""; p99=""; p999=""
          notes="${notes}no -l samples; "
        fi
      else
        notes="${notes}rc=$rc: $(first_error "$outlog")"
      fi

      row="$(printf '%s\t' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$LABEL" \
        "$workload" "$mode" "$clients" "$jobs" "$SCALE" "$DURATION" \
        "$tps" "$lat_avg" "$lat_sd" "$p50" "$p99" "$p999" "$txns" \
        "$failed")"
      row="$row$(clean "$notes")"
      printf '%s\n' "$row" | tee -a "$TSV" >> "$RUNTSV"

      # Only this run's -l files, and only inside the temp dir.
      rm -rf "${TMP:?}/$base"
    done
  done
done

echo
echo "results appended to $TSV"
echo
awk -F'\t' -v OFS='\t' \
  '{ for (i = 1; i <= NF; i++) if ($i == "") $i = "-"; print }' \
  "$RUNTSV" | column -t -s $'\t'
