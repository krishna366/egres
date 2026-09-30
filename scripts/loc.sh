#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Lines of code in this repo, split into logic, test, bench and generated
# buckets, each into Zig and other languages. Vendored code (third_party/,
# zig-pkg/) and the unused/ graveyard are left out, as are docs and config
# files: only .zig, .c, .h, .py, .sh and .sql count.
#
# Usage: scripts/loc.sh [--detail]
#   --detail   also print cloc's per-language table for every bucket
#
# A file's bucket is decided by its path, in this order:
#   generated  *.gen.zig
#   test       *_test.zig, */tests.zig, */testing.zig, */testing/*,
#              u/test_runner.zig, test_client.py, scripts/*_e2e.sh
#   bench      *_bench.zig, */bench.zig, benchmark.zig, benchmarks/*,
#              scripts/loadtest.py
#   logic      everything else
set -euo pipefail

cd "$(dirname "$0")/.."
command -v cloc >/dev/null || { echo "cloc not found" >&2; exit 1; }

detail=0
[[ "${1:-}" == "--detail" ]] && detail=1

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

git ls-files -- '*.zig' '*.c' '*.h' '*.py' '*.sh' '*.sql' |
    grep -v -E '^(third_party|zig-pkg|unused)/' |
    awk -v dir="$tmp" '
        /\.gen\.zig$/ { print > dir "/generated"; next }
        /_test\.zig$/ || /(^|\/)tests?\.zig$/ || /(^|\/)testing\.zig$/ ||
        /\/testing\// || /^u\/test_runner\.zig$/ || /^test_client\.py$/ ||
        /^scripts\/.*_e2e\.sh$/ { print > dir "/test"; next }
        /_bench\.zig$/ || /(^|\/)bench\.zig$/ || /^benchmark\.zig$/ ||
        /^benchmarks\// || /^scripts\/loadtest\.py$/ {
            print > dir "/bench"; next
        }
        { print > dir "/logic" }
    '

# One row per bucket and language group: Zig, then everything else
# (Python, shell, C, SQL) folded into "other". Rows with no files are
# left out. cloc's CSV rows are: files, language, blank, comment, code.
for b in logic test bench generated; do
    [[ -s "$tmp/$b" ]] || continue
    cloc --quiet --csv --list-file="$tmp/$b" |
        awk -F, -v b="$b" '$1 ~ /^[0-9]+$/ && $2 != "SUM" {
            g = ($2 == "Zig") ? "zig" : "other"
            files[g] += $1; blank[g] += $3; comment[g] += $4; code[g] += $5
        }
        END {
            for (g in code) printf "%s (%s),%d,%d,%d,%d\n", b, g,
                files[g], blank[g], comment[g], code[g]
        }'
done | sort -t, -k1,1 -s >"$tmp/rows"

# Buckets in the order above, zig before other within each.
total=$(awk -F, '{ t += $5 } END { print t + 0 }' "$tmp/rows")
printf '%-18s %6s %8s %8s %8s %6s\n' bucket files blank comment code '%'
for b in logic test bench generated; do
    for g in zig other; do
        grep "^$b ($g)," "$tmp/rows" || true
    done
done | awk -F, -v t="$total" '{
    printf "%-18s %6d %8d %8d %8d %5.1f%%\n", $1, $2, $3, $4, $5, 100 * $5 / t
}'
printf '%-18s %6s %8s %8s %8d\n' total '' '' '' "$total"

if (( detail )); then
    for b in logic test bench generated; do
        [[ -s "$tmp/$b" ]] || continue
        echo; echo "== $b"
        cloc --quiet --list-file="$tmp/$b" | sed '/^$/d'
    done
fi
