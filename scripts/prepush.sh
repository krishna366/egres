#!/usr/bin/env bash
# Pre-push gate: build the exe and run the full (big) test suite before a
# push is allowed. `zig build test` alone skips the exe (it rots silently),
# so build it too. Wired to the `jj push` alias in this repo's jj config;
# also runnable on its own: `scripts/prepush.sh`.
set -euo pipefail

# Run from the repo root regardless of the caller's cwd.
cd "$(dirname "${BASH_SOURCE[0]}")/.."

echo "prepush: zig build (exe) ..."
zig build

# The non-default allocator would otherwise never compile: build its exe
# and run the generic allocator tests against it (the full suite runs on
# the default backend below).
echo "prepush: zig build -Dallocator=jemalloc (exe) ..."
zig build -Dallocator=jemalloc

echo "prepush: zig build test -- --bigtest ..."
zig build test -- --bigtest

echo "prepush: zig build test -Dallocator=jemalloc -- --filter galloc ..."
zig build test -Dallocator=jemalloc -- --filter galloc

echo "prepush: OK"
