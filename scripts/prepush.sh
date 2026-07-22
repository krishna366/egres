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

echo "prepush: zig build test -- --bigtest ..."
zig build test -- --bigtest

echo "prepush: OK"
