#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

if (( $# != 2 )); then
    echo "usage: $0 VERSION SHA256" >&2
    exit 1
fi
version=$1
expected_sha=$2

project_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "invalid mimalloc version: $version" >&2
    exit 1
fi
if [[ ! "$expected_sha" =~ ^[0-9a-f]{64}$ ]]; then
    echo "invalid mimalloc SHA-256" >&2
    exit 1
fi

deps_dir="$project_dir/.deps/mimalloc"
archive_dir="$deps_dir/archives"
archive="$archive_dir/mimalloc-v$version.tar.gz"
source_dir="$deps_dir/source/$version"
build_dir="$deps_dir/build/$version"
install_dir="$deps_dir/install/$version"
library="$install_dir/lib/libmimalloc.a"
# The stamp names the build flavor (override = MI_OVERRIDE=ON) so
# flipping that cmake flag invalidates an install built the old way
# even though version and sha are unchanged.
stamp="$install_dir/.installed-$expected_sha-override"

if [[ -f "$library" && -f "$stamp" ]]; then
    exit 0
fi

mkdir -p "$archive_dir"
if [[ ! -f "$archive" ]]; then
    base_url="https://github.com/microsoft/mimalloc/archive/refs/tags"
    curl -fL --retry 5 --retry-delay 2 \
        -o "$archive.part" \
        "$base_url/v$version.tar.gz"
    mv "$archive.part" "$archive"
fi

actual_sha=$(sha256sum "$archive")
actual_sha=${actual_sha%% *}
if [[ "$actual_sha" != "$expected_sha" ]]; then
    echo "mimalloc checksum mismatch" >&2
    echo "expected: $expected_sha" >&2
    echo "actual:   $actual_sha" >&2
    exit 1
fi

if [[ ! -f "$source_dir/.extracted" ]]; then
    mkdir -p "$source_dir"
    tar -xf "$archive" --strip-components=1 -C "$source_dir"
    touch "$source_dir/.extracted"
fi

# MI_OVERRIDE=ON: the archive defines libc malloc/free as well as the
# mi_* API (the overrides are #include-d into alloc.o, which the Zig
# adapter's mi_* references always pull in). Each binary links exactly
# one allocator (-Dallocator in build.zig), so nothing else defines
# malloc — and the whole process, C libraries included, allocates
# through mimalloc, mirroring how jemalloc's non-prefixed archive owns
# libc malloc in a jemalloc build.
mkdir -p "$build_dir"
cmake -S "$source_dir" -B "$build_dir" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="$install_dir" \
    -DMI_OVERRIDE=ON \
    -DMI_BUILD_SHARED=OFF \
    -DMI_BUILD_STATIC=ON \
    -DMI_BUILD_OBJECT=OFF \
    -DMI_BUILD_TESTS=OFF \
    -DMI_BUILD_CXX=OFF \
    -DMI_INSTALL_TOPLEVEL=ON
cmake --build "$build_dir" --parallel
cmake --install "$build_dir"

touch "$stamp"
echo "mimalloc $version installed under $install_dir"
