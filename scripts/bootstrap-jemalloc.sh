#!/usr/bin/env bash
set -euo pipefail

if (( $# != 2 )); then
    echo "usage: $0 VERSION SHA256" >&2
    exit 1
fi
version=$1
expected_sha=$2

project_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "invalid jemalloc version: $version" >&2
    exit 1
fi
if [[ ! "$expected_sha" =~ ^[0-9a-f]{64}$ ]]; then
    echo "invalid jemalloc SHA-256" >&2
    exit 1
fi

deps_dir="$project_dir/.deps/jemalloc"
archive_dir="$deps_dir/archives"
archive="$archive_dir/jemalloc-$version.tar.bz2"
source_dir="$deps_dir/source/$version"
build_dir="$deps_dir/build/$version"
install_dir="$deps_dir/install/$version"
library="$install_dir/lib/libjemalloc.a"
stamp="$install_dir/.installed-$expected_sha"

if [[ -f "$library" && -f "$stamp" ]]; then
    exit 0
fi

mkdir -p "$archive_dir"
if [[ ! -f "$archive" ]]; then
    base_url="https://github.com/jemalloc/jemalloc/releases/download"
    curl -fL --retry 5 --retry-delay 2 \
        -o "$archive.part" \
        "$base_url/$version/jemalloc-$version.tar.bz2"
    mv "$archive.part" "$archive"
fi

actual_sha=$(sha256sum "$archive")
actual_sha=${actual_sha%% *}
if [[ "$actual_sha" != "$expected_sha" ]]; then
    echo "jemalloc checksum mismatch" >&2
    echo "expected: $expected_sha" >&2
    echo "actual:   $actual_sha" >&2
    exit 1
fi

if [[ ! -f "$source_dir/.extracted" ]]; then
    mkdir -p "$source_dir"
    tar -xf "$archive" --strip-components=1 -C "$source_dir"
    touch "$source_dir/.extracted"
fi

mkdir -p "$build_dir"
(
    cd "$build_dir"
    CFLAGS="-O3 -DNDEBUG" "$source_dir/configure" \
        --prefix="$install_dir" \
        --disable-shared \
        --enable-static \
        --enable-stats \
        --disable-debug
    make -j"${JEMALLOC_BUILD_JOBS:-$(nproc)}"
    make install_lib_static install_include
)

touch "$stamp"
echo "jemalloc $version installed under $install_dir"
