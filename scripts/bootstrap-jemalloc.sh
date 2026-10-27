#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

if (( $# < 2 || $# > 3 )); then
    echo "usage: $0 VERSION SHA256 [ZIG_TARGET]" >&2
    exit 1
fi
version=$1
expected_sha=$2
# A non-native build (`zig build -Dtarget=...`): jemalloc is compiled
# for that target and installed beside the host's copy, so a cross build
# never replaces the archive the native build links.
zig_target=${3:-}

# jemalloc is compiled with Zig's own C compiler, native or cross, so
# building Egres needs no gcc or clang on the machine. build.zig passes
# the running Zig in ZIG; a direct invocation falls back to PATH.
zig=${ZIG:-zig}

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
build_dir="$deps_dir/build/$version${zig_target:+-$zig_target}"
install_dir="$deps_dir/install/$version${zig_target:+-$zig_target}"
library="$install_dir/lib/libjemalloc.a"
# The stamp names the toolchain so an archive built with the system
# compiler by an older bootstrap is rebuilt once, not kept.
stamp="$install_dir/.installed-$expected_sha-zigcc"

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

# macOS ships shasum, not sha256sum; both print "<hash>  <file>".
if command -v sha256sum >/dev/null; then
    actual_sha=$(sha256sum "$archive")
else
    actual_sha=$(shasum -a 256 "$archive")
fi
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

# Cross-compiling (the target's arch is not the host's): configure
# gets --host so it skips the probes that run a test program, and the
# page size those would have measured. arm64 Linux kernels use 4 KiB
# or 64 KiB pages; an archive built for 64 KiB runs on both.
# Expanded below with the `[@]+` form: an empty array is "unbound" to
# bash 3.2 (macOS) under set -u.
target_opts=()
# `zig cc` compiles for the host's own CPU unless told otherwise, and
# an archive built on one machine and cached for another (CI) then
# faults on an instruction that machine lacks. Baseline is the arch's
# floor, which a cross target gets by itself.
export CC="$zig cc -mcpu=baseline" AR="$zig ar" RANLIB="$zig ranlib"
if [[ -n "$zig_target" ]]; then
    export CC="$zig cc -target $zig_target"
    host_arch=$(uname -m)
    [[ "$host_arch" == arm64 ]] && host_arch=aarch64
    target_arch=${zig_target%%-*}
    if [[ "$target_arch" != "$host_arch" ]]; then
        host=${zig_target%%.*}
        case "$host" in
            *-macos*) host="$target_arch-apple-darwin" ;;
        esac
        target_opts+=(--host="$host")
        case "$target_arch" in
            aarch64) target_opts+=(--with-lg-page=16) ;;
        esac
    fi
fi

mkdir -p "$build_dir"
(
    cd "$build_dir"
    # On macOS jemalloc defaults to a je_ API prefix; empty keeps the
    # names galloc imports and makes it the default malloc zone too,
    # as it overrides libc malloc on Linux.
    # --disable-cxx drops the operator new/delete wrappers: nothing
    # here links C++ (mimalloc's bootstrap turns its C++ build off
    # too), and skipping them keeps the build off the host's C++
    # headers, which macOS CommandLineTools do not always ship intact.
    CFLAGS="-O3 -DNDEBUG" "$source_dir/configure" \
        --prefix="$install_dir" \
        --with-jemalloc-prefix= \
        --disable-shared \
        --enable-static \
        --disable-cxx \
        --enable-stats \
        --disable-debug \
        ${target_opts[@]+"${target_opts[@]}"}
    # nproc is Linux; macOS answers through sysctl.
    jobs=${JEMALLOC_BUILD_JOBS:-$(nproc 2>/dev/null ||
        sysctl -n hw.ncpu)}
    make -j"$jobs"
    make install_lib_static install_include
)

touch "$stamp"
echo "jemalloc $version installed under $install_dir"
