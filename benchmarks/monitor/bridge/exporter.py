#!/usr/bin/env python3
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

"""Prometheus bridge for a running Egres.

Egres answers the admin command `zdb` with its storage counters in
Prometheus text format, terminated by a blank line. This serves that
blob at /metrics for Prometheus to scrape, and adds what the server
does not report about itself: the data dir's size, its SST and WAL
byte totals and file count, and whether the admin port answered.

    exporter.py --admin 127.0.0.1:5210 --data /path/to/forest --listen 9877

`--wal /path/to/wal` when the WAL has a dir of its own: its size is
then every file under that dir.

Standard library only, so it runs from a stock python image.
"""

import argparse
import os
import re
import socket
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# A metric line: name, optional labels, value, optional timestamp.
METRIC_RE = re.compile(r"^[A-Za-z_:][A-Za-z0-9_:]*(\{[^}]*\})? -?[0-9.eE+\-]+|^[A-Za-z_:][A-Za-z0-9_:]*(\{[^}]*\})? [+-]?(Inf|NaN)")


def admin(addr: tuple[str, int], command: str, timeout: float = 8.0) -> str:
    """One admin round trip: send the command, read to the blank line
    that ends the reply (or until the server closes)."""
    with socket.create_connection(addr, timeout=timeout) as s:
        s.sendall((command + "\n").encode())
        out = b""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                chunk = s.recv(1 << 16)
            except socket.timeout:
                break
            if not chunk:
                break
            out += chunk
            if out.endswith(b"\n\n"):
                break
    return out.decode(errors="replace")


def dir_stats(root: str) -> dict[str, int]:
    """Bytes and counts under the forest dir, one recursive walk."""
    total = sst_bytes = sst_files = wal_bytes = 0
    for dirpath, _dirs, files in os.walk(root):
        for name in files:
            try:
                size = os.lstat(os.path.join(dirpath, name)).st_size
            except OSError:
                continue
            total += size
            if name.endswith(".sst"):
                sst_bytes += size
                sst_files += 1
            elif name.endswith(".wal"):
                wal_bytes += size
    return {
        "egres_data_dir_bytes": total,
        "egres_sst_bytes": sst_bytes,
        "egres_sst_files": sst_files,
        "egres_wal_bytes": wal_bytes,
    }


def dir_bytes(root: str) -> int:
    """Bytes of every file under `root`."""
    total = 0
    for dirpath, _dirs, files in os.walk(root):
        for name in files:
            try:
                total += os.lstat(os.path.join(dirpath, name)).st_size
            except OSError:
                continue
    return total


SIZE_RE = re.compile(r"([\d.]+)(B|KiB|MiB|GiB|TiB)")
UNIT = {"B": 1, "KiB": 2**10, "MiB": 2**20, "GiB": 2**30, "TiB": 2**40}


def size_bytes(text: str) -> int:
    m = SIZE_RE.match(text)
    return int(float(m.group(1)) * UNIT[m.group(2)]) if m else 0


def forest_gauges(text: str) -> dict[str, int]:
    """Gauges from the admin `zdb forest` text: the memtables, block
    cache, tree count, L0 shape and in-flight job count. The text is
    for people, so this is by pattern, and a line that does not match
    contributes nothing rather than failing the scrape."""
    g: dict[str, int] = {}
    m = re.search(r"memtables: active=(\S+) immutable=(\d+) \((\S+) pending",
                  text)
    if m:
        g["egres_memtable_active_bytes"] = size_bytes(m.group(1))
        g["egres_memtable_immutable_count"] = int(m.group(2))
        g["egres_memtable_immutable_bytes"] = size_bytes(m.group(3))
    m = re.search(r"block_cache: entries=(\d+) bytes=(\S+) hits=(\d+) "
                  r"misses=(\d+)", text)
    if m:
        g["egres_block_cache_entries"] = int(m.group(1))
        g["egres_block_cache_bytes"] = size_bytes(m.group(2))
        g["egres_block_cache_hits_total"] = int(m.group(3))
        g["egres_block_cache_misses_total"] = int(m.group(4))
    m = re.search(r"trees: (\d+)", text)
    if m:
        g["egres_trees"] = int(m.group(1))
    m = re.search(r"largest_tree: id=\d+ size=(\S+)", text)
    if m:
        g["egres_largest_tree_bytes"] = size_bytes(m.group(1))
    m = re.search(r"L0: tables=(\d+) \(max=(\d+) in tree \d+\)\s+"
                  r"bytes=(\S+) \(max=(\S+)", text)
    if m:
        g["egres_l0_tables"] = int(m.group(1))
        g["egres_l0_tables_max_tree"] = int(m.group(2))
        g["egres_l0_bytes"] = size_bytes(m.group(3))
        g["egres_l0_bytes_max_tree"] = size_bytes(m.group(4))
    m = re.search(r"in-flight:(.*?)(?:\n\n|\Z)", text, re.S)
    if m:
        body = m.group(1)
        g["egres_inflight_jobs"] = (
            0 if "(none)" in body else len([l for l in body.splitlines()
                                            if l.strip()]))
    return g


def cgroup_gauges(cg: str) -> dict[str, int]:
    """The capped server's memory: what it uses against its limit."""
    out = {}
    for name, fname in (("egres_cgroup_memory_bytes", "memory.current"),
                        ("egres_cgroup_memory_max_bytes", "memory.max")):
        try:
            v = open(os.path.join(cg, fname)).read().strip()
            out[name] = int(v) if v != "max" else 0
        except (OSError, ValueError):
            pass
    return out


class Handler(BaseHTTPRequestHandler):
    admin_addr: tuple[str, int]
    data_dir: str
    wal_dir: str = ""
    cgroup_dir: str = ""

    def log_message(self, *_args):  # quiet
        pass

    def do_GET(self):
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return
        lines = []
        t0 = time.monotonic()
        try:
            blob = admin(self.admin_addr, "zdb")
            up = 1
            for line in blob.splitlines():
                if line.startswith("#") or METRIC_RE.match(line):
                    lines.append(line)
        except OSError as e:
            up = 0
            print(f"admin unreachable: {e}", file=sys.stderr)
        if up:
            try:
                for name, value in forest_gauges(
                        admin(self.admin_addr, "zdb forest")).items():
                    lines.append(f"# TYPE {name} gauge")
                    lines.append(f"{name} {value}")
            except OSError as e:
                print(f"zdb forest failed: {e}", file=sys.stderr)
        lines.append("# TYPE egres_up gauge")
        lines.append(f"egres_up {up}")
        lines.append("# TYPE egres_admin_seconds gauge")
        lines.append(f"egres_admin_seconds {time.monotonic() - t0:.4f}")
        if os.path.isdir(self.data_dir):
            stats = dir_stats(self.data_dir)
            # A WAL dir of its own is outside the data dir's walk.
            if os.path.isdir(self.wal_dir):
                stats["egres_wal_bytes"] = dir_bytes(self.wal_dir)
            for name, value in stats.items():
                lines.append(f"# TYPE {name} gauge")
                lines.append(f"{name} {value}")
        if self.cgroup_dir:
            for name, value in cgroup_gauges(self.cgroup_dir).items():
                lines.append(f"# TYPE {name} gauge")
                lines.append(f"{name} {value}")
        body = ("\n".join(lines) + "\n").encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--admin", default="127.0.0.1:5210")
    ap.add_argument("--data", required=True)
    ap.add_argument("--listen", type=int, default=9877)
    # The WAL dir when it is not inside --data; empty = it is.
    ap.add_argument("--wal", default="")
    # A cgroup (memory-capped server scope) to report, as its relative
    # path under --cgroup-root; empty = none.
    ap.add_argument("--cgroup", default="")
    ap.add_argument("--cgroup-root", default="/sys/fs/cgroup")
    args = ap.parse_args()
    Handler.cgroup_dir = ""
    if args.cgroup:
        Handler.cgroup_dir = os.path.join(args.cgroup_root,
                                          args.cgroup.lstrip("/"))
    host, port = args.admin.rsplit(":", 1)
    Handler.admin_addr = (host, int(port))
    Handler.data_dir = args.data
    Handler.wal_dir = args.wal
    srv = ThreadingHTTPServer(("0.0.0.0", args.listen), Handler)
    print(f"serving /metrics on :{args.listen}; admin {args.admin}")
    srv.serve_forever()


if __name__ == "__main__":
    main()
