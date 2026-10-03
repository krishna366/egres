# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

"""Disk bytes read and written, and CPU time, over a span of a run.

Two sources, both counters the kernel keeps anyway:

  - /proc/diskstats, per whole disk: everything the device served,
    whoever asked. The same number for either server, so it is the one
    to compare; it includes the harness's own logs and whatever else
    the box does, which is small beside a load.
  - /proc/<pid>/io, for one process: `read_bytes` and `write_bytes`
    are what that process caused at the block layer. Exact for a
    single-process server (Egres); a Postgres backend takes its
    counters with it when it exits, so there only the disks are
    reported.

CPU the same way: the host's busy time from /proc/stat, and the
server's from its cgroup's cpu.stat when it runs in one (the harness's
memory cap puts it in a scope, which covers every Postgres process),
else from /proc/<pid>/stat for a single-process server.

`snapshot` reads all of it; `delta` subtracts two snapshots.
`device_of` names the disk under a path so a report can say which
disk holds the data dir and which the WAL: a partition resolves to
its disk, a ZFS dataset to the disks of its pool.
"""

import os
import subprocess
import time
from pathlib import Path

SECTOR = 512  # /proc/diskstats counts 512-byte sectors on any device


def disks() -> dict:
    """Whole disks: name -> (bytes read, bytes written, reads, writes)
    since boot; the last two are completed requests."""
    names = {p.name for p in Path("/sys/block").iterdir()
             if not p.name.startswith(("loop", "ram", "zram", "zd"))}
    out = {}
    for line in Path("/proc/diskstats").read_text().splitlines():
        f = line.split()
        if len(f) >= 10 and f[2] in names:
            out[f[2]] = (int(f[5]) * SECTOR, int(f[9]) * SECTOR,
                         int(f[3]), int(f[7]))
    return out


def disk_of_dev(name: str) -> str:
    """The whole disk a block device name belongs to."""
    link = Path("/sys/class/block") / name
    if (link / "partition").exists():
        return link.resolve().parent.name
    return name


def device_of(path) -> list:
    """The disks under `path`, or [] when they cannot be told."""
    st = os.stat(path)
    node = Path(f"/sys/dev/block/{os.major(st.st_dev)}:"
                f"{os.minor(st.st_dev)}")
    if node.exists():
        return [disk_of_dev(node.resolve().name)]
    # No block device behind st_dev: ZFS. The dataset's pool is the
    # mount source up to the first slash; zpool names its disks.
    best = ("", "", "")
    for line in Path("/proc/mounts").read_text().splitlines():
        src, mnt, fstype = line.split()[:3]
        if str(Path(path).resolve()).startswith(mnt) and \
                len(mnt) > len(best[1]):
            best = (src, mnt, fstype)
    if best[2] != "zfs":
        return []
    try:
        text = subprocess.run(
            ["zpool", "list", "-v", "-H", "-P", "-L",
             best[0].split("/")[0]],
            capture_output=True, text=True, timeout=10).stdout
    except (OSError, subprocess.TimeoutExpired):
        return []
    found = []
    for line in text.splitlines():
        dev = line.split()[0] if line.split() else ""
        if dev.startswith("/dev/"):
            disk = disk_of_dev(Path(dev).name)
            if disk not in found:
                found.append(disk)
    return found


def proc_io(pid):
    """(read_bytes, write_bytes) of one process, or None."""
    try:
        text = Path(f"/proc/{pid}/io").read_text()
    except OSError:
        return None
    vals = dict(line.split(": ") for line in text.splitlines())
    return int(vals["read_bytes"]), int(vals["write_bytes"])


def host_cpu() -> tuple:
    """(busy, total) CPU seconds of the host since boot, summed over
    every CPU: busy is everything but idle and iowait."""
    f = Path("/proc/stat").read_text().split("\n", 1)[0].split()
    ticks = [int(x) for x in f[1:9]]
    hz = os.sysconf("SC_CLK_TCK")
    idle = ticks[3] + ticks[4]
    return (sum(ticks) - idle) / hz, sum(ticks) / hz


def server_cpu(pid, cgroup, single_process: bool):
    """CPU seconds the server has used: its cgroup's, else its own
    process's when that is the whole server, else None."""
    if cgroup:
        try:
            for line in (Path(cgroup) / "cpu.stat").read_text().splitlines():
                key, val = line.split()
                if key == "usage_usec":
                    return int(val) / 1e6
        except OSError:
            return None
    if pid and single_process:
        try:
            f = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1]
        except OSError:
            return None
        utime, stime = f.split()[11:13]
        return (int(utime) + int(stime)) / os.sysconf("SC_CLK_TCK")
    return None


def snapshot(pid=None, cgroup=None, single_process=False) -> dict:
    return {"time": time.time(), "disks": disks(),
            "pid": pid, "proc": proc_io(pid) if pid else None,
            "host_cpu": host_cpu(),
            "server_cpu": server_cpu(pid, cgroup, single_process)}


def delta(a: dict, b: dict) -> dict:
    """What happened between snapshots `a` and `b`. `proc` is None
    when the process is not the same one at both ends (a restart).
    `cpu`: the host's cores busy on average, its core count, and the
    cores the server used on average (None when unknown)."""
    out = {"secs": b["time"] - a["time"], "disks": {}, "proc": None}
    for name, now in b["disks"].items():
        then = a["disks"].get(name, (0, 0, 0, 0))
        d = tuple(x1 - x0 for x1, x0 in zip(now, then))
        if any(d):
            out["disks"][name] = d
    if a["proc"] and b["proc"] and a["pid"] == b["pid"]:
        out["proc"] = (b["proc"][0] - a["proc"][0],
                       b["proc"][1] - a["proc"][1])
    busy = b["host_cpu"][0] - a["host_cpu"][0]
    total = b["host_cpu"][1] - a["host_cpu"][1]
    ncpu = os.cpu_count() or 1
    server = None
    if a["server_cpu"] is not None and b["server_cpu"] is not None \
            and a["pid"] == b["pid"]:
        server = (b["server_cpu"] - a["server_cpu"]) / max(out["secs"], 1)
    out["cpu"] = {"host_cores_busy": busy / max(total, 1e-9) * ncpu,
                  "host_cores": ncpu, "server_cores": server}
    return out
