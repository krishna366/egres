# jemalloc integration

The `jemalloc_version` and `jemalloc_sha256` constants in `build.zig` are the
single source of truth for the pinned release. The build passes both values to
`scripts/bootstrap-jemalloc.sh`.

The bootstrap stores each release independently:

```text
.deps/jemalloc/
├── archives/jemalloc-<version>.tar.bz2
├── source/<version>/
├── build/<version>/
└── install/<version>/lib/libjemalloc.a
```

To upgrade, replace both constants in `build.zig`, then run:

```bash
zig build egres -Drelease
zig build test -- --filter jemalloc
zig build test
```

No cache deletion is required. Builds select the archive named by
`jemalloc_version`; older releases remain cached for rollback.

Before committing an upgrade, also review the extended allocation API,
`mallctl` names, configuration keys, and `MALLCTL_ARENAS_ALL` value used by
`root.zig`.
