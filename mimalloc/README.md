# mimalloc integration

The `mimalloc_version` and `mimalloc_sha256` constants in `build.zig` are
the single source of truth for the pinned release. The build passes both
values to `scripts/bootstrap-mimalloc.sh`.

The bootstrap stores each release independently:

```text
.deps/mimalloc/
├── archives/mimalloc-v<version>.tar.gz
├── source/<version>/
├── build/<version>/
└── install/<version>/lib/libmimalloc.a
```

To upgrade, replace both constants in `build.zig`, then run:

```bash
zig build egres -Drelease
zig build test -- --filter galloc
zig build test
```

No cache deletion is required. Builds select the archive named by
`mimalloc_version`; older releases remain cached for rollback.

Three properties the build depends on:

- One allocator per binary: `-Dallocator` picks the backend at build
  time (default `mimalloc`), the chosen adapter becomes the `galloc`
  module, and its archive is the only allocator linked. The generic
  allocator tests (`egres/galloc_test.zig`) run against whichever
  backend the test binary was built with.
- `MI_OVERRIDE=ON`: the archive also defines libc malloc/free, so the
  whole process — the Zig gpa and C libraries (libpg_query, lz4/zstd)
  alike — allocates through mimalloc, mirroring jemalloc's
  non-prefixed archive in a `-Dallocator=jemalloc` build.
- No configuration: every knob probed in the zmem study was inert or
  harmful. mimalloc instead needs a caller — it has no background
  threads, so main.zig spawns a maintenance fiber that runs the gentle
  `collect()` once a second in mimalloc builds.
