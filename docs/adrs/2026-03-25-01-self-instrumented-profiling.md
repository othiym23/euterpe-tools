# Self-Instrumented Profiling via tracing + tracing-chrome

## Status

Accepted

## Context

The etp-\* binaries run on a Synology NAS (DSM 7.3) that has no toolchain, perf,
dstat, or other profiling tools. To gather performance data for postmortem
analysis of `etp catalog` runs and other resource-intensive operations, all
instrumentation must be baked into the binary and write output files for offline
analysis.

Options considered:

- **perf**: Requires kernel support (`perf_event_open`) and a version-matched
  binary. DSM's kernel likely lacks support, and cross-compiling perf is
  painful.
- **eBPF/bpftrace**: DSM 7.3's kernel almost certainly doesn't support it.
- **Tracy**: Requires a live network viewer connection, not suitable for
  postmortem analysis of long catalog runs.
- **tracing + tracing-chrome**: Self-contained, writes Chrome Trace Format JSON
  files viewable in Perfetto. Zero host dependencies.

## Decision

Use the `tracing` crate ecosystem with `tracing-chrome` for self-instrumented
profiling, gated behind a `profiling` Cargo feature flag.

Key design choices:

- **Feature-gated**: All profiling code behind `profiling` feature. Production
  builds are unaffected — no extra dependencies, no runtime cost.
- **`#[instrument]` attributes**: Use
  `#[cfg_attr(feature = "profiling", tracing::instrument(name = "...", skip_all))]`
  on instrumented functions. Explicit `name` and `skip_all` keep span names
  short and avoid capturing non-Debug arguments.
- **`/proc/self` metrics**: On Linux, read `/proc/self/io` (I/O bytes, syscall
  counts) and `/proc/self/status` (VmPeak, VmRSS, VmHWM) at phase boundaries.
  No-op on macOS.
- **Chrome Trace Format**: Output is a `.json` file viewable in Perfetto
  (ui.perfetto.dev) or chrome://tracing.
- **`--profile` CLI flag**: Opt-in per invocation, orthogonal to `--verbose`.
  Trace file written to the current working directory.
- **v0 symbol mangling**: Required on macOS to avoid linker assertion failures
  from tracing-subscriber's deep generic layer types. Set in
  `.cargo/config.toml`.

## Consequences

- Profiling-enabled builds require `--features profiling` at build time and
  `--profile` at runtime.
- Trace files can be large for long catalog runs (proportional to span count).
  Per-directory spans are intentionally omitted to keep file sizes reasonable;
  progress events are emitted every 1000 directories instead.
- The `etp-catalog` orchestrator passes `--profile` through to subprocess calls,
  producing per-binary trace files in the working directory.
- New justfile targets: `build-profile` (native) and `build-nas-profile` (musl).
