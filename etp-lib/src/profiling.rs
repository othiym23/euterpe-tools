use std::path::{Path, PathBuf};

/// Guard that flushes the trace file on drop and prints a summary to stderr.
pub struct ProfilingGuard {
    _guard: tracing_chrome::FlushGuard,
    trace_path: PathBuf,
}

impl ProfilingGuard {
    /// Flush the trace file and print a summary to stderr.
    pub fn finish(self) {
        let path = self.trace_path.clone();
        // Drop self (and the inner FlushGuard) to flush all buffered data.
        drop(self);
        if let Ok(meta) = std::fs::metadata(&path) {
            let size = crate::ops::format_size(meta.len());
            eprintln!("profiling: wrote {} ({})", path.display(), size);
        }
    }
}

/// Initialize the tracing-chrome subscriber. Returns a guard that must be held
/// alive for the duration of the program. Call `.finish()` at the end of main
/// to flush and print a summary.
pub fn init_profiling(trace_path: &Path) -> ProfilingGuard {
    use tracing_chrome::{ChromeLayerBuilder, TraceStyle};
    use tracing_subscriber::prelude::*;

    let (chrome_layer, guard) = ChromeLayerBuilder::new()
        .file(trace_path)
        .trace_style(TraceStyle::Async)
        .include_args(true)
        .build();

    tracing_subscriber::registry().with(chrome_layer).init();

    ProfilingGuard {
        _guard: guard,
        trace_path: trace_path.to_path_buf(),
    }
}

/// Generate a trace file path in the current working directory.
/// Format: `etp-trace-<binary>-<epoch_seconds>.json`
pub fn trace_path(binary_name: &str) -> PathBuf {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    PathBuf::from(format!("etp-trace-{binary_name}-{secs}.json"))
}

/// Sample `/proc/self/io` and `/proc/self/status` and emit metrics as tracing events.
/// No-op on non-Linux platforms.
#[cfg(target_os = "linux")]
pub fn sample_proc_metrics(label: &str) {
    if let Ok(io_contents) = std::fs::read_to_string("/proc/self/io") {
        let mut rchar = 0u64;
        let mut wchar = 0u64;
        let mut syscr = 0u64;
        let mut syscw = 0u64;
        let mut read_bytes = 0u64;
        let mut write_bytes = 0u64;

        for line in io_contents.lines() {
            if let Some((key, val)) = line.split_once(':') {
                let val = val.trim();
                match key.trim() {
                    "rchar" => rchar = val.parse().unwrap_or(0),
                    "wchar" => wchar = val.parse().unwrap_or(0),
                    "syscr" => syscr = val.parse().unwrap_or(0),
                    "syscw" => syscw = val.parse().unwrap_or(0),
                    "read_bytes" => read_bytes = val.parse().unwrap_or(0),
                    "write_bytes" => write_bytes = val.parse().unwrap_or(0),
                    _ => {}
                }
            }
        }
        tracing::info!(
            label,
            rchar,
            wchar,
            syscr,
            syscw,
            read_bytes,
            write_bytes,
            "proc_io"
        );
    }

    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        let mut vm_peak_kb = 0u64;
        let mut vm_rss_kb = 0u64;
        let mut vm_hwm_kb = 0u64;

        for line in status.lines() {
            if let Some((key, val)) = line.split_once(':') {
                let val = val.trim().trim_end_matches(" kB").trim();
                match key.trim() {
                    "VmPeak" => vm_peak_kb = val.parse().unwrap_or(0),
                    "VmRSS" => vm_rss_kb = val.parse().unwrap_or(0),
                    "VmHWM" => vm_hwm_kb = val.parse().unwrap_or(0),
                    _ => {}
                }
            }
        }
        tracing::info!(label, vm_peak_kb, vm_rss_kb, vm_hwm_kb, "proc_status");
    }
}

#[cfg(not(target_os = "linux"))]
pub fn sample_proc_metrics(_label: &str) {}
