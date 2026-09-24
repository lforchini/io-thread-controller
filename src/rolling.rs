// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Nutanix, Inc.
//
// Author: Thanos Makatos <thanos.makatos@nutanix.com>

//! Bounded per-VM history for uptime-style 1m/5m/15m metrics.

use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

/// Longest retained status window.
const MAX_WINDOW: Duration = Duration::from_secs(15 * 60);

/// Delta recorded for one successful sampling interval.
#[derive(Debug, Clone, Copy)]
struct TickSample {
    sampled_at: Instant,
    io_ops_delta: u64,
    cpu_ns_delta: u64,
    wall_dt_ns: u64,
}

/// Rolling deltas plus the cumulative-counter baseline for the next tick.
#[derive(Debug, Default, Clone)]
pub struct RollingMetrics {
    samples: VecDeque<TickSample>,
    previous_wall_ts: Option<Instant>,
    previous_io_ops: Option<u64>,
    previous_cpu_ticks: Option<u64>,
}

impl RollingMetrics {
    /// Construct an empty history.
    pub fn new() -> Self {
        Self::default()
    }

    /// Diff cumulative backend and `/proc` counters and append one interval.
    ///
    /// The first call establishes a baseline. A counter reset also replaces the
    /// baseline without manufacturing a large wrapped delta.
    pub fn push_from_procfs_delta(
        &mut self,
        now: Instant,
        io_ops_cumulative: u64,
        cpu_ticks_cumulative: u64,
        clock_ticks_per_second: f64,
    ) {
        if let (Some(elapsed), Some(previous_io_ops), Some(previous_cpu_ticks)) = (
            self.elapsed_since_previous(now),
            self.previous_io_ops,
            self.previous_cpu_ticks,
        ) && io_ops_cumulative >= previous_io_ops
            && cpu_ticks_cumulative >= previous_cpu_ticks
            && clock_ticks_per_second > 0.0
        {
            let cpu_tick_delta = cpu_ticks_cumulative - previous_cpu_ticks;
            let cpu_ns_delta =
                (cpu_tick_delta as f64 * 1_000_000_000.0 / clock_ticks_per_second) as u64;
            self.push_sample(
                now,
                elapsed,
                io_ops_cumulative - previous_io_ops,
                cpu_ns_delta,
            );
        }
        self.previous_wall_ts = Some(now);
        self.previous_io_ops = Some(io_ops_cumulative);
        self.previous_cpu_ticks = Some(cpu_ticks_cumulative);
    }

    /// Append an interval from backend-computed per-thread utilisation.
    pub fn push_from_backend_util(
        &mut self,
        now: Instant,
        io_ops_cumulative: u64,
        per_thread_util: f64,
        thread_count: u32,
    ) {
        if let (Some(elapsed), Some(previous_io_ops)) =
            (self.elapsed_since_previous(now), self.previous_io_ops)
            && io_ops_cumulative >= previous_io_ops
        {
            let cpu_ns_delta = (saturating_nanos(elapsed) as f64
                * per_thread_util.clamp(0.0, 1.0)
                * f64::from(thread_count))
            .min(u64::MAX as f64) as u64;
            self.push_sample(
                now,
                elapsed,
                io_ops_cumulative - previous_io_ops,
                cpu_ns_delta,
            );
        }
        self.previous_wall_ts = Some(now);
        self.previous_io_ops = Some(io_ops_cumulative);
        self.previous_cpu_ticks = None;
    }

    /// Append an already-computed delta, used by fleet aggregation.
    pub fn push_delta(&mut self, now: Instant, io_ops_delta: u64, cpu_ns_delta: u64) {
        if let Some(elapsed) = self.elapsed_since_previous(now) {
            self.push_sample(now, elapsed, io_ops_delta, cpu_ns_delta);
        }
        self.previous_wall_ts = Some(now);
    }

    /// Return the newest I/O and CPU deltas.
    pub fn last_delta(&self) -> Option<(u64, u64)> {
        self.samples
            .back()
            .map(|sample| (sample.io_ops_delta, sample.cpu_ns_delta))
    }

    /// Average IOPS over retained intervals inside `window`.
    pub fn iops_over(&self, window: Duration) -> Option<u64> {
        let (io_ops, _, wall_ns) = self.totals(window)?;
        (wall_ns > 0).then(|| {
            (u128::from(io_ops) * 1_000_000_000 / u128::from(wall_ns)).min(u128::from(u64::MAX))
                as u64
        })
    }

    /// CPU microseconds consumed per completed I/O inside `window`.
    pub fn cpu_us_per_io_over(&self, window: Duration) -> Option<u64> {
        let (io_ops, cpu_ns, _) = self.totals(window)?;
        (io_ops > 0).then(|| cpu_ns / io_ops / 1_000)
    }

    /// Number of retained sampling intervals.
    pub fn len(&self) -> usize {
        self.samples.len()
    }

    /// Whether no complete sampling interval has been retained.
    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    /// Time since the previous baseline, if one exists and `now` is not
    /// earlier.
    fn elapsed_since_previous(&self, now: Instant) -> Option<Duration> {
        now.checked_duration_since(self.previous_wall_ts?)
    }

    /// Record one completed interval ending at `now`.
    fn push_sample(
        &mut self,
        now: Instant,
        elapsed: Duration,
        io_ops_delta: u64,
        cpu_ns_delta: u64,
    ) {
        self.samples.push_back(TickSample {
            sampled_at: now,
            io_ops_delta,
            cpu_ns_delta,
            wall_dt_ns: saturating_nanos(elapsed),
        });
        self.trim(now);
    }

    fn trim(&mut self, now: Instant) {
        // If `now` is still within MAX_WINDOW of the
        // opaque Instant origin, keep every sample.
        let Some(cutoff) = now.checked_sub(MAX_WINDOW) else {
            return;
        };
        while self
            .samples
            .front()
            .is_some_and(|sample| sample.sampled_at < cutoff)
        {
            self.samples.pop_front();
        }
    }

    fn totals(&self, window: Duration) -> Option<(u64, u64, u64)> {
        let newest = self.samples.back()?.sampled_at;
        // Same as trim: a failed checked_sub means "include all retained samples".
        let cutoff = newest.checked_sub(window);
        let mut io_ops = 0u64;
        let mut cpu_ns = 0u64;
        let mut wall_ns = 0u64;
        for sample in self
            .samples
            .iter()
            .filter(|sample| cutoff.is_none_or(|cutoff| sample.sampled_at >= cutoff))
        {
            io_ops = io_ops.saturating_add(sample.io_ops_delta);
            cpu_ns = cpu_ns.saturating_add(sample.cpu_ns_delta);
            wall_ns = wall_ns.saturating_add(sample.wall_dt_ns);
        }
        Some((io_ops, cpu_ns, wall_ns))
    }
}

/// Convert a duration to nanoseconds, saturating at `u64::MAX`.
fn saturating_nanos(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

/// Render 1m/5m/15m cells, using `-` until a window has a sample.
pub fn format_1_5_15<F>(metrics: &RollingMetrics, getter: F) -> String
where
    F: Fn(&RollingMetrics, Duration) -> Option<u64>,
{
    [1, 5, 15]
        .map(|minutes| {
            getter(metrics, Duration::from_secs(minutes * 60))
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string())
        })
        .join("/")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that the first sample records a baseline and yields no
    /// rates yet.
    #[test]
    fn first_sample_only_establishes_baseline() {
        let mut metrics = RollingMetrics::new();
        let t0 = Instant::now();
        metrics.push_from_procfs_delta(t0, 10, 20, 100.0);
        assert!(metrics.is_empty());
    }

    /// Test that rolling IOPS/CPU-per-IO rates use real elapsed time
    /// between samples.
    #[test]
    fn rates_use_real_elapsed_time() {
        let mut metrics = RollingMetrics::new();
        let t0 = Instant::now();
        metrics.push_from_procfs_delta(t0, 10, 20, 100.0);
        metrics.push_from_procfs_delta(t0 + Duration::from_secs(2), 210, 120, 100.0);
        assert_eq!(metrics.iops_over(Duration::from_secs(60)), Some(100));
        assert_eq!(
            metrics.cpu_us_per_io_over(Duration::from_secs(60)),
            Some(5_000)
        );
    }

    /// Test that backend-reported utilisation converts to CPU time over
    /// the elapsed interval.
    #[test]
    fn backend_util_derives_cpu_time_from_elapsed_interval() {
        let mut metrics = RollingMetrics::new();
        let t0 = Instant::now();
        metrics.push_from_backend_util(t0, 0, 0.5, 2);
        metrics.push_from_backend_util(t0 + Duration::from_secs(1), 100, 0.5, 2);
        assert_eq!(metrics.iops_over(Duration::from_secs(60)), Some(100));
        assert_eq!(
            metrics.cpu_us_per_io_over(Duration::from_secs(60)),
            Some(10_000)
        );
    }

    /// Test that a counter reset drops prior rates and starts a new
    /// baseline.
    #[test]
    fn counter_reset_replaces_baseline() {
        let mut metrics = RollingMetrics::new();
        let t0 = Instant::now();
        metrics.push_from_procfs_delta(t0, 100, 100, 100.0);
        metrics.push_from_procfs_delta(t0 + Duration::from_secs(1), 10, 10, 100.0);
        assert!(metrics.is_empty());
    }
}
