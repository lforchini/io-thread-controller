// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Nutanix, Inc.
//
// Author: Thanos Makatos <thanos.makatos@nutanix.com>

//! Uptime-style per-VM and aggregate status log lines.

use std::{collections::HashMap, sync::Arc};

use crate::{
    config::Config,
    instance::{Instance, InstanceStatus},
    rolling::{WINDOWS, format_1_5_15, format_cells},
};

/// Emit a one-time legend describing the status-line fields.
pub(crate) fn emit_legend() {
    tracing::info!(
        target: "status",
        "# vm=<id> thr=<threads> iops=<read>/<write>/<other> \
         iops_1_5_15m=<1m>/<5m>/<15m> bw_mb_s=<read>/<write> \
         cpu=<average>/<total> cpu_us_per_io_1_5_15m=<1m>/<5m>/<15m>"
    );
    tracing::info!(
        target: "status",
        "# aggregate tracked=<instances> threads=<threads> \
         iops_1_5_15m=<1m>/<5m>/<15m>"
    );
}

/// Emit configured per-VM and aggregate status lines.
pub(crate) async fn emit_status_lines(cfg: &Config, instances: &HashMap<String, Arc<Instance>>) {
    let mut total_threads = 0u64;
    let mut aggregate_iops: [Option<u64>; 3] = [None; 3];
    let mut fleet: Vec<_> = instances.values().collect();
    fleet.sort_unstable_by(|left, right| left.id.cmp(&right.id));
    for instance in fleet {
        let status = instance.status.read().await;
        let iops_windows = WINDOWS.map(|window| status.rolling.iops_over(window));
        for (total, value) in aggregate_iops.iter_mut().zip(iops_windows) {
            if let Some(value) = value {
                *total = Some(total.unwrap_or(0).saturating_add(value));
            }
        }
        if cfg.enable_per_vm_status_line {
            emit_instance_status(instance, &status, iops_windows);
        }
        total_threads += u64::from(status.thread_count);
    }

    if cfg.enable_aggregate_status_line {
        tracing::info!(
            target: "status",
            tracked = instances.len(),
            total_threads,
            iops_1_5_15m = %format_cells(aggregate_iops),
            "aggregate"
        );
    }
}

/// Emit one uptime-style per-VM status record.
fn emit_instance_status(
    instance: &Instance,
    status: &InstanceStatus,
    iops_windows: [Option<u64>; 3],
) {
    let cpu = status.cpu_stats();
    tracing::info!(
        target: "status",
        vm = instance.to_string(),
        thr = status.thread_count,
        iops = %format!(
            "{}/{}/{}",
            status.rates.read_iops,
            status.rates.write_iops,
            status.rates.other_iops
        ),
        iops_1_5_15m = %format_cells(iops_windows),
        bw_mb_s = %format!(
            "{}/{}",
            status.rates.read_bytes_per_second / 1_000_000,
            status.rates.write_bytes_per_second / 1_000_000
        ),
        cpu = %format!("{}/{}/{}", cpu.avg_pct, cpu.median_pct, cpu.total_pct),
        cpu_us_per_io_1_5_15m =
            %format_1_5_15(&status.rolling, |rolling, window| {
                rolling.cpu_us_per_io_over(window)
            }),
        ""
    );
}
