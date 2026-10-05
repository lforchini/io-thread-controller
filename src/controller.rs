// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Nutanix, Inc.
//
// Author: Thanos Makatos <thanos.makatos@nutanix.com>

//! Engine-agnostic fleet inventory, sampling, and actuation.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use futures_util::future::join_all;
use procfs::{CurrentSI, ProcError};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    backends::{BackendClientError, IoThreadProperties, VqMapping},
    config::Config,
    daemon::VERSION,
    dbus::DbusRequest,
    engines::{AppliedOutcome, BlockedReason, EngineTickContext, ScaleAction, ScalingEngine},
    instance::{Instance, InstanceStatus},
    state::{StateError, VmOwnership, VmStateStore},
    status,
};
/// Wire-facing container for the D-Bus `GetSnapshot` reply.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SnapshotPayload {
    /// One entry per tracked instance.
    pub vms: Vec<SnapshotVm>,
}

/// One tracked instance's freshest state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SnapshotVm {
    /// Instance / vm identifier.
    pub id: String,
    /// Guest vCPU count reported by the backend.
    pub vcpu_count: u32,
    /// Backend worker PID.
    pub pid: i32,
    /// Current effective thread-pool size.
    pub thread_count: u32,
    /// Whether the most recent refresh succeeded.
    pub alive: bool,
    /// Whether the backend supplied trustworthy performance data.
    pub perf_available: bool,
    /// Average utilisation per worker, as a 0..1 fraction.
    pub per_thread_util: f64,
    /// Whether a sticky manual override suppresses automatic scaling.
    pub manual_scaling_sticky: bool,
    /// Per-tick read operation rate.
    pub read_iops: u64,
    /// Per-tick write operation rate.
    pub write_iops: u64,
    /// Per-tick non-read/write operation rate.
    pub other_iops: u64,
    /// Per-tick read bandwidth in bytes per second.
    pub read_bw_bps: u64,
    /// Per-tick write bandwidth in bytes per second.
    pub write_bw_bps: u64,
    /// Read-latency histogram digest, when available.
    #[serde(default)]
    pub read_latency_us: Option<SnapshotLatency>,
    /// Write-latency histogram digest, when available.
    #[serde(default)]
    pub write_latency_us: Option<SnapshotLatency>,
    /// Number of queues represented by `per_vq_depth`.
    #[serde(default)]
    pub num_queues: Option<u32>,
    /// One in-flight depth reading per virtqueue.
    #[serde(default)]
    pub per_vq_depth: Option<Vec<u64>>,
    /// Aggregate virtqueue depth.
    #[serde(default)]
    pub qd_total: Option<u64>,
    /// Average virtqueue depth multiplied by 100.
    #[serde(default)]
    pub qd_avg_x100: Option<u64>,
    /// Median virtqueue depth.
    #[serde(default)]
    pub qd_median: Option<u64>,
    /// Mean per-worker CPU utilisation, in percent.
    #[serde(default)]
    pub cpu_pct_avg: Option<u64>,
    /// Median per-worker CPU utilisation, in percent.
    #[serde(default)]
    pub cpu_pct_median: Option<u64>,
    /// Sum of per-worker CPU utilisation, in percent.
    #[serde(default)]
    pub cpu_pct_total: Option<u64>,
}

/// Serialised latency histogram digest.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SnapshotLatency {
    /// Median latency in microseconds.
    pub p50: u64,
    /// 95th-percentile latency in microseconds.
    pub p95: u64,
    /// 99th-percentile latency in microseconds.
    pub p99: u64,
    /// Histogram-derived arithmetic mean in microseconds.
    pub avg: u64,
}

#[derive(Debug, Error)]
pub enum ControllerError {
    #[error(transparent)]
    BackendClient(#[from] BackendClientError),

    #[error(transparent)]
    Proc(#[from] ProcError),

    #[error(transparent)]
    State(#[from] StateError),

    #[error("Set thread count error: {0}")]
    ThreadCountError(String),

    #[error("VM error: {0}")]
    VmError(String),
}

/// Host-wide cumulative CPU counters from `/proc/stat`.
#[derive(Debug, Default, Clone, Copy)]
struct HostCpuSample {
    busy_ticks: u64,
    total_ticks: u64,
}

impl HostCpuSample {
    /// Read aggregate host CPU counters.
    fn read() -> Result<Self, ProcError> {
        let total = procfs::KernelStats::current()?.total;
        let idle_ticks = total.idle.saturating_add(total.iowait.unwrap_or(0));
        let total_ticks = total
            .user
            .saturating_add(total.nice)
            .saturating_add(total.system)
            .saturating_add(total.idle)
            .saturating_add(total.iowait.unwrap_or(0))
            .saturating_add(total.irq.unwrap_or(0))
            .saturating_add(total.softirq.unwrap_or(0))
            .saturating_add(total.steal.unwrap_or(0))
            .saturating_add(total.guest.unwrap_or(0))
            .saturating_add(total.guest_nice.unwrap_or(0));
        Ok(Self {
            busy_ticks: total_ticks.saturating_sub(idle_ticks),
            total_ticks,
        })
    }

    /// Fraction of host CPU that was busy between `previous` and `self`.
    fn utilisation_since(self, previous: Self) -> f64 {
        let delta_busy = self.busy_ticks.saturating_sub(previous.busy_ticks) as f64;
        let delta_total = self.total_ticks.saturating_sub(previous.total_ticks) as f64;
        if delta_total == 0.0 {
            0.0
        } else {
            (delta_busy / delta_total).clamp(0.0, 1.0)
        }
    }
}

/// Tracks host CPU utilisation across successive ticks.
#[derive(Debug, Default)]
struct HostCpuMonitor {
    /// Previous host CPU sample.
    last: Option<HostCpuSample>,
    /// Fraction of host CPU consumed between the last two samples; zero until
    /// two samples exist.
    util: f64,
}

impl HostCpuMonitor {
    /// Take a new sample and return the latest utilisation.
    ///
    /// A failed read keeps the previous value.
    fn sample(&mut self) -> f64 {
        if let Ok(current) = HostCpuSample::read() {
            if let Some(previous) = self.last {
                self.util = current.utilisation_since(previous);
            }
            self.last = Some(current);
        }
        self.util
    }
}

/// Live VM inventory and per-tick driver.
pub struct Controller {
    /// Effective daemon configuration.
    pub cfg: Config,
    /// Exclusively owned engine borrowed by concurrent evaluation futures.
    ///
    /// The controller does not need shared ownership: fleet evaluation lends
    /// `&self.engine` to concurrent calls, which is safe because
    /// [`ScalingEngine`] requires both [`Send`] and [`Sync`].
    pub engine: Box<dyn ScalingEngine>,
    /// Atomic ownership-registry store.
    pub vm_state_store: VmStateStore,
    /// Restart-surviving managed and unmanaged VM sets, loaded once.
    pub vm_ownership: VmOwnership,
    /// Tracked VMs keyed by stable identifier.
    ///
    /// The controller needs to pass Instances to the engine for it to evaluate
    /// and decide.
    // FIXME Instance probably doesn't need to be an Arc.
    pub instances: HashMap<String, Arc<Instance>>,
    /// Monotonically increasing tick sequence.
    pub tick_index: u64,
    /// Host CPU utilisation used by the scale-up ceiling guard.
    host_cpu: HostCpuMonitor,
}

impl Controller {
    /// Construct an empty controller after loading VM ownership once.
    pub fn new(cfg: Config, engine: Box<dyn ScalingEngine>) -> Result<Self, ControllerError> {
        let vm_state_store = VmStateStore::new(cfg.vm_state_path.clone());
        let vm_ownership = vm_state_store.load()?;
        Ok(Self {
            cfg,
            engine,
            vm_state_store,
            vm_ownership,
            instances: HashMap::new(),
            tick_index: 0,
            host_cpu: HostCpuMonitor::default(),
        })
    }

    /// Reconcile a complete discovery result with the live inventory. Returns
    /// the number added and removed instances.
    pub async fn sync_instances(
        &mut self,
        discovered: Vec<Arc<Instance>>,
    ) -> Result<(usize, usize), ControllerError> {
        let seen: HashSet<_> = discovered
            .iter()
            .map(|instance| instance.id.clone())
            .collect();
        let mut added = 0;

        for instance in discovered {
            if self.instances.contains_key(&instance.id) {
                instance.client.close().await;
                continue;
            }
            let classification = self.vm_ownership.classification(&instance.id);
            let mut status = instance.status.write().await;
            status.ownership_classification = classification;
            drop(status);
            self.engine.on_instance_added(&instance).await;
            self.instances.insert(instance.id.clone(), instance);
            added += 1;
        }

        let stale: Vec<_> = self
            .instances
            .keys()
            .filter(|id| !seen.contains(*id))
            .cloned()
            .collect();
        let removed = stale.len();
        for id in stale {
            self.remove_instance(&id).await;
        }
        Ok((added, removed))
    }

    /// Stop tracking a VM, closing its client and notifying the engine.
    async fn remove_instance(&mut self, id: &str) {
        if let Some(instance) = self.instances.remove(id) {
            instance.client.close().await;
            self.engine.on_instance_removed(id).await;
        }
    }

    /// Serve one D-Bus request in-line with the tick loop.
    pub async fn handle_dbus_request(&self, request: DbusRequest) {
        match request {
            DbusRequest::SetThreadCount {
                vm,
                threads,
                sticky,
                reply,
            } => {
                let result = self
                    .handle_set_thread_count(&vm, threads, sticky)
                    .await
                    .map_err(|error| error.to_string());
                let _ = reply.send(result);
            }
            DbusRequest::GetSnapshot { reply } => {
                let _ = reply.send(self.handle_get_snapshot().await);
            }
            DbusRequest::GetVersion { reply } => {
                let _ = reply.send(VERSION.to_string());
            }
            DbusRequest::GetStats { reply } => {
                let _ = reply.send(self.handle_get_stats().await);
            }
            DbusRequest::GetIoThreadVqMapping { vm, device, reply } => {
                let _ = reply.send(self.handle_get_io_thread_vq_mapping(&vm, &device).await);
            }
            DbusRequest::AddIoThread {
                vm,
                id,
                poll_max_ns,
                reply,
            } => {
                let _ = reply.send(self.handle_add_io_thread(&vm, &id, poll_max_ns).await);
            }
            DbusRequest::DelIoThread { vm, id, reply } => {
                let _ = reply.send(self.handle_del_io_thread(&vm, &id).await);
            }
            DbusRequest::SetIoThreadVqMapping {
                vm,
                device,
                mapping_json,
                reply,
            } => {
                let _ = reply.send(
                    self.handle_set_io_thread_vq_mapping(&vm, &device, &mapping_json)
                        .await,
                );
            }
        }
    }

    /// Look up a tracked VM for a D-Bus request.
    fn instance(&self, vm: &str) -> Result<&Arc<Instance>, String> {
        self.instances
            .get(vm)
            .ok_or_else(|| format!("unknown VM {vm}"))
    }

    /// Assemble the `GetStats` JSON reply.
    async fn handle_get_stats(&self) -> String {
        let mut vms = Vec::with_capacity(self.instances.len());
        for (id, instance) in &self.instances {
            let status = instance.status.read().await;
            let (read_io_count, write_io_count, other_io_count) = match status.perf {
                Some(perf) => (perf.read_io_count, perf.write_io_count, perf.other_io_count),
                None => (0, 0, 0),
            };
            vms.push(serde_json::json!({
                "vm": id,
                "thread_count": status.thread_count,
                "manual_scaling_sticky": status.manual_scaling_sticky,
                "scaling_allowed": status.scaling_allowed(),
                "vcpu_count": status.vcpu_count,
                "per_thread_util": status.per_thread_util,
                // FIXME omit if status.perf.is_none()?
                "read_io_count": read_io_count,
                "write_io_count": write_io_count,
                "other_io_count": other_io_count,
            }));
        }
        serde_json::json!({
            "tick": self.tick_index,
            "vms": vms,
        })
        .to_string()
    }

    /// Read a device's virtqueue mapping as JSON.
    async fn handle_get_io_thread_vq_mapping(
        &self,
        vm: &str,
        device: &str,
    ) -> Result<String, String> {
        let mapping = self
            .instance(vm)?
            .client
            .get_io_thread_vq_mapping(device)
            .await
            .map_err(|error| error.to_string())?;
        serde_json::to_string(&mapping).map_err(|error| format!("serialize mapping: {error}"))
    }

    /// Create a named IOThread; a negative `poll_max_ns` keeps the default.
    async fn handle_add_io_thread(
        &self,
        vm: &str,
        id: &str,
        poll_max_ns: i64,
    ) -> Result<(), String> {
        let instance = self.instance(vm)?;
        let properties = (poll_max_ns >= 0).then(|| IoThreadProperties {
            poll_max_ns: Some(poll_max_ns),
            ..Default::default()
        });
        instance
            .client
            .add_io_thread(id, properties.as_ref())
            .await
            .map_err(|error| error.to_string())
    }

    /// Delete a named IOThread.
    async fn handle_del_io_thread(&self, vm: &str, id: &str) -> Result<(), String> {
        self.instance(vm)?
            .client
            .del_io_thread(id)
            .await
            .map_err(|error| error.to_string())
    }

    /// Replace a device's virtqueue mapping from its JSON encoding.
    async fn handle_set_io_thread_vq_mapping(
        &self,
        vm: &str,
        device: &str,
        mapping_json: &str,
    ) -> Result<(), String> {
        let mapping: Vec<VqMapping> = serde_json::from_str(mapping_json)
            .map_err(|error| format!("parse mapping JSON: {error}"))?;
        self.instance(vm)?
            .client
            .set_io_thread_vq_mapping(device, &mapping)
            .await
            .map_err(|error| error.to_string())
    }

    /// Apply a debug-only manual thread-count request.
    async fn handle_set_thread_count(
        &self,
        vm: &str,
        threads: u32,
        sticky: bool,
    ) -> Result<(), ControllerError> {
        let instance = self
            .instances
            .get(vm)
            .cloned()
            .ok_or_else(|| ControllerError::VmError(format!("unknown VM {vm}")))?;
        let vcpu_count = instance.status.read().await.vcpu_count;
        if threads == 0 {
            return Err(ControllerError::ThreadCountError(
                "target thread count must be greater than zero".to_string(),
            ));
        }
        if threads > vcpu_count {
            return Err(ControllerError::ThreadCountError(format!(
                "target thread count {threads} > guest vCPU count {vcpu_count}"
            )));
        }

        instance.client.set_thread_count(threads).await?;
        let mut status = instance.status.write().await;
        status.thread_count = threads;
        status.manual_scaling_sticky = sticky;
        Ok(())
    }

    /// Assemble the machine-readable fleet snapshot.
    async fn handle_get_snapshot(&self) -> String {
        let mut payload = SnapshotPayload {
            vms: Vec::with_capacity(self.instances.len()),
        };
        let ids: Vec<_> = self.instances.keys().collect();
        for id in ids {
            let instance = &self.instances[id];
            let s = instance.status.read().await;
            let cpu_stats = s.cpu_stats();
            payload.vms.push(SnapshotVm {
                id: id.clone(),
                vcpu_count: s.vcpu_count,
                pid: instance.pid,
                thread_count: s.thread_count,
                alive: s.alive,
                perf_available: s.perf.is_some(),
                // Guard JSON serialization: NaN/inf are not valid JSON numbers.
                per_thread_util: if s.per_thread_util.is_finite() {
                    s.per_thread_util
                } else {
                    0.0
                },
                manual_scaling_sticky: s.manual_scaling_sticky,
                read_iops: s.read_iops,
                write_iops: s.write_iops,
                other_iops: s.other_iops,
                read_bw_bps: s.read_bytes_per_second,
                write_bw_bps: s.write_bytes_per_second,
                read_latency_us: None,
                write_latency_us: None,
                num_queues: None,
                per_vq_depth: None,
                qd_total: None,
                qd_avg_x100: None,
                qd_median: None,
                cpu_pct_avg: Some(cpu_stats.avg_pct),
                cpu_pct_median: Some(cpu_stats.median_pct),
                cpu_pct_total: Some(cpu_stats.total_pct),
            });
        }
        serde_json::to_string(&payload).unwrap_or_else(|e| format!("{{\"error\":\"{e}\"}}"))
    }

    /// Refresh the fleet, evaluate one coherent plan, and apply eligible work.
    pub async fn tick(&mut self) -> Result<(), ControllerError> {
        self.tick_index = self.tick_index.wrapping_add(1);
        let host_cpu_util = self.host_cpu.sample();
        let context = EngineTickContext {
            now: Instant::now(),
            min_thread_count: self.cfg.min_thread_count,
            max_thread_count: self.cfg.max_thread_count,
            host_cpu_util,
            tick_index: self.tick_index,
        };
        let fleet: Vec<_> = self.instances.values().cloned().collect();
        let refreshes = join_all(fleet.iter().map(|instance| instance.refresh_state())).await;

        for (instance, refreshed) in fleet.iter().zip(refreshes) {
            if !refreshed {
                self.remove_instance(&instance.id).await;
            }
        }

        let fleet: Vec<_> = self.instances.values().cloned().collect();
        self.persist_new_classifications(&fleet).await?;
        let plan = self.engine.evaluate_fleet(&fleet, &context).await;
        for item in plan {
            if let Some(instance) = self.instances.get(&item.instance_id) {
                self.apply_engine_decision(instance, item.decision).await?;
            }
        }
        status::emit_status_lines(&self.cfg, &self.instances).await;
        Ok(())
    }

    /// Commit classifications produced by first successful backend snapshots.
    async fn persist_new_classifications(
        &mut self,
        fleet: &[Arc<Instance>],
    ) -> Result<(), ControllerError> {
        let mut changed = false;
        for instance in fleet {
            let classification = instance.status.read().await.ownership_classification;
            let Some(managed) = classification else {
                continue;
            };
            match self.vm_ownership.classification(&instance.id) {
                Some(persisted) => {
                    if persisted != managed {
                        return Err(ControllerError::VmError(format!(
                            "VM {} ownership changed after classification",
                            instance.id
                        )));
                    }
                }
                None => {
                    self.vm_ownership.record(&instance.id, managed)?;
                    changed = true;
                }
            }
        }
        if changed {
            self.vm_state_store.save(&self.vm_ownership)?;
        }
        Ok(())
    }

    /// Apply one engine action after controller safety checks.
    async fn apply_engine_decision(
        &self,
        instance: &Instance,
        action: ScaleAction,
    ) -> Result<(), ControllerError> {
        let Some(target) = action.target() else {
            return Ok(());
        };

        let status = instance.status.read().await;
        let previous_count = status.thread_count;
        let previous_io_count = match status.perf {
            Some(perf) => perf.total_io_count(),
            None => 0,
        };
        let blocked = self.blocked_reason(&status, action, target, Instant::now());
        drop(status);

        if let Some(reason) = blocked {
            self.report_blocked_scale(&instance.id, action, reason)
                .await;
            tracing::info!(
                target: "controller",
                event = "scale_blocked",
                vm = %instance.id,
                action = %action,
                target,
                %reason
            );
            return Ok(());
        }

        if self.cfg.dry_run {
            tracing::info!(
                target: "controller",
                vm = %instance.id,
                action = %action,
                target,
                "dry-run: would scale"
            );
        } else if let Err(error) = instance.client.set_thread_count(target).await {
            let error_text = error.to_string();
            self.engine
                .on_applied(&instance.id, AppliedOutcome::Failed { action, error })
                .await;
            tracing::warn!(
                target: "controller",
                event = "scale_failed",
                vm = %instance.id,
                action = %action,
                target,
                error = %error_text
            );
            return Ok(());
        }

        {
            let mut status = instance.status.write().await;
            if !self.cfg.dry_run {
                status.thread_count = target;
            }
            if action.is_ordinary() {
                status.cooldown_until =
                    Some(Instant::now() + Duration::from_secs_f64(self.cfg.cooldown_secs));
            }
        }
        let outcome = if self.cfg.dry_run {
            AppliedOutcome::DryRun {
                action,
                prev_thread_count: previous_count,
                prev_io_count_total: previous_io_count,
            }
        } else {
            AppliedOutcome::Success {
                action,
                prev_thread_count: previous_count,
                prev_io_count_total: previous_io_count,
            }
        };
        self.engine.on_applied(&instance.id, outcome).await;
        if !self.cfg.dry_run {
            tracing::info!(
                target: "controller",
                event = "scale_applied",
                vm = %instance.id,
                action = %action,
                target,
                prev_thread_count = previous_count,
                prev_io_count_total = previous_io_count
            );
        }
        Ok(())
    }

    /// Return the first controller guard that forbids applying `action`.
    fn blocked_reason(
        &self,
        status: &InstanceStatus,
        action: ScaleAction,
        target: u32,
        now: Instant,
    ) -> Option<BlockedReason> {
        if status.manual_scaling_sticky {
            return Some(BlockedReason::ManualOverride);
        }
        if !status.scaling_allowed() {
            return Some(BlockedReason::UnmanagedVm);
        }
        if target < self.cfg.min_thread_count {
            return Some(BlockedReason::TargetBelowMinimum);
        }
        if target > self.cfg.max_thread_count {
            return Some(BlockedReason::TargetExceedsMaximum);
        }
        if target > status.vcpu_count {
            return Some(BlockedReason::TargetExceedsVcpuCount);
        }
        if action.is_ordinary() && status.cooldown_until.is_some_and(|until| now < until) {
            return Some(BlockedReason::Cooldown);
        }
        // FIXME This condition allows ScaleAction::Down(...) with an increasing target
        // to bypass the ceiling. This check should be removed or Down(...) should be
        // blocked from having targets greater than the original.
        if target > status.thread_count
            && matches!(action, ScaleAction::Up(_))
            && self.cfg.host_cpu_scale_up_ceiling > 0.0
            && self.host_cpu.util >= self.cfg.host_cpu_scale_up_ceiling
        {
            return Some(BlockedReason::HostCpuCeiling);
        }
        None
    }

    /// Report a controller-blocked action to the proposing engine.
    async fn report_blocked_scale(
        &self,
        instance_id: &str,
        action: ScaleAction,
        reason: BlockedReason,
    ) {
        self.engine
            .on_applied(instance_id, AppliedOutcome::Blocked { action, reason })
            .await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicU32, AtomicUsize, Ordering},
    };

    use async_trait::async_trait;
    use test_log::test;

    use super::*;
    use crate::{
        backends::BackendClientError,
        engines::{EngineTickContext, ScalingEngine},
        instance::{InstanceClient, ThreadPoolSnapshot},
        util::Path,
    };

    #[cfg(feature = "threshold-engine")]
    use crate::engines::threshold::{ThresholdConfig, ThresholdEngine};

    struct VcpuLimitedClient {
        target: Arc<AtomicU32>,
    }

    #[async_trait]
    impl InstanceClient for VcpuLimitedClient {
        async fn set_thread_count(&self, count: u32) -> Result<(), BackendClientError> {
            self.target.store(count, Ordering::Relaxed);
            Ok(())
        }

        async fn get_thread_pool_snapshot(&self) -> Result<ThreadPoolSnapshot, BackendClientError> {
            Ok(ThreadPoolSnapshot {
                thread_count: self.target.load(Ordering::Relaxed),
                vcpu_count: 4,
                perf: None,
                per_thread_util: None,
            })
        }

        async fn close(&self) {}
    }

    /// Test that actuation will not raise the pool above the VM vCPU
    /// count.
    #[test(tokio::test)]
    async fn actuation_enforces_vcpu_cap() {
        let state_dir = tempfile::tempdir().unwrap();
        let cfg = Config {
            vm_state_path: Path::new(&state_dir.path().join("ownership.json")),
            cooldown_secs: 0.0,
            ..Default::default()
        };
        let target = Arc::new(AtomicU32::new(2));
        let client = VcpuLimitedClient {
            target: Arc::clone(&target),
        };
        let instance = Arc::new(Instance::new(
            "some-vcpu-limited-test-instance".to_string(),
            Path::new(""),
            0,
            client,
        ));
        {
            let mut status = instance.status.write().await;
            status.thread_count = 2;
            status.vcpu_count = 4;
            status.ownership_classification = Some(true);
        }
        let mut controller = Controller::new(
            cfg,
            Box::new(ThresholdEngine::new(ThresholdConfig::default())),
        )
        .unwrap();
        controller
            .instances
            .insert(instance.id.clone(), Arc::clone(&instance));

        controller
            .apply_engine_decision(&instance, ScaleAction::Up(4))
            .await
            .unwrap();
        assert_eq!(target.load(Ordering::Relaxed), 4);

        controller
            .apply_engine_decision(&instance, ScaleAction::Up(5))
            .await
            .unwrap();
        assert_eq!(target.load(Ordering::Relaxed), 4);

        controller
            .apply_engine_decision(&instance, ScaleAction::Down(3))
            .await
            .unwrap();
        assert_eq!(target.load(Ordering::Relaxed), 3);

        instance.status.write().await.ownership_classification = Some(false);
        controller
            .apply_engine_decision(&instance, ScaleAction::Up(4))
            .await
            .unwrap();
        controller
            .apply_engine_decision(&instance, ScaleAction::Down(2))
            .await
            .unwrap();
        assert_eq!(target.load(Ordering::Relaxed), 3);
    }

    /// Test that dry-run skips actuation but still starts cooldown.
    #[test(tokio::test)]
    async fn dry_run_skips_actuation_and_starts_cooldown() {
        let state_dir = tempfile::tempdir().unwrap();
        let cfg = Config {
            vm_state_path: Path::new(&state_dir.path().join("ownership.json")),
            dry_run: true,
            ..Default::default()
        };
        let target = Arc::new(AtomicU32::new(2));
        let instance = Arc::new(Instance::new(
            "dry-run".to_string(),
            Path::new(""),
            0,
            VcpuLimitedClient {
                target: Arc::clone(&target),
            },
        ));
        {
            let mut status = instance.status.write().await;
            status.thread_count = 2;
            status.vcpu_count = 4;
            status.ownership_classification = Some(true);
        }
        let mut controller = Controller::new(
            cfg,
            Box::new(ThresholdEngine::new(ThresholdConfig::default())),
        )
        .unwrap();
        controller
            .instances
            .insert(instance.id.clone(), Arc::clone(&instance));

        controller
            .apply_engine_decision(&instance, ScaleAction::Up(3))
            .await
            .unwrap();

        assert_eq!(target.load(Ordering::Relaxed), 2);
        let status = instance.status.read().await;
        assert_eq!(status.thread_count, 2);
        assert!(status.cooldown_until.is_some());
    }

    fn guard_controller() -> (tempfile::TempDir, Controller) {
        let state_dir = tempfile::tempdir().unwrap();
        let cfg = Config {
            vm_state_path: Path::new(&state_dir.path().join("ownership.json")),
            min_thread_count: 2,
            max_thread_count: 6,
            host_cpu_scale_up_ceiling: 0.5,
            ..Default::default()
        };
        let controller = Controller::new(
            cfg,
            Box::new(ThresholdEngine::new(ThresholdConfig::default())),
        )
        .unwrap();
        (state_dir, controller)
    }

    /// A managed, idle VM with 3 of 4 vCPUs' worth of workers.
    fn guard_status() -> InstanceStatus {
        InstanceStatus {
            thread_count: 3,
            vcpu_count: 4,
            ownership_classification: Some(true),
            ..Default::default()
        }
    }

    /// Test that each controller guard maps to its `BlockedReason`, in
    /// priority order.
    #[test]
    fn blocked_reason_guards() {
        type Tweak = fn(&mut InstanceStatus);
        let in_cooldown: Tweak =
            |s| s.cooldown_until = Some(Instant::now() + Duration::from_secs(60));
        let cases: [(&str, Tweak, ScaleAction, f64, Option<BlockedReason>); 11] = [
            ("allowed", |_| {}, ScaleAction::Up(4), 0.0, None),
            (
                "sticky",
                |s| s.manual_scaling_sticky = true,
                ScaleAction::Up(4),
                0.0,
                Some(BlockedReason::ManualOverride),
            ),
            (
                "unmanaged",
                |s| s.ownership_classification = Some(false),
                ScaleAction::Up(4),
                0.0,
                Some(BlockedReason::UnmanagedVm),
            ),
            (
                "unclassified",
                |s| s.ownership_classification = None,
                ScaleAction::Up(4),
                0.0,
                Some(BlockedReason::UnmanagedVm),
            ),
            (
                "below minimum",
                |_| {},
                ScaleAction::Down(1),
                0.0,
                Some(BlockedReason::TargetBelowMinimum),
            ),
            (
                "above maximum",
                |s| s.vcpu_count = 16,
                ScaleAction::Up(7),
                0.0,
                Some(BlockedReason::TargetExceedsMaximum),
            ),
            (
                "above vCPUs",
                |_| {},
                ScaleAction::Up(5),
                0.0,
                Some(BlockedReason::TargetExceedsVcpuCount),
            ),
            (
                "cooldown",
                in_cooldown,
                ScaleAction::Down(2),
                0.0,
                Some(BlockedReason::Cooldown),
            ),
            (
                "revert ignores cooldown",
                in_cooldown,
                ScaleAction::Revert(2),
                0.0,
                None,
            ),
            (
                "host CPU ceiling",
                |_| {},
                ScaleAction::Up(4),
                0.5,
                Some(BlockedReason::HostCpuCeiling),
            ),
            (
                "host CPU allows down",
                |_| {},
                ScaleAction::Down(2),
                0.9,
                None,
            ),
        ];

        let (_state_dir, mut controller) = guard_controller();
        for (name, tweak, action, host_cpu_util, expected) in cases {
            controller.host_cpu.util = host_cpu_util;
            let mut status = guard_status();
            tweak(&mut status);
            let target = action.target().unwrap();
            assert_eq!(
                controller.blocked_reason(&status, action, target, Instant::now()),
                expected,
                "{name}"
            );
        }
    }

    struct SnapshotClient {
        threads: u32,
        closed: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl InstanceClient for SnapshotClient {
        async fn set_thread_count(&self, _count: u32) -> Result<(), BackendClientError> {
            Ok(())
        }

        async fn get_thread_pool_snapshot(&self) -> Result<ThreadPoolSnapshot, BackendClientError> {
            Ok(ThreadPoolSnapshot {
                thread_count: self.threads,
                // FIXME these weren't required, looked like broken due to rebase
                vcpu_count: 2,
                perf: None,
                per_thread_util: None,
            })
        }

        async fn close(&self) {
            self.closed.fetch_add(1, Ordering::Relaxed);
        }
    }

    struct FailingClient {
        closed: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl InstanceClient for FailingClient {
        async fn set_thread_count(&self, _count: u32) -> Result<(), BackendClientError> {
            Ok(())
        }

        async fn get_thread_pool_snapshot(&self) -> Result<ThreadPoolSnapshot, BackendClientError> {
            Err(BackendClientError::Transport("boom".into()))
        }

        async fn close(&self) {
            self.closed.fetch_add(1, Ordering::Relaxed);
        }
    }

    struct SharedEngine {
        added: Arc<AtomicUsize>,
        removed: Arc<AtomicUsize>,
        evaluated: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl ScalingEngine for SharedEngine {
        fn name(&self) -> &'static str {
            "shared"
        }

        fn dump_config(&self) -> serde_json::Value {
            serde_json::Value::Null
        }

        async fn evaluate(
            &self,
            _instance: &Arc<Instance>,
            _context: &EngineTickContext,
        ) -> ScaleAction {
            self.evaluated.fetch_add(1, Ordering::Relaxed);
            ScaleAction::None
        }

        async fn on_instance_added(&self, _instance: &Arc<Instance>) {
            self.added.fetch_add(1, Ordering::Relaxed);
        }

        async fn on_instance_removed(&self, _instance_id: &str) {
            self.removed.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn instance(id: &str, threads: u32, closed: Arc<AtomicUsize>) -> Arc<Instance> {
        Arc::new(Instance::new(
            id.to_string(),
            Path::new(""),
            1,
            SnapshotClient { threads, closed },
        ))
    }

    /// Test that discovery sync adds new VMs, retains existing ones,
    /// and removes disappeared ones.
    #[tokio::test]
    async fn sync_instances_adds_retains_and_removes() {
        let added = Arc::new(AtomicUsize::new(0));
        let removed = Arc::new(AtomicUsize::new(0));
        let evaluated = Arc::new(AtomicUsize::new(0));
        let state_dir = tempfile::tempdir().unwrap();
        let cfg = Config {
            vm_state_path: Path::new(&state_dir.path().join("ownership.json")),
            ..Default::default()
        };
        let mut controller = Controller::new(
            cfg,
            Box::new(SharedEngine {
                added: Arc::clone(&added),
                removed: Arc::clone(&removed),
                evaluated: Arc::clone(&evaluated),
            }),
        )
        .unwrap();
        let closed = Arc::new(AtomicUsize::new(0));

        let first = instance("vm-a", 1, Arc::clone(&closed));
        let (n_added, n_removed) = controller
            .sync_instances(vec![Arc::clone(&first)])
            .await
            .unwrap();
        assert_eq!((n_added, n_removed), (1, 0));
        assert_eq!(controller.instances.len(), 1);
        assert_eq!(added.load(Ordering::Relaxed), 1);

        let duplicate = instance("vm-a", 9, Arc::clone(&closed));
        let (n_added, n_removed) = controller.sync_instances(vec![duplicate]).await.unwrap();
        assert_eq!((n_added, n_removed), (0, 0));
        assert_eq!(controller.instances.len(), 1);
        assert_eq!(closed.load(Ordering::Relaxed), 1);

        let (n_added, n_removed) = controller.sync_instances(vec![]).await.unwrap();
        assert_eq!((n_added, n_removed), (0, 1));
        assert!(controller.instances.is_empty());
        assert_eq!(removed.load(Ordering::Relaxed), 1);
        assert_eq!(evaluated.load(Ordering::Relaxed), 0);
    }

    /// Test that manual thread-count errors name the offending VM and
    /// values.
    #[tokio::test]
    async fn set_thread_count_errors_include_context() {
        let state_dir = tempfile::tempdir().unwrap();
        let cfg = Config {
            vm_state_path: Path::new(&state_dir.path().join("ownership.json")),
            ..Default::default()
        };
        let mut controller = Controller::new(
            cfg,
            Box::new(ThresholdEngine::new(ThresholdConfig::default())),
        )
        .unwrap();
        let closed = Arc::new(AtomicUsize::new(0));
        let vm = instance("vm-a", 1, closed);
        vm.status.write().await.vcpu_count = 2;
        controller.sync_instances(vec![vm]).await.unwrap();

        let error = controller
            .handle_set_thread_count("vm-missing", 1, false)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("unknown VM vm-missing"), "{error}");

        let error = controller
            .handle_set_thread_count("vm-a", 3, false)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("3 > guest vCPU count 2"), "{error}");
    }

    /// Test that one tick refreshes state, runs the engine, and drops
    /// and closes instances that fail refresh.
    #[tokio::test]
    async fn tick_refreshes_evaluates_and_drops_failed_instances() {
        let added = Arc::new(AtomicUsize::new(0));
        let removed = Arc::new(AtomicUsize::new(0));
        let evaluated = Arc::new(AtomicUsize::new(0));
        let state_dir = tempfile::tempdir().unwrap();
        let cfg = Config {
            vm_state_path: Path::new(&state_dir.path().join("ownership.json")),
            ..Default::default()
        };
        let mut controller = Controller::new(
            cfg,
            Box::new(SharedEngine {
                added: Arc::clone(&added),
                removed: Arc::clone(&removed),
                evaluated: Arc::clone(&evaluated),
            }),
        )
        .unwrap();

        let closed = Arc::new(AtomicUsize::new(0));
        let ok = instance("ok", 4, Arc::clone(&closed));
        let bad = Arc::new(Instance::new(
            "bad".to_string(),
            Path::new(""),
            2,
            FailingClient {
                closed: Arc::clone(&closed),
            },
        ));
        controller.sync_instances(vec![ok, bad]).await.unwrap();
        assert_eq!(added.load(Ordering::Relaxed), 2);

        controller.tick().await.unwrap();
        assert_eq!(controller.tick_index, 1);
        assert_eq!(controller.instances.len(), 1);
        assert!(controller.instances.contains_key("ok"));
        assert_eq!(
            controller.instances["ok"].status.read().await.thread_count,
            4
        );
        assert_eq!(evaluated.load(Ordering::Relaxed), 1);
        assert_eq!(removed.load(Ordering::Relaxed), 1);
        assert_eq!(closed.load(Ordering::Relaxed), 1);
    }
}
