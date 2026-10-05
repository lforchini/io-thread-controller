// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Nutanix, Inc.
//
// Author: Thanos Makatos <thanos.makatos@nutanix.com>

//! D-Bus surface for operator-facing verbs.
//!
//! The module defines a flat request enum ([`DbusRequest`]) and hands each
//! request to the controller over an mpsc channel. The controller finds the
//! target VM and dispatches through its [`crate::instance::InstanceClient`].
//!
//! Verbs currently exposed:
//!   * debug-only `SetThreadCount(vm, threads, sticky)`;
//!   * read-only `GetStats()`;
//!   * read-only `GetVersion()`;
//!   * named-IOThread and virtqueue-mapping operations for clients that support
//!     them.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

/// D-Bus well-known bus name the daemon claims at startup.
pub const DBUS_BUS_NAME: &str = "com.nutanix.io_thread_controller1";
/// Object path served by the daemon on the system bus.
pub const DBUS_OBJECT_PATH: &str = "/com/nutanix/io_thread_controller1";
/// Interface name the operator verbs live on.
pub const DBUS_INTERFACE: &str = "com.nutanix.io_thread_controller1";
/// Upper bound on how long a D-Bus verb waits for the engine
/// task to service the request before failing the call.
pub const DBUS_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Cross-thread message shape: each variant matches one D-Bus
/// method. The engine task pulls these off, services them
/// serially, and replies through the embedded oneshot channel.
#[derive(Debug)]
pub enum DbusRequest {
    /// Operator-driven `SetThreadCount` verb; asks the engine
    /// to walk the addressed instance to exactly `threads`.
    SetThreadCount {
        /// Instance / vm id to address.
        vm: String,
        /// Target thread count.
        threads: u32,
        /// If true, the engine remembers the manual override and
        /// suppresses automatic scaling on this instance until
        /// cleared.
        sticky: bool,
        /// One-shot channel used to complete the D-Bus method call.
        reply: oneshot::Sender<Result<(), String>>,
    },
    /// Structured snapshot of every tracked instance. The reply is a JSON
    /// string so the wire signature stays a bare `s` and the payload shape can
    /// evolve without D-Bus IDL churn.  Field contract is documented on
    /// JSON snapshot of all tracked VMs.
    GetStats {
        /// Reply channel carrying the serialized snapshot.
        reply: oneshot::Sender<String>,
    },
    /// [`SnapshotPayload`].
    GetSnapshot {
        /// Reply channel; JSON-encoded
        /// [`SnapshotPayload`] on success.
        reply: oneshot::Sender<String>,
    },
    /// Return the controller binary version string.
    GetVersion {
        /// Reply channel carrying the version string.
        reply: oneshot::Sender<String>,
    },
    /// Read the current virtqueue-to-IOThread mapping.
    GetIoThreadVqMapping {
        /// Instance / vm id to address.
        vm: String,
        /// QOM device path whose mapping should be read.
        device: String,
        /// Reply channel carrying a JSON-encoded mapping.
        reply: oneshot::Sender<Result<String, String>>,
    },
    /// Create a named IOThread object.
    AddIoThread {
        /// Instance / vm id to address.
        vm: String,
        /// QOM identifier of the new IOThread.
        id: String,
        /// poll-max-ns override, or `-1` for "leave default".
        poll_max_ns: i64,
        /// Reply channel carrying success or an error message.
        reply: oneshot::Sender<Result<(), String>>,
    },
    /// Delete a named IOThread object.
    DelIoThread {
        /// Instance / vm id to address.
        vm: String,
        /// QOM identifier of the IOThread to remove.
        id: String,
        /// Reply channel carrying success or an error message.
        reply: oneshot::Sender<Result<(), String>>,
    },
    /// Replace a device's virtqueue-to-IOThread mapping.
    SetIoThreadVqMapping {
        /// Instance / vm id to address.
        vm: String,
        /// QOM device path whose mapping should be replaced.
        device: String,
        /// JSON-encoded `Vec<VqMapping>` (kept as a string so
        /// the D-Bus signature stays trivial).
        mapping_json: String,
        /// Reply channel carrying success or an error message.
        reply: oneshot::Sender<Result<(), String>>,
    },
}

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

/// zbus interface object -- one handle per bus connection.
#[derive(Clone)]
pub struct Service {
    /// Controller-task request channel.
    tx: mpsc::Sender<DbusRequest>,
}

impl Service {
    /// Build a service handle bound to the given engine-request
    /// channel.
    pub fn new(tx: mpsc::Sender<DbusRequest>) -> Self {
        Self { tx }
    }
}

/// Wait for a controller reply while enforcing the D-Bus request timeout.
async fn await_with_timeout<T>(rx: oneshot::Receiver<T>) -> Result<T, zbus::fdo::Error> {
    match tokio::time::timeout(DBUS_REQUEST_TIMEOUT, rx).await {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(_canceled)) => Err(zbus::fdo::Error::Failed(
            "controller reply channel closed".into(),
        )),
        Err(_) => Err(zbus::fdo::Error::Failed(format!(
            "request timed out after {DBUS_REQUEST_TIMEOUT:?}"
        ))),
    }
}

#[zbus::interface(name = "com.nutanix.io_thread_controller1")]
impl Service {
    /// Set one VM's worker count through the debug-only control surface.
    #[cfg(debug_assertions)]
    async fn set_thread_count(
        &self,
        vm: String,
        threads: u32,
        sticky: bool,
    ) -> zbus::fdo::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(DbusRequest::SetThreadCount {
                vm,
                threads,
                sticky,
                reply: tx,
            })
            .await
            .map_err(|_| zbus::fdo::Error::Failed("controller channel closed".into()))?;
        match await_with_timeout(rx).await? {
            Ok(()) => Ok(()),
            Err(e) => Err(zbus::fdo::Error::Failed(e)),
        }
    }

    /// Return the controller's machine-readable fleet snapshot.
    async fn get_stats(&self) -> zbus::fdo::Result<String> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(DbusRequest::GetStats { reply: tx })
            .await
            .map_err(|_| zbus::fdo::Error::Failed("controller channel closed".into()))?;
        await_with_timeout(rx).await
    }

    /// D-Bus wire signature: `GetSnapshot() -> s`.
    ///
    /// The `s` return is a JSON payload matching
    /// `SnapshotPayload` on the controller side.  Kept as a
    /// bare string on the wire so we can evolve the payload
    /// shape (add a new metric, reorder fields, extend a
    /// nested block) without a fresh D-Bus signature and
    /// forced re-flash of every consumer.  Consumers parse it
    /// with `serde_json` -- unknown fields are ignored, missing
    /// fields fall back to the type's default -- so slightly
    /// older consumers keep running against a newer daemon and
    /// vice versa.
    async fn get_snapshot(&self) -> zbus::fdo::Result<String> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(DbusRequest::GetSnapshot { reply: tx })
            .await
            .map_err(|_| zbus::fdo::Error::Failed("controller channel closed".into()))?;
        await_with_timeout(rx).await
    }

    /// D-Bus wire signature: `GetVersion() -> s`.
    ///
    /// Returns the controller binary version string (same source as startup
    /// logs), exposed separately so high-frequency snapshot payloads stay
    /// focused on dynamic metrics.
    async fn get_version(&self) -> zbus::fdo::Result<String> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(DbusRequest::GetVersion { reply: tx })
            .await
            .map_err(|_| zbus::fdo::Error::Failed("controller channel closed".into()))?;
        await_with_timeout(rx).await
    }

    async fn get_io_thread_vq_mapping(
        &self,
        vm: String,
        device: String,
    ) -> zbus::fdo::Result<String> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(DbusRequest::GetIoThreadVqMapping {
                vm,
                device,
                reply: tx,
            })
            .await
            .map_err(|_| zbus::fdo::Error::Failed("controller channel closed".into()))?;
        match await_with_timeout(rx).await? {
            Ok(v) => Ok(v),
            Err(e) => Err(zbus::fdo::Error::Failed(e)),
        }
    }

    async fn add_io_thread(
        &self,
        vm: String,
        id: String,
        poll_max_ns: i64,
    ) -> zbus::fdo::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(DbusRequest::AddIoThread {
                vm,
                id,
                poll_max_ns,
                reply: tx,
            })
            .await
            .map_err(|_| zbus::fdo::Error::Failed("controller channel closed".into()))?;
        match await_with_timeout(rx).await? {
            Ok(()) => Ok(()),
            Err(e) => Err(zbus::fdo::Error::Failed(e)),
        }
    }

    async fn del_io_thread(&self, vm: String, id: String) -> zbus::fdo::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(DbusRequest::DelIoThread { vm, id, reply: tx })
            .await
            .map_err(|_| zbus::fdo::Error::Failed("controller channel closed".into()))?;
        match await_with_timeout(rx).await? {
            Ok(()) => Ok(()),
            Err(e) => Err(zbus::fdo::Error::Failed(e)),
        }
    }

    async fn set_io_thread_vq_mapping(
        &self,
        vm: String,
        device: String,
        mapping_json: String,
    ) -> zbus::fdo::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(DbusRequest::SetIoThreadVqMapping {
                vm,
                device,
                mapping_json,
                reply: tx,
            })
            .await
            .map_err(|_| zbus::fdo::Error::Failed("controller channel closed".into()))?;
        match await_with_timeout(rx).await? {
            Ok(()) => Ok(()),
            Err(e) => Err(zbus::fdo::Error::Failed(e)),
        }
    }
}

/// Spin the D-Bus service on the well-known name / object path
/// backed by `service` and drive replies from the controller mpsc.
pub async fn serve(service: Service) -> Result<zbus::Connection, zbus::Error> {
    let conn = zbus::connection::Builder::system()?
        .name(DBUS_BUS_NAME)?
        .serve_at(DBUS_OBJECT_PATH, service)?
        .build()
        .await?;
    Ok(conn)
}
