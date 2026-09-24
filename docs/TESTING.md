# Testing: coverage evaluation

## Scope
This document evaluates the test suite as of commit d737fd1: which tests add little, which ones guard real behaviour, and which behaviours would be better tested by Python component tests. There are 59 Rust unit tests across 10 files and 1 Python component test. `make test` only runs `cargo test`; CI (`.github/workflows/pull_request.yaml`) runs `make pre-push`. No coverage tooling is installed (no llvm-cov or tarpaulin), so this is a review of the test code, not a coverage report.

## Headline findings

1. **The one Python component test cannot run.** `tests/component/test_runaway_scale_regression.py` imports `conftest.wait_for` and uses the `controller` and `fake_backend` fixtures, but there is no conftest.py and no fake backend anywhere in the repo. The only backend is qemu (`src/backends/mod.rs` linkme registry). CI never runs pytest. It has been dead since the first commit (81aebfb).

2. **Likely production bug that the unit tests hide: IOPS validation compares cumulative counters, not rates.**
   - `ThresholdEngine::evaluate` (`src/engines/threshold.rs:342-352`) reads `status.perf.total_io_count()`. The baseline comes from `prev_io_count_total` (`src/controller.rs:499`). Both are raw QMP `query-blockstats` lifetime counters (`libvirt.rs:400`, stored as-is at `instance.rs:248`). The real rates live separately in `status.rates` and `status.rolling`.
   - Result 1: scale-down validation can never fail, because a monotonic counter never drops by 5%.
   - Result 2: scale-up validation needs the lifetime counter to grow 5% within about 2 polls. A long-running VM will almost always revert, which gives up→revert→up oscillation. A fresh VM will almost always pass.
   - The engine tests (`scale_up_revert_fires_on_iops_rate_drop`, `scale_up_flat_rate_reverts`, `pending_validation_reverts_regressive_scale`) write rate-like numbers into `read_io_count` and even make the counter go *down* (155 000 → 122 000). Real data never does that, so the tests pass against a model of the data that doesn't match production.
   - This is exactly what a component test with a fake backend that emits monotonic counters would catch.

3. **One VM can crash the whole daemon.** `persist_new_classifications` returns `Err` when a persisted ownership differs (`controller.rs:473`), or when the state file can't be saved. `daemon.rs:85` does `controller.tick().await?`, so the daemon exits. This path has no test.

4. **Known guard gap, untested:** the FIXME at `controller.rs:608` (a `Down` with a target above the current count bypasses the host-CPU ceiling). `blocked_reason_guards` doesn't include that case.

## Tests that add ~nothing (delete or fold in)
| Test | Why |
|---|---|
| `util.rs` — all 3 | They build `Path` and check the string comes back unchanged. None sets `IO_THREAD_CONTROLLER_ROOT_PATH`, so the only real logic (root prefixing) is never exercised. The `LazyLock`/`lazy_static` also makes it untestable in-process. Either replace with one test that runs a subprocess with the env var set, or cover it from Python (see below). |
| `instance.rs::display_trims_instance_id` | Tests `trim()`. |
| `instance.rs::boxed_client_forwards_io_thread_operations` | All four calls return `Ok(())` / an empty vec, so a broken forwarding impl that returned `Ok` would still pass. It only asserts that the code compiles. To be useful, record the calls and assert on them. |
| `engines/mod.rs::scale_action_target_and_display`, `instance_decision_new_preserves_fields` | Test the enum/struct boilerplate. |
| `config.rs::dump_default_config_contains_engine`, `default_config_is_valid`, `controller_policy_defaults_round_trip` | Mostly overlap `default_matches_serde_defaults`, which is the one worth keeping. |
| `threshold.rs::missing_config_file_uses_defaults` | `let _ = engine.config();` asserts nothing beyond "no error", and `load_config_or_default_only_defaults_missing_files` already covers it. |
| `threshold.rs::performance_revert_log_contains_decision_inputs` | Checks log formatting substrings. It breaks on any wording change and catches no logic bugs. Keep it only if operators grep these exact keys. |
| `threshold.rs::evaluate_holds_when_util_below_thresholds` | Util 0 with 1 thread and min 2: it holds for a trivial reason. |
| `rolling.rs::first_sample_only_establishes_baseline` | Trivial, though cheap. |

## Tests that genuinely guard behaviour (keep)
- `controller.rs::blocked_reason_guards`: a table test of the guard priority order. It is the best test in the repo; add the FIXME case and a `Down` target at or above `thread_count`.
- `controller.rs::actuation_enforces_vcpu_cap`, `dry_run_skips_actuation_and_starts_cooldown`, `sync_instances_adds_retains_and_removes` (checks that a duplicate client is closed), `tick_refreshes_evaluates_and_drops_failed_instances`.
- `threshold.rs::no_scale_up_during_pending_validation`: the actual runaway-scale regression, as a unit test. It is deterministic and already covers what the Python test tries to do. Caveat: `engine()` sets `scale_validation_sample_polls: 0`, so the loop runs **zero** times and the test only checks the final `Up(6)`. **It does not test the hold at all.** Set polls to ≥2.
- `threshold.rs::percent_wire_format_round_trips` (it also checks the typo'd key is rejected).
- `state.rs`, all 3: persistence and overlap rejection.
- `rolling.rs::rates_use_real_elapsed_time`, `backend_util_derives_cpu_time...`, `counter_reset_replaces_baseline`.
- `instance.rs::rates_since_scales_deltas_per_second`, `duplicate_worker_names_keep_independent_deltas`, `new_worker_starts_without_a_lifetime_spike`: real edge cases in the counter math.
- `qemu/helpers.rs`, all 4 (topology parsing, round-robin, gap-fill ids, util averaging). `qemu/libvirt.rs`: the envelope and blockstats parsers. Also check `xml_has_virtio_scsi` with `model='virtio-scsi-pci'`-style variants if they occur in practice.

## Gaps worth unit tests (pure logic, no I/O)
- `ThresholdEngine::scale_down_target`: `max_scale_down_step`, the min clamp and the `target >= thread_count` early return. Nothing covers it today.
- Scale-down sustain: `low_util_polls` accumulates, resets on a successful apply, and is **kept** when an action is Blocked (the comment at `threshold.rs:416` states this intent and no test enforces it).
- Scale-down validation, once finding #2 is fixed.
- `on_applied` for Revert/Failed/Blocked does not start validation.
- `ThresholdConfig::validate` bounds.

## Better as Python component tests (real binary, fake backend, real D-Bus/fs)
These cross process, filesystem, time or D-Bus boundaries, where Rust mocks either don't exist or would re-implement the thing under test:
1. **Closed-loop scaling against a fake QEMU**: a fake QMP/virsh endpoint serving monotonic blockstats and IOThread CPU. It would catch finding #2, oscillation, cooldown timing and runaway ramps end to end. The existing Python test belongs here, but it needs the harness built.
2. **Discovery lifecycle**: `discover_via_control_socket` + inotify in `daemon.rs`. Create and delete `socket_dir/<id>/sock` and assert the VM appears and disappears. Also cover the peer-PID lookup and the unreadable-dir warning. There are zero tests today.
3. **Ownership persistence across restarts**: start, classify, stop, edit or keep the state file, restart. Include the mismatch case (finding #3, the daemon exits) and a corrupt state file.
4. **D-Bus API** (`dbus.rs`, 8 methods, 0 tests): SetThreadCount sticky/manual override, error strings, GetSnapshot/GetStats JSON shape (the TUI and `tools/rolling_status_metrics.py` depend on it), and timeouts when the controller is busy.
5. **Config loading end to end**: `IO_THREAD_CONTROLLER_ROOT_PATH` prefixing, conf.d engine/backend dirs, unknown-field rejection at startup, `--dump-config`.
6. **Dry-run mode through the real binary**: no QMP writes, but cooldown and status lines still emitted.
7. **Status line / log output contract** (`status.rs`): assert on parsed output instead of the Rust substring log test.

Keep in Rust: guard tables, engine state machine, counter/rate math and parsers. They are fast and deterministic, and the Python layer would only make them slower and flakier. In particular the runaway-scale invariant is better as a deterministic Rust test (with polls ≥2) than as a 3 s wall-clock Python test with +2 slack.

## proptest: worth adding, for a few specific properties
Add it as a dev-dependency pinned like the others (`proptest = "=1.x"`). Commit the `proptest-regressions/` files. Candidates, in order of value:
1. **Engine state machine** (a stateful property; `proptest-state-machine` or a hand-rolled op sequence). Drive `ThresholdEngine` + `apply_engine_decision` with random sequences of (util, IO-counter increment, apply success/blocked/failed). Invariants:
   - never produces Up while a validation is pending;
   - never exceeds min/max/vCPU;
   - never makes two ordinary actions closer together than `scale_validation_sample_polls`;
   - Revert always targets the baseline.
   The generator must model counters as **monotonic** (increments ≥ 0). That is the model the current hand-written tests get wrong, and it would expose finding #2. It deterministically replaces the Python runaway test.
2. `scale_down_target`: the result stays within [max(min, thread-step), thread]; it never scales up; after scaling, load per thread ≤ threshold unless a clamp applies.
3. `round_robin_vq_mapping`: each vq in 0..n appears exactly once, bucket sizes differ by ≤1, and the output length equals the number of ids.
4. `next_qemu_iothread_id`: the result is not in the input, and **it matches `MANAGED_IOT_ID_RE`**. That second part fails today: with ≥1024 ids it returns `iot-extra-N`, which `^iot[0-9]+$` rejects, so `QemuTopology` would never see a thread we created. You need a generator biased toward dense `iot0..iotN` sets to hit it.
5. Counter math (`rates_since`, `RollingMetrics`, `compute_per_worker_util`, `CpuSample::utilisation_since`): no panics or overflow on arbitrary u64s or resets, and results are finite and ≥ 0.
6. Parsers (`QemuTopology::new`, `parse_qmp_envelope`, `parse_blockstats`): never panic on arbitrary strings. Blockstats sums saturate.
7. Percent serde: `serialize → deserialize` round-trips within ε. Values outside [0,100] are rejected by `validate`.

Skip proptest for `blocked_reason` (the table test is clearer), config defaults, D-Bus and discovery. Those are covered by the unit and component tests above.

## Suggested follow-up work
Keep these as separate small commits, with bug fixes separate from test changes.
1. Fix `no_scale_up_during_pending_validation` so validation polls are > 0 (test-only).
2. Add a unit test showing validation with monotonic counters, then fix the engine to use `status.rates`/`rolling` IOPS (bug-fix commit).
3. Decide what a classification mismatch should do (skip the VM vs exit), then test it.
4. Delete or merge the trivial tests listed above.
5. Build the pytest harness: conftest with `controller` (spawns the binary under a temp `IO_THREAD_CONTROLLER_ROOT_PATH`) and `fake_backend` (fake QMP socket or a test-only backend behind a cargo feature). Add `make component-test` and a CI step.

## Measuring progress
- `make test` (cargo test --all-features) stays green after the test edits.
- Run `cargo llvm-cov --all-features` before and after to confirm coverage moves in `threshold.rs`, `controller.rs`, `daemon.rs` and `dbus.rs`.
- Component tests: `pytest tests/component` locally, then wire them into `pull_request.yaml`.
