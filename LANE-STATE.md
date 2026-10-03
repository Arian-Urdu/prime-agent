# D2 holder-liveness lifetime lease — recovery state

2026-10-03: worktree exists at e38702160795b172b1f9f66a4a97a5debd060f57, matching current remote main tip; clean branch. Banked callers: worker/connection.rs:252 bind, worker.rs:871 cleanup (#3291); supervisor.rs:411 prepare and supervisor.rs:520 cleanup are D2 surfaces. TS citations pending. Oracle RED/GREEN pending. Fix pending. Gate pending.

2026-10-03 parity citations initial: TS packages/coding-agent/src/modes/daemon/daemon-socket.ts:13-18 stale=5000ms, update=1000ms; acquireDaemonSocketPathLease uses proper-lockfile with 600x25ms retries, onCompromised; daemon-supervisor.ts:852-865 lease acquired BEFORE socket prep, :877-884 bind+identity capture, :7391-7415 compromise fences socket/client without unlink, cleanup with lease identity, :7495-7500 releases lock on cleanup. Rust socket.rs has only ephemeral cleanup lock, LockDir has no refresher and Drop unconditionally rmdir: stale lease takeover could delete successor lock if former holder drops. Oracle pending (VM-only build).

2026-10-03 independent bench recon: RECORD-SCHEMA.md record required fields; same-VM ABBA and separate bind vs stale grace cohorts recommended; bench worker initially examined stale primary checkout (50e011) but actual lane worktree is e387021 and includes D1 socket_identity_guard_e2e.rs. Socket lease implementation delegated socket.rs only; supervisor integration reserved parent. Oracle still pending VM.

2026-10-03 ORACLE PREMISE CONFLICT: TS daemon-socket.ts:135-137 rejects live socket connect even if stale lease reclaimed. SIGSTOP bound listener may accept kernel connect while userland wedged => TS likely rejects; SIGKILL between bind/use leaves stale socket but current Rust socket.rs already self-heals (1s grace). Do not green-fish or bypass TS live-listener probe. Need distinguish real lifetime lease gap via stale lock takeover + fence-without-unlink, and record honest negative if impossible.

2026-10-03 failing-first oracle added in socket_identity_guard_e2e.rs: supervisor_renews_lifetime_socket_lease_and_reclaims_a_dead_holder checks live lock dir + mtime refresh, refusal of rival, kill holder, successor takeover. RED structurally (no lock dir in current tree); VM execution pending.

2026-10-03 audit reconciliation: read record 20261001-174500 (bench commit 4e0f00d). E4c asserts TS stalled-event-loop -> stale lease -> successor bind, but TS daemon-socket.ts:135-137 live-listener connect rejects even after lease takeover. Permanently SIGSTOP-bound predecessor remains kernel-connectable, so successor cannot bind; TS old holder fences only when resumed. Audit E2/E3 itself calls dead-holder stale-socket cleanup equivalent. D2 “forever self-heals” claim unsupported; narrowed gap lifetime exclusion/fence under external socket unlink or stale ownership race. Parent notified.

2026-10-03 new genuinely failing-first oracle added: live_supervisor_lease_prevents_rebind_after_external_socket_rename, contrast E6 audit + D1. Original live daemon socket renamed aside (inode pinned), successor MUST wait at lifetime lease, not bind empty original path (current Rust starts successor immediately). No changed protocol bytes. VM RED pending.

2026-10-03 fix in progress: supervisor.rs now holds socket::SocketLease from before prepare through accept/shutdown (Unix), gates bind with assert_held; select on lease compromise fences new accept and skip unlink through lease cleanup; Windows unchanged. socket.rs implementation underway. Gate pending.

2026-10-03 baseline RED isolation: created separate disposable baseline worktree /home/ubuntu/lane-worktrees/perf-bind-d2-oracle-baseline at e38702160; appended same two failing-first integration tests there, to gate on VM via branch push. Candidate worktree independent. No host builds.

2026-10-03 RED gate ARMED on baseline-only branch lane/perf-bind-d2-oracle-baseline head 25e7921d7b8f09cf3885918ec93a5a98a1dc7ecf; fleet_pipeline gate_1791024748_2635548, mode=crates pa-daemon on Prime VM. Expected first oracle RED no .lock held; second RED socket rebind under external rename. Candidate implementation concurrent.

2026-10-03 baseline gate discovered oracle-format-only FMT_RC=1 at 25e7921d; fixed formatting in both branches, amended baseline-only branch, force-with-lease pushed replacement; redo RED gate at exact new head. This was pre-oracle, not claimed RED.

2026-10-03 first baseline gate also found test compile E0596 (second guard needs mut) after fmt-only blocker; fixed both branches and amended baseline. No RED claim yet.

2026-10-03 baseline RED gate #2 ARMED at 33113a987d50cda745a801f71eff097179bfcc4c, gate gate_1791025001_2635548 mode=crates after fmt + E0596 fixes. First gate was compile RED only, no oracle.

2026-10-03 socket.rs/lock_dir.rs implementation delivered: lifetime 1s refresh on pinned dir file handle, 5s stale reclaim, sticky compromise via watch, inode-safe drop, retained live-connect rejection; supervisor asserts lease before and after bind and prior to serve, listens for compromise. Candidate fmt clean; VM gate after RED baseline.

2026-10-03 11:07 UTC: baseline VM gate gate_1791025001_2635548 at 33113a987 fmt=0 clippy=0 test=101, new behavioral RED oracles `live_supervisor_lease_prevents_rebind_after_external_socket_rename` and `supervisor_renews_lifetime_socket_lease_and_reclaims_a_dead_holder` both fail; 4 existing socket_identity_guard_e2e tests pass. This is real failing-first evidence.

2026-10-03 11:08 UTC: added resume displacement oracle `resumed_displaced_supervisor_fences_before_successor_binds`: SIGSTOP original, rename old lock + create successor lock, SIGCONT, assert old clean exit and preserves socket/successor lock, then release simulated lock and bind successor. Candidate fmt 0, diff --check clean.

2026-10-03 11:09 UTC: candidate c05133a52af04f911312476e808c440dc4492b96 pushed; local fmt check 0 and diff --check 0 (no local builds). Full workspace VM gate ARMED gate_1791028502_2635548; gate fmt/clippy run on VM prior to tests.

2026-10-03 11:12 UTC: first candidate full VM gate at c05133a52 found clippy-only issues (missing # Errors docs on 3 API functions; map_unwrap_or; test-only dead wrapper), not behavioral results. Applied minimal fixes to socket.rs, fmt 0; will supersede and rerun full gate at new head.

2026-10-03 11:57 UTC: candidate amended at 9c22d0f13c3194205c3ed9173047a34ab4b19ab7 (clippy fixes only), pushed force-with-lease after original gate had cloned old SHA. Full VM gate ARMED gate_1791028604_2635548 exact new head.

2026-10-03 11:58 UTC: reviewer shutdown discipline applied: dedicated std::thread refresher (independent of Tokio scheduling), mpsc stop signal then join before lock release, no release if compromised. Previous 9c22 gate may be diagnostic only; final head will be re-gated.

2026-10-03 12:00 UTC: audit premise correction committed and pushed to prime-agent-bench hillclimb as 9be7045dd01f149a9fd47033143beb28121aa40e, sole pathspec original 20261001-174500-other-daemon-bind-choreography-parity.json. Three misleading E4c passages corrected with TS live-connect citations and baseline VM failing-first evidence. Unrelated bench WIP untouched.

2026-10-03 12:02 UTC: resume oracle expectation refined: compromised supervisor intentionally returns an error/fenced exit (nonzero normal process exit); assert exited with code instead of success=0, preserve socket and successor lock before successor bind. fmt 0.

2026-10-03 12:04 UTC: full re-gate gate_1791028604_2635548 CHECKOUT verified exact ef97fab4934ba48679a8a2b3c42c464ea4a61f2b, base main e38702160, clean; VM fmt RC=0 and full-workspace clippy RC=0. Old c051 gate already mid-suite on separate VM; allowed to finish, does not block. Test verdict pending.

2026-10-03 12:08 UTC: same-VM Prime VM x8apbkgp0pkgiwvi7ft8o995 exact ef97 vs e387 ABBA 12 sequential (6+6 valid): observer bind p50 3.4585->3.4495ms (-0.009ms, below calibrated 1.056ms polling sleep, no meaningful speed claim), connect-ready 3.499->3.492ms; candidate p95 4.817 vs base 3.606 due 5.247ms outlier (do not claim p95 win). Candidate 6/6 trials each 4/4 lock mtime changes, 18 interior intervals median 1.000113s, stable inode, clean lock removal. CPU/syscall heartbeat cost separately pending because cadence is not cost. Raw bench-evidence/results.json; VM disposed.

2026-10-03 12:17 UTC: exact ef97 full VM suite revealed two same-shape test harness reds: protocol_breadth_b10_b11::wave_b11_saved_sessions_wire_shapes and protocol_breadth_b6_b9::wave_b7_agent_messages_wire_shapes, connect refused after force-killing an earlier supervisor and immediately reusing the SAME socket path. Baseline fixture assumed no persistent lease; new TS-parity lease leaves abandoned lock for 5s stale reclaim and predecessor socket pathname already exists, so spawn_daemon returned on file exists and Client::connect timed out in 5s. Revised only their Client::connect wait budgets 5s -> 20s (through 600x25ms acquisition retry ceiling); local fmt 0. Full suite still in flight; inspect all failures before candidate supersession.

2026-10-03 12:20 UTC: known-reds live and fleet-tools repo searched: neither wave_b11_saved_sessions_wire_shapes nor wave_b7_agent_messages_wire_shapes registered. Explicit D2-specific suspect: no startup/shutdown fence/admission implemented here, only lifetime lease; both fixture tests share same path after SIGKILL and 5s connect deadline, likely stale-lock reclaim timing. Fresh VM solo x3 candidate original + corrected test helper, baseline x1 delegated; do not classify until evidence.
