//! In-process RLM child sessions: the standalone [`RlmSubagentHost`]
//! implementation a resident embedding (the cloud guest) installs into a
//! session engine. The daemon's supervisor-backed host gives every child
//! its own supervised worker process through the supervisor link; this
//! host runs each child as a full pa-core [`SessionEngine`] inside the
//! parent's own process — no supervisor, no worker processes, no nested
//! daemons — which is the TS guest daemon's hosting model
//! (`createRlmSubagentRuntime` + the session-host core, ported to the
//! Rust engine).
//!
//! Kernel-visible parity with the daemon host: the same
//! [`RlmSpawnHandle`] shape, the same roster/collect/delete envelopes and
//! selector errors, spawn-name reservation across admission, deleted-child
//! tombstones, terminal notices, child usage attribution, and the
//! in-process family surface (`agent_message`/`agent_observe`) the worker
//! gets over the wire. Divergences are documented on each seam.
//!
//! Ownership: one host instance per parent session. The host is created
//! before the parent engine (it rides `SessionEngineConfig`'s
//! `rlm_subagent_host`), then [`InProcessRlmHost::bind_parent`] binds it
//! to the assembled engine. Every strong edge points down the tree
//! (parent engine → host → child records → child engines → child hosts);
//! every edge back up is weak, so dropping the parent engine tears the
//! whole subtree down with it, kernels included.

mod family;
mod model;
mod notices;
mod registry;
mod run;
mod spawn;

#[cfg(test)]
mod tests;

pub use family::{family_host_handlers, FamilyHostHandlers, FamilySelf, InProcessFamilyController};
pub use model::{assert_thinking_supported, resolve_child_model, ResolvedChildModel};
pub use registry::{ChildIdentity, InProcessChildRecord};

use std::path::PathBuf;
use std::sync::{Arc, Weak};
use std::time::Duration;

use pa_agent::stream::StreamFn;
use pa_agent::types::Model as AgentModel;
use tokio::sync::{watch, Mutex};

use super::engine::SessionEngine;
use super::rlm_host::RlmSubagentHost;
use crate::models::registry::ModelRegistry;

/// How long a detached child prompt waits for its spawning parent turn to
/// end before prompting anyway (the daemon host's `TURN_DONE_WAIT_SECS`;
/// a stuck turn must not orphan the child's task).
const TURN_DONE_WAIT_SECS: u64 = 60;

/// The default recursion bound (TS `resolveRlmMaxDepth`).
pub const DEFAULT_RLM_MAX_DEPTH: u32 = 2;

/// The stream seam every child session runs on: given the resolved child
/// model, produce the session's `stream_fn`. The resident guest closes
/// over its live provider-target slot (one seam reading per call, like
/// the headless runtime's switchable stream); hermetic embeddings supply
/// a scripted stream.
pub type StreamFnFactory = Arc<dyn Fn(&AgentModel) -> StreamFn + Send + Sync>;

/// Everything the host needs to admit children for one parent session.
pub struct InProcessRlmHostConfig {
    /// The agent dir children resolve settings, skills, and the kernel
    /// Python-skill inventory from (the parent's own agent dir).
    pub agent_dir: PathBuf,
    /// The model catalog child references resolve against.
    pub registry: Arc<ModelRegistry>,
    /// The per-child stream seam (see [`StreamFnFactory`]).
    pub stream_fn_factory: StreamFnFactory,
    /// The parent session's RLM depth (0 for a resident root).
    pub rlm_depth: u32,
    /// The recursion bound; `0` adopts [`DEFAULT_RLM_MAX_DEPTH`].
    pub rlm_max_depth: u32,
    /// The thinking level children inherit when neither the spawn request
    /// nor the parent's live level supplies one.
    pub default_thinking: Option<String>,
}

/// The parent engine a host is bound to (all weak: the parent engine owns
/// the host through its kernel handlers, and a strong edge back would
/// keep a released session's kernel alive forever).
struct ParentBinding {
    engine: Weak<SessionEngine>,
    /// The parent session's durable id (the artifacts tree children live
    /// under).
    session_id: String,
    /// The parent session's name (the family roster's parent row).
    session_name: Option<String>,
    /// The parent session file (the child headers' parent edge).
    session_file: Option<String>,
    /// The working directory children inherit.
    cwd: PathBuf,
}

/// Identity facts children derive from the bound parent.
pub(crate) struct ParentFacts {
    pub(crate) session_id: String,
    pub(crate) session_file: Option<String>,
    pub(crate) cwd: PathBuf,
}

struct HostInner {
    config: InProcessRlmHostConfig,
    /// This parent session's children, admission order.
    children: Mutex<Vec<Arc<InProcessChildRecord>>>,
    /// Requested-name reservations held until admission is durable (TS
    /// #2396): two parallel same-name spawns cannot both admit.
    pending_spawn_names: std::sync::Mutex<std::collections::HashSet<String>>,
    /// Delete-receipt tombstones (TS `_deletedRlmChildRuns`): a deleted
    /// child's identity stays behind the registry so `rlm.collect` can
    /// answer a just-deleted selector with its settled cancelled envelope.
    deleted_children: std::sync::Mutex<std::collections::HashMap<String, registry::DeletedChild>>,
    parent: std::sync::RwLock<Option<ParentBinding>>,
    /// Bumped once per completed parent run (the parent agent subscription
    /// installed at bind time). Detached child prompts wait for the next
    /// bump so the parent's continuation request is always in flight
    /// before the child's first model turn — the same ordering the daemon
    /// host gets from the worker turn loop.
    turn_done: watch::Sender<u64>,
    /// The parent agent subscription keeping the turn boundary alive.
    turn_subscription: Mutex<Option<pa_agent::agent::Subscription>>,
    /// The host of the parent this one spawns under (`None` for the
    /// resident root's host): the sibling roster resolves through it.
    parent_host: std::sync::Mutex<Option<Weak<HostInner>>>,
}

/// The in-process children host. Cheap to clone (one shared handle); pass
/// the same clone into `SessionEngineConfig::rlm_subagent_host` and
/// [`InProcessRlmHost::bind_parent`].
#[derive(Clone)]
pub struct InProcessRlmHost {
    inner: Arc<HostInner>,
}

impl InProcessRlmHost {
    /// Build the host for one parent session. Call
    /// [`InProcessRlmHost::bind_parent`] once the parent engine exists.
    #[must_use]
    pub fn new(config: InProcessRlmHostConfig) -> Self {
        let config = if config.rlm_max_depth == 0 {
            InProcessRlmHostConfig {
                rlm_max_depth: DEFAULT_RLM_MAX_DEPTH,
                ..config
            }
        } else {
            config
        };
        Self {
            inner: Arc::new(HostInner {
                config,
                children: Mutex::new(Vec::new()),
                pending_spawn_names: std::sync::Mutex::new(std::collections::HashSet::new()),
                deleted_children: std::sync::Mutex::new(std::collections::HashMap::new()),
                parent: std::sync::RwLock::new(None),
                turn_done: watch::Sender::new(0),
                turn_subscription: Mutex::new(None),
                parent_host: std::sync::Mutex::new(None),
            }),
        }
    }

    /// The parent session's depth (children sit one level below it).
    pub(crate) fn rlm_depth(&self) -> u32 {
        self.inner.config.rlm_depth
    }

    /// The recursion bound.
    pub(crate) fn max_depth(&self) -> u32 {
        self.inner.config.rlm_max_depth
    }

    /// The host config (registry, agent dir, stream seam, defaults).
    pub(crate) fn config(&self) -> &InProcessRlmHostConfig {
        &self.inner.config
    }

    /// Bind the host to its parent engine. Subscribes the parent agent's
    /// turn boundary (the host owns the bump, no embedding wiring), and
    /// captures the identity children derive their session dir and family
    /// membership from. Binding is idempotent: a second call replaces the
    /// previous binding (a rebuilt parent session re-binds like the
    /// daemon's `set_identity`).
    ///
    /// # Panics
    ///
    /// Panics when a host lock is poisoned.
    pub async fn bind_parent(&self, engine: Arc<SessionEngine>) {
        let persistence = engine.session.shared_persistence();
        let (session_id, session_name, session_file, cwd) = {
            let session = persistence.lock().await;
            (
                session.get_session_id().to_string(),
                session.get_session_name(),
                session
                    .get_session_file()
                    .map(|path| path.display().to_string()),
                session.get_cwd().to_path_buf(),
            )
        };
        *self.inner.parent.write().expect("parent binding lock") = Some(ParentBinding {
            engine: Arc::downgrade(&engine),
            session_id,
            session_name,
            session_file,
            cwd,
        });
        // The turn boundary: the parent agent's run end bumps the watch a
        // detached child prompt waits on. Listener futures run inline with
        // the run's settlement, so the bump itself stays trivial.
        let agent = engine.session.agent().clone();
        let turn_done = self.inner.turn_done.clone();
        let subscription = agent
            .subscribe(move |event, _signal| {
                let turn_done = turn_done.clone();
                Box::pin(async move {
                    if matches!(event, pa_agent::types::AgentEvent::AgentEnd { .. }) {
                        turn_done.send_modify(|value| *value += 1);
                    }
                    Ok(())
                })
            })
            .await;
        *self.inner.turn_subscription.lock().await = Some(subscription);
    }

    /// The parent binding, split into its weak engine and the identity
    /// facts (an unbound host fails the spawn admission with a precise
    /// error).
    pub(crate) fn parent(&self) -> anyhow::Result<(Weak<SessionEngine>, ParentFacts)> {
        let guard = self.inner.parent.read().expect("parent binding lock");
        let binding = guard.as_ref().ok_or_else(|| {
            anyhow::anyhow!("the in-process RLM host has no parent session bound yet")
        })?;
        Ok((
            binding.engine.clone(),
            ParentFacts {
                session_id: binding.session_id.clone(),
                session_file: binding.session_file.clone(),
                cwd: binding.cwd.clone(),
            },
        ))
    }

    /// The parent engine, when bound and still alive.
    pub(crate) fn parent_engine(&self) -> Option<Arc<SessionEngine>> {
        self.inner
            .parent
            .read()
            .expect("parent binding lock")
            .as_ref()
            .and_then(|binding| binding.engine.upgrade())
    }

    /// Record the host of the parent this session spawns under (the
    /// spawned child's host gets the parent host's handle for sibling
    /// enumeration).
    pub(crate) fn set_parent_host(&self, parent_host: &InProcessRlmHost) {
        *self.inner.parent_host.lock().expect("parent host lock") =
            Some(Arc::downgrade(&parent_host.inner));
    }

    /// The host of the parent this session spawns under, when the parent
    /// is itself an in-process child (`None` for a resident root).
    pub(crate) fn parent_host(&self) -> Option<InProcessRlmHost> {
        self.inner
            .parent_host
            .lock()
            .expect("parent host lock")
            .as_ref()
            .and_then(Weak::upgrade)
            .map(|inner| InProcessRlmHost { inner })
    }

    /// The parent binding's identity (the family roster's parent row):
    /// its session id and name.
    pub(crate) fn parent_identity(&self) -> Option<(String, Option<String>)> {
        let guard = self.inner.parent.read().expect("parent binding lock");
        guard
            .as_ref()
            .map(|binding| (binding.session_id.clone(), binding.session_name.clone()))
    }

    /// Mark that `child_id` replied to its parent since its task was
    /// admitted (TS `_parentReplyCount`): the parent's no-reply terminal
    /// notice is withheld.
    pub(crate) async fn mark_replied(&self, child_id: &str) {
        let children = self.children().await;
        for record in children {
            if record.rlm_child_id == child_id {
                record.state().await.replied_since_task = true;
                return;
            }
        }
    }

    /// Whether any tracked child run is still unsettled (TS
    /// `_hasUnsettledRlmQuiescenceWork`'s child-run arm): the guest's
    /// status record and a child's own task-settle both read it.
    pub async fn any_running(&self) -> bool {
        let children = self.children().await;
        for record in &children {
            if record.is_running().await {
                return true;
            }
        }
        false
    }

    /// The live child records (admission order).
    pub(crate) async fn children(&self) -> Vec<Arc<InProcessChildRecord>> {
        self.inner.children.lock().await.clone()
    }

    /// Register an admitted child.
    pub(crate) async fn push_child(&self, record: Arc<InProcessChildRecord>) {
        self.inner.children.lock().await.push(record);
    }

    /// Remove one child from the registry (delete or close).
    pub(crate) async fn remove_child(&self, record: &Arc<InProcessChildRecord>) {
        self.inner
            .children
            .lock()
            .await
            .retain(|candidate| !Arc::ptr_eq(candidate, record));
    }

    /// The resident child identities (the family roster's child join): id,
    /// session id, and name per child, without status refreshes.
    pub async fn child_identities(&self) -> Vec<registry::ChildIdentity> {
        let children = self.children().await;
        let mut identities = Vec::with_capacity(children.len());
        for record in children {
            identities.push(registry::ChildIdentity {
                rlm_child_id: record.rlm_child_id.clone(),
                session_id: record.session_id.clone(),
                session_name: record.session_name.clone(),
            });
        }
        identities
    }

    /// The in-process family handlers for THIS session (the parent's own
    /// `agent_message`/`agent_observe` surface): merge the result into
    /// the parent engine's `SessionEngineConfig::extra_host_handlers`.
    /// The host registers the children's own handlers at spawn.
    #[must_use]
    pub fn family_host_handlers(self: &Arc<Self>) -> FamilyHostHandlers {
        family_host_handlers(self, FamilySelf::Root)
    }

    /// Close every tracked child with the parent session (TS
    /// `closeChildSessions`): abort the runs, owe no terminal notices (the
    /// parent is going away), and drop the child engines — each child's
    /// kernel tears down with its engine, and each child's own children
    /// close through the same walk. Not a delete: no tombstones, no
    /// ledger markers.
    pub async fn close_children(&self) {
        let children = self.children().await;
        for record in &children {
            {
                let mut state = record.state().await;
                state.closed_by_parent = true;
                state.notice_delivered = true;
            }
            let () = record
                .settle_as("cancelled", Some("Closed with parent session".to_string()))
                .await;
            record.engine.session.agent().abort();
        }
        self.inner.children.lock().await.clear();
    }

    /// Wait for the parent turn after `generation` to end (bounded: a
    /// stuck turn releases the child anyway).
    pub(crate) async fn wait_turn_done(&self, generation: u64) {
        let mut receiver = self.inner.turn_done.subscribe();
        let _ = tokio::time::timeout(
            Duration::from_secs(TURN_DONE_WAIT_SECS),
            receiver.wait_for(|value| *value > generation),
        )
        .await;
    }

    /// The current turn-boundary generation (captured at spawn admission).
    pub(crate) fn turn_generation(&self) -> u64 {
        *self.inner.turn_done.subscribe().borrow()
    }
}

/// Wall-clock ms since the epoch (the roster's clock).
pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}

impl RlmSubagentHost for InProcessRlmHost {
    fn spawn(
        &self,
        request: super::rlm_host::RlmSpawnRequest,
    ) -> super::rlm_host::RlmHostFuture<super::rlm_host::RlmSpawnHandle> {
        spawn::spawn(self.clone(), request)
    }

    fn create_session(
        &self,
        _request: super::rlm_host::RlmCreateSessionRequest,
    ) -> super::rlm_host::RlmHostFuture<super::rlm_host::RlmCreateSessionHandle> {
        // A resident depth-0 session is the guest's own composition (the
        // guest daemon owns the one resident root), not something a
        // session's kernel may mint; the TS-parity refusal stands.
        Box::pin(async {
            anyhow::bail!("rlm.create_session requires a daemon-backed depth-0 session");
        })
    }

    fn list_subagents(
        &self,
    ) -> super::rlm_host::RlmHostFuture<Vec<super::rlm_host::RlmSubagentEntry>> {
        run::list_subagents(self.clone())
    }

    fn delete_subagent(
        &self,
        target: String,
    ) -> super::rlm_host::RlmHostFuture<super::rlm_host::RlmDeleteSubagentResult> {
        run::delete_subagent(self.clone(), target)
    }

    fn collect(
        &self,
        targets: Vec<String>,
        timeout_ms: u64,
    ) -> super::rlm_host::RlmHostFuture<Vec<super::rlm_host::RlmChildResult>> {
        run::collect(self.clone(), targets, timeout_ms)
    }
}
