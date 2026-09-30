//! Quiet agent watches (swarm PR E, TS `core/agent-watch.ts`): the
//! subscription registry behind `watch.agent` and the range-only notice
//! formatters shared by both watches.
//!
//! `watch.agent` subscribes a session to one direct child's activity.
//! Notices carry message-index ranges and status transitions only — never
//! content — so an orchestrator sees that a child progressed without a
//! second wakeup per message. Quiet by default: nothing emits until a
//! subscription is registered, and even then notices are ranges only. On
//! the digest lane the events land in the inbox and wake the session once
//! per batch (the receiving worker owns that routing; this module owns the
//! registry the poller drives).

/// TS `AGENT_WATCH_MAX_ACTIVE`: active subscriptions per session.
pub const AGENT_WATCH_MAX_ACTIVE: usize = 64;
/// TS `AGENT_WATCH_MAX_TOTAL`: lifetime registrations per session.
pub const AGENT_WATCH_MAX_TOTAL: usize = 1_024;
/// TS `AGENT_WATCH_POLL_INTERVAL_MS`: the one shared poll cadence.
pub const AGENT_WATCH_POLL_INTERVAL_MS: std::time::Duration = std::time::Duration::from_secs(5);

/// TS `AGENT_WATCH_NOTICE_CUSTOM_TYPE`: the custom row of a push-lane
/// watch notice (display: false).
pub const AGENT_WATCH_NOTICE_CUSTOM_TYPE: &str = "agent_watch_notice";

/// One registered watch: a direct child plus its baselines.
#[derive(Debug, Clone)]
pub struct AgentWatchSubscription {
    pub id: String,
    /// The direct child id (RLM child id) this watch resolves to.
    pub child_id: String,
    /// The child's live active session id — the supervisor-routable key the
    /// poller queries (`get_state` routes by `activeSessionId`, never by
    /// the RLM child id).
    pub active_session_id: String,
    pub child_name: String,
    pub last_seen_messages: u64,
    pub last_status: String,
}

impl AgentWatchSubscription {
    /// The `rlm.watch.agent_list` row (TS `listAgentWatches`).
    #[must_use]
    pub fn list_row(&self) -> serde_json::Value {
        serde_json::json!({
            "id": self.id,
            "childName": self.child_name,
            "messages": self.last_seen_messages,
            "status": self.last_status,
        })
    }
}

/// One child's activity snapshot (the poller's input).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentWatchSnapshot {
    pub message_count: u64,
    pub status: String,
}

/// Emitted per change batch: ranges only, no content (TS `AgentWatchEvent`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentWatchEvent {
    pub subscription_id: String,
    pub child_name: String,
    /// Inclusive start index of new messages (0-based transcript index).
    pub from_index: u64,
    /// Exclusive end index of new messages.
    pub to_index: u64,
    /// Status transition, when the child's status changed; else `None`.
    pub status_change: Option<(String, String)>,
}

/// The per-poll inputs (TS `AgentWatchState`): the snapshot provider and
/// the event sink.
pub struct AgentWatchState<'a> {
    pub message_count: &'a dyn Fn(&str) -> Option<AgentWatchSnapshot>,
    pub on_event: &'a mut dyn FnMut(AgentWatchEvent),
}

/// The subscription registry: per-child baselines, the active/total limits,
/// and the poll that emits one event per child with changes (TS
/// `AgentWatchRegistry`).
#[derive(Debug, Default)]
pub struct AgentWatchRegistry {
    subscriptions: std::collections::HashMap<String, AgentWatchSubscription>,
    total_registered: usize,
}

impl AgentWatchRegistry {
    /// Whether one more registration fits the lifetime limit (re-baselining
    /// callers check this BEFORE cancelling the active subscription, so a
    /// rejected replacement cannot silently drop it).
    #[must_use]
    pub fn can_register(&self) -> bool {
        self.total_registered < AGENT_WATCH_MAX_TOTAL
    }

    /// Register a direct child.
    ///
    /// # Errors
    ///
    /// Returns an error when `id` is already registered or the active or
    /// total limit is reached.
    pub fn register(
        &mut self,
        id: &str,
        child_id: &str,
        active_session_id: &str,
        child_name: &str,
        initial: AgentWatchSnapshot,
    ) -> anyhow::Result<AgentWatchSubscription> {
        if self.subscriptions.contains_key(id) {
            anyhow::bail!("Agent watch {id} already exists");
        }
        if self.subscriptions.len() >= AGENT_WATCH_MAX_ACTIVE {
            anyhow::bail!("Agent watch limit reached ({AGENT_WATCH_MAX_ACTIVE} active)");
        }
        if self.total_registered >= AGENT_WATCH_MAX_TOTAL {
            anyhow::bail!("Agent watch total limit reached ({AGENT_WATCH_MAX_TOTAL})");
        }
        let subscription = AgentWatchSubscription {
            id: id.to_string(),
            child_id: child_id.to_string(),
            active_session_id: active_session_id.to_string(),
            child_name: child_name.to_string(),
            last_seen_messages: initial.message_count,
            last_status: initial.status,
        };
        self.subscriptions
            .insert(id.to_string(), subscription.clone());
        self.total_registered += 1;
        Ok(subscription)
    }

    /// Cancel one subscription (re-registration re-baselines by cancelling
    /// first, exactly like TS `registerAgentWatch`).
    pub fn cancel(&mut self, id: &str) -> bool {
        self.subscriptions.remove(id).is_some()
    }

    /// The active subscriptions in registration order.
    #[must_use]
    pub fn list(&self) -> Vec<AgentWatchSubscription> {
        self.subscriptions.values().cloned().collect()
    }

    #[must_use]
    pub fn get(&self, id: &str) -> Option<&AgentWatchSubscription> {
        self.subscriptions.get(id)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.subscriptions.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.subscriptions.is_empty()
    }

    /// Poll every subscription; emits one event per child with changes.
    /// A vanished child (no snapshot) is skipped: the baseline holds.
    pub fn poll(&mut self, state: &mut AgentWatchState<'_>) {
        for subscription in self.subscriptions.values_mut() {
            // The snapshot provider is keyed by the child's ACTIVE SESSION
            // ID — the supervisor-routable form the poller queries
            // (`get_state` routes by `activeSessionId`).
            let Some(snapshot) = (state.message_count)(&subscription.active_session_id) else {
                continue;
            };
            let status_change = (snapshot.status != subscription.last_status).then(|| {
                let change = (
                    std::mem::take(&mut subscription.last_status),
                    snapshot.status.clone(),
                );
                subscription.last_status.clone_from(&snapshot.status);
                change
            });
            let from_index = subscription.last_seen_messages;
            if snapshot.message_count > from_index || status_change.is_some() {
                subscription.last_seen_messages = snapshot.message_count;
                (state.on_event)(AgentWatchEvent {
                    subscription_id: subscription.id.clone(),
                    child_name: subscription.child_name.clone(),
                    from_index,
                    to_index: snapshot.message_count,
                    status_change,
                });
            }
        }
    }
}

/// The quiet notice text for one agent-watch event: ranges only (TS
/// `formatAgentWatchNotice`).
#[must_use]
pub fn format_agent_watch_notice(event: &AgentWatchEvent) -> String {
    let mut parts = vec![format!(
        "[watch-agent child:{}] messages {}..{}",
        event.child_name, event.from_index, event.to_index
    )];
    if event.to_index > event.from_index {
        parts.push(format!("(+{})", event.to_index - event.from_index));
    }
    if let Some((from, to)) = &event.status_change {
        parts.push(format!("status: {from} -> {to}"));
    }
    parts.join(" ")
}

/// The quiet notice text for one job-watch progress event: byte ranges
/// only (TS `formatJobWatchNotice`; the command label caps at 60 chars).
#[must_use]
pub fn format_job_watch_notice(pid: u64, from_bytes: u64, to_bytes: u64, command: &str) -> String {
    let command_label = if command.chars().count() > 60 {
        let clipped: String = command.chars().take(60).collect();
        format!("{clipped}...")
    } else {
        command.to_string()
    };
    format!(
        "[watch-job pid:{pid}] output +{} bytes ({from_bytes}..{to_bytes}) command: {command_label}",
        to_bytes.saturating_sub(from_bytes)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_poll_and_cancel_follow_the_ts_semantics() {
        let mut registry = AgentWatchRegistry::default();
        registry
            .register(
                "w1",
                "child-1",
                "active-1",
                "c1",
                AgentWatchSnapshot {
                    message_count: 3,
                    status: "idle".to_string(),
                },
            )
            .unwrap();

        let mut seen: Vec<String> = Vec::new();
        let messages = |child: &str| {
            if child == "active-1" {
                Some(AgentWatchSnapshot {
                    message_count: 7,
                    status: "running".to_string(),
                })
            } else {
                None
            }
        };
        registry.poll(&mut AgentWatchState {
            message_count: &messages,
            on_event: &mut |event| seen.push(format_agent_watch_notice(&event)),
        });
        assert_eq!(seen.len(), 1);
        assert!(seen[0].contains("messages 3..7 (+4)"));
        assert!(seen[0].contains("status: idle -> running"));

        // No growth and no status change: quiet.
        seen.clear();
        registry.poll(&mut AgentWatchState {
            message_count: &messages,
            on_event: &mut |event| seen.push(format_agent_watch_notice(&event)),
        });
        assert!(seen.is_empty());

        // A vanished child stops emitting; the baseline holds.
        let missing = |_child: &str| -> Option<AgentWatchSnapshot> { None };
        registry.poll(&mut AgentWatchState {
            message_count: &missing,
            on_event: &mut |event| seen.push(format_agent_watch_notice(&event)),
        });
        assert!(seen.is_empty());
        assert_eq!(registry.get("w1").unwrap().last_seen_messages, 7);

        assert!(registry.cancel("w1"));
        assert!(!registry.cancel("w1"));
    }

    #[test]
    fn active_limit_is_enforced() {
        let mut registry = AgentWatchRegistry::default();
        for index in 0..AGENT_WATCH_MAX_ACTIVE {
            registry
                .register(
                    &format!("w-{index}"),
                    &format!("child-{index}"),
                    &format!("active-{index}"),
                    "c",
                    AgentWatchSnapshot {
                        message_count: 0,
                        status: "idle".to_string(),
                    },
                )
                .unwrap();
        }
        let error = registry
            .register(
                "w-over",
                "child-over",
                "active-over",
                "c",
                AgentWatchSnapshot {
                    message_count: 0,
                    status: "idle".to_string(),
                },
            )
            .unwrap_err();
        assert!(error.to_string().contains("active"), "{error}");
    }

    #[test]
    fn job_notices_carry_byte_ranges_and_a_capped_command_label() {
        assert!(format_job_watch_notice(123, 100, 4567, "echo long command")
            .contains("[watch-job pid:123] output +4467 bytes (100..4567)"));
        let long = "c".repeat(80);
        let notice = format_job_watch_notice(9, 0, 10, &long);
        assert!(notice.contains(&"c".repeat(60)));
        assert!(notice.ends_with("..."));
    }

    #[test]
    fn duplicate_registration_is_refused() {
        let mut registry = AgentWatchRegistry::default();
        let initial = AgentWatchSnapshot {
            message_count: 0,
            status: "idle".to_string(),
        };
        registry
            .register("w1", "child-1", "active-1", "c1", initial.clone())
            .unwrap();
        let error = registry
            .register("w1", "child-1", "active-1", "c1", initial)
            .unwrap_err();
        assert!(error.to_string().contains("already exists"), "{error}");
    }
}
