//! The experimental switch that starts Prime Agent without the continual
//! harness (no harness prompt sections, no `[harness-digest]`, no
//! refine/`rlm.harness`, no refinements).
//!
//! A process-wide property: a daemon inherits it from the process that
//! spawns it, workers from the daemon, kernels from the worker. A daemon
//! already running on a socket keeps the mode it started with — use a
//! fresh `--daemon-socket` (or `prime-agent shutdown`).
//!
//! Markdown and Python skills stay available in both modes; only the bundled
//! `refine` skill (the refinement trigger) is dropped.

/// Set to `1` to start without the continual harness (the `--no-harness`
/// CLI flag sets the same variable).
pub const NO_HARNESS_ENV: &str = "PRIME_AGENT_NO_HARNESS";

/// Whether the continual harness runs in this process tree.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum HarnessMode {
    /// The continual harness runs (the default).
    #[default]
    Enabled,
    /// No harness prompt sections, no `[harness-digest]`, no refine
    /// surface, no refinements.
    Disabled,
}

impl HarnessMode {
    /// [`HarnessMode::Disabled`] when [`NO_HARNESS_ENV`] is exactly `1`.
    #[must_use]
    pub fn from_env() -> Self {
        if std::env::var(NO_HARNESS_ENV).as_deref() == Ok("1") {
            Self::Disabled
        } else {
            Self::Enabled
        }
    }
}
