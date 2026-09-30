//! System 1 / System 2 harness router (#2484).
//!
//! The session model (System 2) declares an environment with a finite action
//! space and an action-only System 1 sub-model; the router runs the step loop
//! (observe -> decide -> gate -> execute -> record) and hands back the
//! complete trace for steering. One model call per step, the finite action
//! space compiled into a single typed choice, every decision gated by
//! confidence, no free-form generated actions.
//!
//! Rust port of `packages/coding-agent/src/core/system-router/` (the
//! TypeScript-era PR #2484). The module is pure and host-agnostic: it makes no
//! assumptions about where the model, auth, or environment come from. The
//! kernel host-request bridge lives in
//! [`crate::session_engine::system_router_host`].

mod action_space;
mod decide;
mod r#loop;
mod segment;
mod stdio_environment;
mod types;

// The unit batteries live in per-module `tests.rs` children; the shared test
// doubles live in `test_support`.
#[cfg(test)]
pub(crate) mod test_support;

pub use action_space::{
    compile_action_space, compile_decision_prompt, format_history_entry, gate_threshold,
    observation_digest, truncate_observation, CompiledAction, CompiledActionSpace,
};
pub use decide::{
    create_model_decision_function, parse_decision, router_thinking_level, supports_images,
    RouterDecisionContext, RouterDecisionFn, RouterDecisionOutcome, RouterDecisionRequest,
    ROUTER_DECISION_MAX_TOKENS, ROUTER_DECISION_SYSTEM_PROMPT,
};
pub use r#loop::{
    run_system_router_loop, SystemRouterLoopOptions, ROUTER_CLOSE_GRACE_MS,
    ROUTER_REFUSAL_STREAK_LIMIT, ROUTER_REPETITION_LIMIT,
};
pub use segment::{run_router_segment, RouterSegmentOptions};
pub use stdio_environment::StdioRouterEnvironment;
pub use types::{
    default_router_gate, parse_action_space, parse_environment_actions,
    parse_system_router_run_spec, resolve_gate, RouterActionParamSpec, RouterActionRisk,
    RouterActionSpec, RouterCloseOptions, RouterEnvironment, RouterEnvironmentSpec,
    RouterExecution, RouterGateSpec, RouterGateTrace, RouterGateVerdict, RouterModelInfo,
    RouterObservation, RouterRunStatus, RouterSegmentEnvironment, RouterStdioEnvironmentSpec,
    RouterStepTrace, RouterUsage, SystemRouterRunResult, DEFAULT_ROUTER_ENV_REQUEST_TIMEOUT_MS,
    DEFAULT_ROUTER_HISTORY_STEPS, DEFAULT_ROUTER_MAX_STEPS, DEFAULT_ROUTER_OBSERVATION_CHARS,
    DEFAULT_ROUTER_TIMEOUT_MS, ESCALATE_ACTION, FINISH_ACTION, MAX_ROUTER_ENV_REQUEST_TIMEOUT_MS,
    MAX_ROUTER_HISTORY_STEPS, MAX_ROUTER_OBSERVATION_CHARS, MAX_ROUTER_STEPS,
    MAX_ROUTER_TIMEOUT_MS,
};
