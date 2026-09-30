use serde_json::json;

use super::transcript::snapshot_from_transcript;
use super::*;

fn snapshot() -> MessagingStatsSnapshot {
    MessagingStatsSnapshot {
        arrivals: ArrivalCounts {
            total: 3,
            last5m: 3,
        },
        model_steps: StepCounts {
            total: 6,
            last5m: 6,
            tokens: 6_000,
        },
        ingestion_steps: StepCounts {
            total: 2,
            last5m: 2,
            tokens: 800,
        },
        context: ContextShape {
            estimated_agent_message_tokens: 100,
            context_tokens: Some(1_000),
            share: Some(0.1),
        },
        sends: SendCounts {
            attempts: 3,
            failures: 0,
        },
    }
}

fn args(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

fn eval_config() -> SwarmEvalConfig {
    let mut config = default_eval_config();
    config.model = "internal/glm-5.2-fast".to_string();
    config
}

// ---------------------------------------------------------------------------
// Defense lines
// ---------------------------------------------------------------------------

#[test]
fn passes_when_every_pre_registered_line_holds() {
    let defense =
        evaluate_messaging_defense_lines(&snapshot(), MessagingDefenseLineLimits::default());
    assert_eq!(defense.context_share.value, Some(0.1));
    assert_eq!(defense.context_share.limit, 0.25);
    assert_eq!(defense.context_share.passed, Some(true));
    assert_eq!(defense.turn_share.value, Some(2.0 / 6.0));
    assert_eq!(defense.turn_share.passed, Some(true));
    assert_eq!(defense.cost_share.value, Some(800.0 / 6_000.0));
    assert_eq!(defense.cost_share.passed, Some(true));
    assert_eq!(defense.verdict, DefenseVerdict::Pass);
}

#[test]
fn fails_on_the_first_crossed_line() {
    let crossed = MessagingStatsSnapshot {
        ingestion_steps: StepCounts {
            total: 3,
            last5m: 3,
            tokens: 800,
        },
        ..snapshot()
    };
    let defense = evaluate_messaging_defense_lines(&crossed, MessagingDefenseLineLimits::default());
    assert_eq!(defense.turn_share.value, Some(0.5));
    assert_eq!(defense.turn_share.limit, 1.0 / 3.0);
    assert_eq!(defense.turn_share.passed, Some(false));
    assert_eq!(defense.verdict, DefenseVerdict::Fail);
}

#[test]
fn stays_inconclusive_instead_of_passing_without_measurements() {
    let empty = MessagingStatsSnapshot::default();
    let defense = evaluate_messaging_defense_lines(&empty, MessagingDefenseLineLimits::default());
    assert_eq!(defense.context_share.passed, None);
    assert_eq!(defense.turn_share.passed, None);
    assert_eq!(defense.cost_share.passed, None);
    assert_eq!(defense.verdict, DefenseVerdict::Inconclusive);
}

#[test]
fn unknown_lines_never_override_a_failure() {
    // A failed cost line with unknown context/turn lines still fails.
    let only_cost = MessagingStatsSnapshot {
        arrivals: ArrivalCounts::default(),
        model_steps: StepCounts {
            total: 4,
            last5m: 4,
            tokens: 100,
        },
        ingestion_steps: StepCounts {
            total: 0,
            last5m: 0,
            tokens: 50,
        },
        context: ContextShape::default(),
        sends: SendCounts::default(),
    };
    let defense =
        evaluate_messaging_defense_lines(&only_cost, MessagingDefenseLineLimits::default());
    assert_eq!(defense.context_share.passed, None);
    assert_eq!(defense.cost_share.passed, Some(false));
    assert_eq!(defense.verdict, DefenseVerdict::Fail);
}

#[test]
fn treats_the_exact_limit_as_passing_and_honors_overrides() {
    let at_limit = MessagingStatsSnapshot {
        context: ContextShape {
            estimated_agent_message_tokens: 250,
            context_tokens: Some(1_000),
            share: Some(0.25),
        },
        ..snapshot()
    };
    let at_limit_defense =
        evaluate_messaging_defense_lines(&at_limit, MessagingDefenseLineLimits::default());
    assert_eq!(at_limit_defense.context_share.passed, Some(true));

    // A tighter override flips the same measurement to a failure.
    let overridden = evaluate_messaging_defense_lines(
        &at_limit,
        MessagingDefenseLineLimits {
            context_share: 0.2,
            ..MessagingDefenseLineLimits::default()
        },
    );
    assert_eq!(overridden.context_share.limit, 0.2);
    assert_eq!(overridden.context_share.passed, Some(false));
    assert_eq!(overridden.verdict, DefenseVerdict::Fail);
}

#[test]
fn default_limits_match_the_pre_registered_values() {
    assert_eq!(MESSAGING_DEFENSE_LINE_LIMITS.context_share, 0.25);
    assert_eq!(MESSAGING_DEFENSE_LINE_LIMITS.turn_share, 1.0 / 3.0);
    assert_eq!(MESSAGING_DEFENSE_LINE_LIMITS.cost_share, 0.2);
    assert_eq!(
        MessagingDefenseLineLimits::default(),
        MESSAGING_DEFENSE_LINE_LIMITS
    );
}

// ---------------------------------------------------------------------------
// Config and prompts
// ---------------------------------------------------------------------------

#[test]
fn parses_arguments_with_defaults_and_validates_the_model() {
    let config = parse_eval_args(&args(&[
        "--model",
        "internal/glm-5.2-fast",
        "--sizes",
        "2,10",
    ]))
    .expect("parses");
    assert_eq!(config.model, "internal/glm-5.2-fast");
    assert_eq!(config.sizes, vec![2, 10]);
    assert_eq!(config.message_size, MessageSize::Short);
    assert_eq!(config.pattern, ArrivalPattern::Spread);
    assert_eq!(config.trials, 1);
    assert_eq!(config.gap_seconds, 2.0);
    assert_eq!(config.timeout_minutes, 15.0);
    assert_eq!(config.seed, 1);

    assert_eq!(
        parse_eval_args(&args(&["--sizes", "2"])),
        Err(EvalArgsError::Message(
            "--model is required (provider/id)".to_string()
        ))
    );
    assert_eq!(
        parse_eval_args(&args(&["--model", "x/y", "--nonsense"])),
        Err(EvalArgsError::Message(
            "Unknown argument: --nonsense".to_string()
        ))
    );
    assert_eq!(
        parse_eval_args(&args(&["--model"])),
        Err(EvalArgsError::Message(
            "Missing value for --model".to_string()
        ))
    );
    assert_eq!(
        parse_eval_args(&args(&["--help"])),
        Err(EvalArgsError::Help)
    );
}

#[test]
fn drops_non_positive_sizes_and_requires_at_least_one() {
    let config =
        parse_eval_args(&args(&["--model", "m", "--sizes", "2, 0, x, 5"])).expect("parses");
    assert_eq!(config.sizes, vec![2, 5]);
    assert_eq!(
        parse_eval_args(&args(&["--model", "m", "--sizes", "0, nope"])),
        Err(EvalArgsError::Message(
            "--sizes must contain at least one positive size".to_string()
        ))
    );
}

#[test]
fn default_out_dir_is_stamped_when_omitted() {
    let config = parse_eval_args(&args(&["--model", "m"])).expect("parses");
    assert!(
        config.out_dir.starts_with("swarm-eval-reports/"),
        "{}",
        config.out_dir
    );
    let explicit = parse_eval_args(&args(&["--model", "m", "--out", "./reports"])).expect("parses");
    assert_eq!(explicit.out_dir, "./reports");
}

#[test]
fn builds_deterministic_child_prompts_per_pattern_and_message_size() {
    let config = eval_config();
    let short = build_child_prompt(1, 481, &config);
    assert!(short.contains("481"));
    assert!(short.contains("REPORT 481"));
    assert!(short.contains("asyncio.sleep(2)"));
    assert!(!short.contains("filler"));

    let burst = build_child_prompt(
        0,
        481,
        &SwarmEvalConfig {
            pattern: ArrivalPattern::Burst,
            ..config.clone()
        },
    );
    assert!(burst.contains("Reply immediately"));
    assert!(!burst.contains("asyncio.sleep"));

    let long = build_child_prompt(
        0,
        481,
        &SwarmEvalConfig {
            message_size: MessageSize::Long,
            ..config
        },
    );
    assert!(long.contains("200 lines each containing only the word filler"));
}

#[test]
fn embeds_every_child_prompt_and_the_answer_format_in_the_orchestrator_prompt() {
    let prompt = build_orchestrator_prompt(&eval_config(), 3, &[111, 222, 333]);
    assert!(prompt.contains("3-subagent crew"));
    assert!(prompt.contains("111"));
    assert!(prompt.contains("333"));
    assert!(prompt.contains("ANSWER:"));
    assert!(prompt.contains("\"\"\""));
}

#[test]
fn seeds_secrets_deterministically() {
    let first = seeded_secrets(1, 5);
    assert_eq!(first, seeded_secrets(1, 5));
    assert_ne!(first, seeded_secrets(2, 5));
    assert_eq!(first.len(), 5);
    assert!(first.iter().all(|secret| (100..1000).contains(secret)));
}

// ---------------------------------------------------------------------------
// Verification and reporting
// ---------------------------------------------------------------------------

#[test]
fn parses_the_answer_line() {
    assert_eq!(
        parse_answer_line(Some("work done\nANSWER: 12, 34, 56")),
        Some(vec![12, 34, 56])
    );
    assert_eq!(parse_answer_line(Some("ANSWER: 12,34")), Some(vec![12, 34]));
    assert_eq!(parse_answer_line(Some("answer: 7")), Some(vec![7]));
    assert_eq!(parse_answer_line(Some("ANSWER:12")), Some(vec![12]));
    assert_eq!(parse_answer_line(Some("no answer")), None);
    assert_eq!(parse_answer_line(Some("ANSWER:")), None);
    assert_eq!(parse_answer_line(Some("ANSWER: 1,")), None);
    assert_eq!(parse_answer_line(None), None);
}

#[test]
fn turns_task_failures_and_rate_limit_errors_into_instant_fails() {
    let config = eval_config();
    let ok = trial_result_from_snapshot(&config, 5, 1, &snapshot(), true, None, 12.0);
    assert_eq!(ok.verdict, DefenseVerdict::Pass);

    let wrong_answer = trial_result_from_snapshot(&config, 5, 1, &snapshot(), false, None, 12.0);
    assert_eq!(wrong_answer.verdict, DefenseVerdict::Fail);

    let rate_limited = trial_result_from_snapshot(
        &config,
        5,
        1,
        &snapshot(),
        true,
        Some("rate-limit error during trial".to_string()),
        12.0,
    );
    assert_eq!(rate_limited.verdict, DefenseVerdict::Fail);
    assert_eq!(
        rate_limited.instant_fail.as_deref(),
        Some("rate-limit error during trial")
    );
}

#[test]
fn renders_a_markdown_report_with_every_trial_and_a_verdict_summary() {
    let config = eval_config();
    let rows = vec![
        trial_result_from_snapshot(&config, 2, 1, &snapshot(), true, None, 10.0),
        trial_result_from_snapshot(
            &config,
            2,
            2,
            &MessagingStatsSnapshot {
                ingestion_steps: StepCounts {
                    total: 5,
                    last5m: 5,
                    tokens: 5_000,
                },
                ..snapshot()
            },
            true,
            None,
            11.0,
        ),
    ];
    let report = render_markdown_report(&rows, &config);
    assert!(report.contains("# Swarm starvation eval report"));
    assert!(report.contains("context <= 25%"));
    assert!(report.contains("turns <= 33%"));
    assert!(report.contains("cost <= 20%"));
    assert!(report.contains("| 2 | 1 |"));
    assert!(report.contains("1/2 trials failed"));
}

#[test]
fn reports_all_passed_when_no_trial_fails() {
    let config = eval_config();
    let rows = vec![trial_result_from_snapshot(
        &config,
        2,
        1,
        &snapshot(),
        true,
        None,
        10.0,
    )];
    let report = render_markdown_report(&rows, &config);
    assert!(report.contains("All 1 trials passed every defense line."));
}

// ---------------------------------------------------------------------------
// Transcript-derived snapshots
// ---------------------------------------------------------------------------

#[test]
fn derives_arrivals_steps_and_context_share_from_a_transcript() {
    let messages = vec![
        json!({ "role": "user", "content": "orchestrator prompt" }),
        json!({ "role": "assistant", "usage": { "totalTokens": 1000 }, "stopReason": "toolUse" }),
        json!({
            "role": "custom",
            "customType": "agent_message",
            "content": "12345678",
            "details": { "id": "agentmsg_1" }
        }),
        json!({ "role": "assistant", "usage": { "totalTokens": 200 }, "stopReason": "stop" }),
        json!({ "role": "toolResult", "content": "tool output" }),
        json!({ "role": "assistant", "usage": { "totalTokens": 300 }, "stopReason": "stop" }),
        json!({ "role": "assistant", "usage": { "totalTokens": 999 }, "stopReason": "error" }),
    ];
    let snapshot = snapshot_from_transcript(&messages, Some(2_000));
    assert_eq!(snapshot.arrivals.total, 1);
    // Three completed steps (the error stop is skipped).
    assert_eq!(snapshot.model_steps.total, 3);
    assert_eq!(snapshot.model_steps.tokens, 1_500);
    // Both steps of the agent-triggered turn count: the primary input stays
    // the agent message across the turn's tool result.
    assert_eq!(snapshot.ingestion_steps.total, 2);
    assert_eq!(snapshot.ingestion_steps.tokens, 500);
    // 8 chars -> 2 tokens; share = 2/2000.
    assert_eq!(snapshot.context.estimated_agent_message_tokens, 2);
    assert_eq!(snapshot.context.share, Some(2.0 / 2_000.0));
}

#[test]
fn transcript_context_share_is_unknown_without_context_tokens() {
    let messages = vec![json!({
        "role": "custom",
        "customType": "agent_message",
        "content": "hello"
    })];
    let snapshot = snapshot_from_transcript(&messages, None);
    assert_eq!(snapshot.arrivals.total, 1);
    assert_eq!(snapshot.context.context_tokens, None);
    assert_eq!(snapshot.context.share, None);
    // Unknown context still never silently passes.
    let defense =
        evaluate_messaging_defense_lines(&snapshot, MessagingDefenseLineLimits::default());
    assert_eq!(defense.context_share.passed, None);
    assert_eq!(defense.verdict, DefenseVerdict::Inconclusive);
}

#[test]
fn a_plain_user_message_resets_the_ingestion_trigger() {
    let messages = vec![
        json!({ "role": "custom", "customType": "agent_message", "content": "hi" }),
        json!({ "role": "user", "content": "back to the user" }),
        json!({ "role": "assistant", "usage": { "totalTokens": 50 }, "stopReason": "stop" }),
    ];
    let snapshot = snapshot_from_transcript(&messages, Some(1_000));
    assert_eq!(snapshot.model_steps.total, 1);
    assert_eq!(snapshot.ingestion_steps.total, 0);
}
