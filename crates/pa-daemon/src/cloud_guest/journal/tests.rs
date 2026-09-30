//! Guest command journal unit battery: the admit/claim/settle state
//! machine, the digest-deduped stable id, the uncertain restore, and
//! the compaction rewrite.

use pa_types::daemon::cloud::{cloud_request_digest, CloudCommandState};

use crate::cloud_guest::journal::{parse_claimed_request, GuestAdmission, GuestCommandJournal};

fn prompt_request_value(text: &str) -> serde_json::Value {
    serde_json::json!({"kind": "prompt", "text": text})
}

fn journal() -> (GuestCommandJournal, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("command-journal.ndjson");
    (GuestCommandJournal::open(&path).unwrap(), dir)
}

#[test]
fn admit_is_digest_deduped_and_conflicts_never_readmit() {
    let (mut journal, _dir) = journal();
    let (admission, receipt) = journal
        .admit("cmd_one", &prompt_request_value("turn"))
        .unwrap();
    assert_eq!(admission, GuestAdmission::New);
    assert_eq!(receipt.state, CloudCommandState::Accepted);
    // The same id and digest replays the receipt.
    let (admission, duplicate) = journal
        .admit("cmd_one", &prompt_request_value("turn"))
        .unwrap();
    assert_eq!(admission, GuestAdmission::Duplicate);
    assert_eq!(duplicate, receipt);
    // The same id with a different request conflicts.
    let (admission, _conflict) = journal
        .admit("cmd_one", &prompt_request_value("a different turn"))
        .unwrap();
    assert_eq!(admission, GuestAdmission::Conflict);
    // The stored digest is the canonical request digest.
    let receipt = journal.receipt("cmd_one").unwrap();
    assert_eq!(
        receipt.digest,
        cloud_request_digest(&prompt_request_value("turn")).unwrap()
    );
}

#[test]
fn claim_settles_through_running_and_guards_terminal() {
    let (mut journal, _dir) = journal();
    journal
        .admit("cmd_one", &prompt_request_value("turn"))
        .unwrap();
    let claimed = journal.claim_next_pending().unwrap().unwrap();
    assert_eq!(claimed.receipt.state, CloudCommandState::Running);
    assert!(
        parse_claimed_request(&claimed.request).is_ok(),
        "the claimed request parses back"
    );
    // The next claim finds nothing: one claim at a time.
    assert!(journal.claim_next_pending().unwrap().is_none());
    journal
        .complete("cmd_one", Some("{\"answer\":true}"))
        .unwrap();
    assert_eq!(
        journal.receipt("cmd_one").unwrap().state,
        CloudCommandState::Completed
    );
    assert_eq!(
        journal.receipt("cmd_one").unwrap().result,
        Some("{\"answer\":true}".to_string())
    );
    // Terminal states never transition again.
    assert!(journal.complete("cmd_one", None).is_err());
    // Unknown ids are refused.
    assert!(journal.fail("cmd_missing", Some("nope")).is_err());
}

#[test]
fn restart_replays_pending_and_marks_running_uncertain() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("command-journal.ndjson");
    {
        let mut journal = GuestCommandJournal::open(&path).unwrap();
        journal
            .admit("cmd_running", &prompt_request_value("mid"))
            .unwrap();
        journal
            .admit("cmd_pending", &prompt_request_value("queued"))
            .unwrap();
        journal.claim_next_pending().unwrap().unwrap();
        // The claim fsynced cmd_running's running transition (the oldest
        // accepted command claims first); cmd_pending was never
        // dispatched.
    }
    let mut restored = GuestCommandJournal::open(&path).unwrap();
    let uncertain = restored.list_uncertain();
    assert_eq!(uncertain.len(), 1);
    assert_eq!(uncertain[0].command_id, "cmd_running");
    assert!(uncertain[0].uncertain);
    // The restored pending command claims; the uncertain one never
    // does.
    let claimed = restored.claim_next_pending().unwrap().unwrap();
    assert_eq!(claimed.receipt.command_id, "cmd_pending");
    // The uncertain command is not claimable until the host requeues.
    let next = restored.claim_next_pending().unwrap();
    assert!(next.is_none());
    restored.requeue("cmd_running").unwrap();
    let claimed = restored.claim_next_pending().unwrap().unwrap();
    assert_eq!(claimed.receipt.command_id, "cmd_running");
    assert!(
        !claimed.receipt.uncertain,
        "the requeue settles uncertainty"
    );
    // Requeueing a non-uncertain command is refused.
    assert!(restored.requeue("cmd_pending").is_err());
}

#[test]
fn crash_truncated_tail_folds_to_the_last_complete_record() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("command-journal.ndjson");
    {
        let mut journal = GuestCommandJournal::open(&path).unwrap();
        journal
            .admit("cmd_kept", &prompt_request_value("kept"))
            .unwrap();
    }
    // Simulate a crash mid-append: a truncated final line.
    let contents = std::fs::read_to_string(&path).unwrap();
    std::fs::write(&path, format!("{contents}{{\"version\":1,\"type\":\"admi")).unwrap();
    let restored = GuestCommandJournal::open(&path).unwrap();
    assert!(restored.receipt("cmd_kept").is_some());
    assert!(restored.receipt("cmd_gone").is_none());
}

#[test]
fn corrupt_records_fold_out() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("command-journal.ndjson");
    {
        let mut journal = GuestCommandJournal::open(&path).unwrap();
        journal
            .admit("cmd_kept", &prompt_request_value("kept"))
            .unwrap();
        journal
            .admit("cmd_dropped", &prompt_request_value("dropped"))
            .unwrap();
    }
    // Hand-corrupt the journal: a bad-version record and an admit whose
    // digest does not match its request both fold out.
    let contents = std::fs::read_to_string(&path).unwrap();
    let forged = format!(
        "{contents}{}\n{}\n",
        r#"{"version":2,"type":"admit","commandId":"cmd_new","digest":"sha256:0","request":"{}","recordedAt":"now"}"#,
        r#"{"version":1,"type":"admit","commandId":"cmd_bad_digest","digest":"sha256:0000000000000000000000000000000000000000000000000000000000000000","request":"{\"kind\":\"abort\"}","recordedAt":"now"}"#
    );
    std::fs::write(&path, forged).unwrap();
    let restored = GuestCommandJournal::open(&path).unwrap();
    assert!(restored.receipt("cmd_kept").is_some());
    assert!(restored.receipt("cmd_dropped").is_some());
    assert!(restored.receipt("cmd_new").is_none());
    assert!(restored.receipt("cmd_bad_digest").is_none());
}

#[test]
fn compaction_preserves_state_and_record_bounds() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("command-journal.ndjson");
    let mut journal = GuestCommandJournal::open(&path).unwrap();
    // Shrink the compaction window so the rewrite path runs in-test.
    journal.compact_after_records_for_tests(4);
    journal.admit("cmd_a", &prompt_request_value("a")).unwrap();
    journal.admit("cmd_b", &prompt_request_value("b")).unwrap();
    let claimed = journal.claim_next_pending().unwrap().unwrap();
    journal.complete(&claimed.receipt.command_id, None).unwrap();
    journal.admit("cmd_c", &prompt_request_value("c")).unwrap();
    assert_eq!(journal.record_count_for_tests(), 4, "the rewrite ran");
    // The compacted journal reloads with the same state.
    let mut restored = GuestCommandJournal::open(&path).unwrap();
    assert_eq!(
        restored.receipt("cmd_a").unwrap().state,
        CloudCommandState::Completed
    );
    assert_eq!(
        restored.receipt("cmd_b").unwrap().state,
        CloudCommandState::Accepted
    );
    assert_eq!(
        restored.receipt("cmd_c").unwrap().state,
        CloudCommandState::Accepted
    );
    let claimed = restored.claim_next_pending().unwrap().unwrap();
    assert_eq!(claimed.receipt.command_id, "cmd_b");
}

#[test]
fn settle_bounds_match_the_wire_limits() {
    let (mut journal, _dir) = journal();
    journal
        .admit("cmd_one", &prompt_request_value("turn"))
        .unwrap();
    assert!(journal.complete("cmd_one", Some("")).is_err());
    assert!(journal
        .complete("cmd_one", Some(&"x".repeat(2049)))
        .is_err());
    assert!(journal.fail("cmd_one", Some("")).is_err());
    assert!(journal.fail("cmd_one", Some(&"x".repeat(2049))).is_err());
    assert!(journal
        .receipt("cmd_one")
        .is_some_and(|receipt| receipt.state == CloudCommandState::Accepted));
}

#[test]
fn restored_admissions_claim_in_order_and_settle() {
    // A regression shape for the fold: many small journals restore the
    // same admission map regardless of the id order.
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("command-journal.ndjson");
    let mut ids = Vec::new();
    {
        let mut journal = GuestCommandJournal::open(&path).unwrap();
        for index in 0..10 {
            let id = format!("cmd_{index}");
            journal.admit(&id, &prompt_request_value("turn")).unwrap();
            ids.push(id);
        }
    }
    let mut restored = GuestCommandJournal::open(&path).unwrap();
    let mut seen = std::collections::HashSet::new();
    while let Some(claimed) = restored.claim_next_pending().unwrap() {
        seen.insert(claimed.receipt.command_id.clone());
        restored.cancel(&claimed.receipt.command_id).unwrap();
        assert_eq!(
            restored.receipt(&claimed.receipt.command_id).unwrap().state,
            CloudCommandState::Cancelled
        );
    }
    assert_eq!(seen.len(), 10);
    for id in &ids {
        assert!(seen.contains(id), "every admission claimed in order");
    }
}

#[test]
fn torn_multibyte_tail_never_empties_the_journal() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("command-journal.ndjson");
    {
        let mut journal = GuestCommandJournal::open(&path).unwrap();
        journal
            .admit("cmd_done", &prompt_request_value("ünïcode turn"))
            .unwrap();
        let claimed = journal.claim_next_pending().unwrap().unwrap();
        assert_eq!(claimed.receipt.command_id, "cmd_done");
        journal.complete("cmd_done", None).unwrap();
    }
    // Simulate a power loss mid-append: a torn final line whose UTF-8
    // sequence is split (invalid bytes the string reader would reject
    // wholesale).
    let mut contents = std::fs::read(&path).unwrap();
    contents.extend_from_slice(b"{\"version\":1,\"type\":\"admit\",\"commandId\":\"cmd_\xF0\x9F");
    std::fs::write(&path, &contents).unwrap();
    // The recovered journal still knows the completed command: a
    // retry of the same id is a duplicate, never a fresh admission.
    let mut restored = GuestCommandJournal::open(&path).unwrap();
    assert_eq!(
        restored.receipt("cmd_done").unwrap().state,
        CloudCommandState::Completed
    );
    let (admission, receipt) = restored
        .admit("cmd_done", &prompt_request_value("ünïcode turn"))
        .unwrap();
    assert_eq!(admission, GuestAdmission::Duplicate);
    assert_eq!(receipt.state, CloudCommandState::Completed);
}

#[test]
fn unreadable_journal_fails_closed_instead_of_loading_empty() {
    let dir = tempfile::TempDir::new().unwrap();
    // A directory where the journal file belongs: every read fails; an
    // empty recovery would re-admit completed commands as new.
    let path = dir.path().join("command-journal.ndjson");
    std::fs::create_dir(&path).unwrap();
    assert!(GuestCommandJournal::open(&path).is_err());
}

#[test]
fn corrupted_midfile_record_fails_closed() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("command-journal.ndjson");
    {
        let mut journal = GuestCommandJournal::open(&path).unwrap();
        journal.admit("cmd_a", &prompt_request_value("a")).unwrap();
        journal.admit("cmd_b", &prompt_request_value("b")).unwrap();
    }
    // Corrupt a COMPLETE record (not the torn tail): silently folding
    // it away could lose settled state, so the open refuses.
    let contents = std::fs::read_to_string(&path).unwrap();
    let mut lines: Vec<&str> = contents.split('\n').collect();
    lines.insert(1, "{ not json at all");
    let forged = lines.join("\n");
    std::fs::write(&path, forged).unwrap();
    assert!(GuestCommandJournal::open(&path).is_err());
}
