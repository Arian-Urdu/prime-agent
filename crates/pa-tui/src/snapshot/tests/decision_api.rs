//! The Decision API footer state: the attach snapshot seeds the session's
//! provider and live `decision_api_status` rows switch it.

use super::*;
use pa_types::slash_commands::DecisionApiProvider;

/// The Decision API footer follows the session: the attach state seeds the
/// provider (absent reads off) and a live `decision_api_status` row switches it.
#[test]
fn decision_api_state_reads_from_the_attach_and_the_status_rows() {
    let off = reconstruct(&attach_data_from_response(slim_attach()).unwrap());
    let mut attach = slim_attach();
    attach["snapshot"]["state"]["decisionApi"] = json!("clef");
    let on = reconstruct(&attach_data_from_response(attach).unwrap());
    assert_eq!(
        (off.decision_api, on.decision_api),
        (None, Some(DecisionApiProvider::Clef))
    );

    let status_row = |provider: Option<&str>| {
        event_to_update(&json!({
            "type": "message_start",
            "message": {
                "role": "custom",
                "customType": "decision_api_status",
                "content": "[decision-api]",
                "display": false,
                "details": { "provider": provider },
                "timestamp": 1,
            },
        }))
    };
    assert_eq!(
        (status_row(Some("jev")), status_row(None)),
        (
            Some(TurnUpdate::DecisionApiChanged {
                provider: Some(DecisionApiProvider::Jev)
            }),
            Some(TurnUpdate::DecisionApiChanged { provider: None })
        )
    );
}
