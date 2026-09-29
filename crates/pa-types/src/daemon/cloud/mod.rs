//! Cross-boundary family wire types: the v3 family surface of the cloud
//! session protocol.
//!
//! Port of the family-messaging slice of
//! `packages/coding-agent/src/core/cloud/protocol.ts` (protocol v3, TS
//! `origin/feat/direct-cloud-sandbox @ 193d42bf`): `CloudFamilyInfo`,
//! `CloudAgentMessageSender`, `CloudFamilyRow`, the guest-to-local
//! `family_roster_request` / `agent_message_request` events, the
//! local-to-guest `family_roster_result` / `agent_message_result` commands,
//! the `send_message` cloud command, and the terminal receipt payload. Field
//! names, kind discriminants, bounds, and problem strings match the TS wire
//! exactly, so a Rust endpoint serializes byte-identical frames and rejects
//! malformed ones with the TS messages.
//!
//! String bounds count UTF-16 code units (TS `.length` semantics), including
//! the astral plane, so an id or selector valid here is valid in TS and vice
//! versa.
//!
//! Receipts never claim delivery on this surface: a `CloudAgentMessageReceipt`
//! exists only after the receiving side admitted the message (the journaled
//! `agent_message_result` carries it), so "durably admitted but unanswered"
//! is a request state, never a receipt state.

use serde_json::Value;

pub const CLOUD_PROTOCOL_NAME: &str = "prime-agent.cloud";
/// Version 3 adds the cross-boundary family surface; hello requires an exact
/// match, so a mixed-version pair refuses attachment (TS
/// `CLOUD_PROTOCOL_VERSION`).
pub const CLOUD_PROTOCOL_VERSION: u64 = 3;
/// The hello capability that gates the family surface (TS `family_messages`).
pub const CLOUD_CAPABILITY_FAMILY_MESSAGES: &str = "family_messages";

pub const CLOUD_MAX_MESSAGE_BYTES: usize = 1_048_576;
pub const CLOUD_MAX_JSON_DEPTH: usize = 64;
pub const CLOUD_MAX_ID_CHARS: usize = 128;
pub const CLOUD_MAX_PROMPT_CHARS: usize = 65_536;
pub const CLOUD_MAX_ERROR_CHARS: usize = 2_048;
pub const CLOUD_MAX_TIMESTAMP_CHARS: usize = 64;
pub const CLOUD_MAX_PATH_CHARS: usize = 4_096;
pub const CLOUD_MAX_SESSION_NAME_CHARS: usize = 128;
/// Bound on one remote-family roster batch (`family_roster_result` entries).
pub const CLOUD_MAX_FAMILY_ROWS: usize = 64;
/// Bound on the terminal receipt `result` payload (canonical JSON string).
pub const CLOUD_MAX_RECEIPT_RESULT_CHARS: usize = 2_048;
/// Bound on an agent-message request id and a remote target selector.
pub const CLOUD_MAX_SELECTOR_CHARS: usize = 128;

/// TS `CloudEvent.kind` one-of list, joined exactly as the TS validator
/// reports it (used for the problem string; only the family kinds have typed
/// shapes here).
pub const CLOUD_EVENT_KINDS: &str = "command_accepted, command_state, session_status, output_delta, session_entry, session_event, session_meta, roster_delta, child_update, usage, family_roster_request, agent_message_request";
/// TS `CloudCommandRequest.kind` one-of list, joined exactly as the TS
/// validator reports it.
pub const CLOUD_COMMAND_KINDS: &str = "open_session, prompt, steer, follow_up, abort, send_message, set_model, set_thinking_level, set_session_name, compact, cancel_child, delete_child, extension_ui_response, release, family_roster_result, agent_message_result";

// ---------------------------------------------------------------------------
// Deterministic JSON (TS protocol.ts canonicalJson)
// ---------------------------------------------------------------------------

/// Deterministic JSON: recursively sorted keys, no whitespace, plain
/// values only, finite numbers, depth bounded by [`CLOUD_MAX_JSON_DEPTH`].
/// Two deep-equal values serialize to the same bytes, so digests are stable
/// across processes and key order never matters (TS `canonicalJson`).
///
/// # Errors
///
/// Returns a problem string when the value nests deeper than
/// [`CLOUD_MAX_JSON_DEPTH`].
pub fn canonical_json(value: &Value) -> Result<String, String> {
    let mut out = String::new();
    canonicalize_into(&mut out, value, 0)?;
    Ok(out)
}

/// Renders one JSON number exactly as the TS canonicalizer does, i.e. as
/// JavaScript `String(number)`: shortest round-trip digits, integral floats
/// without a trailing `.0`, plain decimal form inside `[1e-6, 1e21)`, and
/// `1e+21` (with the sign) above it. `serde_json`'s own rendering diverges
/// from JS on all three (e.g. `2.0`, `-0`, `1e20`), and digests are computed
/// over these bytes, so parity is load-bearing. JSON integers beyond ±2^53
/// parse as floats in JS (rounding to the nearest f64), so they normalize
/// through f64 here too.
// The rounding of integers beyond ±2^53 through f64 is the whole point: JS
// `JSON.parse` produces the same rounded double, and the canonical bytes
// must match the TS side's.
#[allow(clippy::cast_precision_loss)]
fn canonical_number(number: &serde_json::Number) -> String {
    const JS_SAFE_INTEGER: u64 = 9_007_199_254_740_992; // 2^53
    let mut buffer = ryu_js::Buffer::new();
    if let Some(value) = number.as_u64() {
        if value <= JS_SAFE_INTEGER {
            return value.to_string();
        }
        return buffer.format(value as f64).to_string();
    }
    if let Some(value) = number.as_i64() {
        if value.unsigned_abs() <= JS_SAFE_INTEGER {
            return value.to_string();
        }
        return buffer.format(value as f64).to_string();
    }
    buffer
        .format(number.as_f64().unwrap_or_default())
        .to_string()
}

fn json_string(out: &mut String, text: &str) -> Result<(), String> {
    let encoded = serde_json::to_string(text).map_err(|error| error.to_string())?;
    out.push_str(&encoded);
    Ok(())
}

fn canonicalize_into(out: &mut String, value: &Value, depth: usize) -> Result<(), String> {
    if depth > CLOUD_MAX_JSON_DEPTH {
        return Err(format!(
            "canonical JSON depth exceeds {CLOUD_MAX_JSON_DEPTH}"
        ));
    }
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(true) => out.push_str("true"),
        Value::Bool(false) => out.push_str("false"),
        Value::Number(number) => {
            out.push_str(&canonical_number(number));
        }
        Value::String(text) => json_string(out, text)?,
        Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                canonicalize_into(out, item, depth + 1)?;
            }
            out.push(']');
        }
        Value::Object(map) => {
            out.push('{');
            let mut keys: Vec<&str> = map.keys().map(String::as_str).collect();
            keys.sort_unstable();
            for (index, key) in keys.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                let Some(item) = map.get(*key) else {
                    return Err("canonical JSON key vanished".to_string());
                };
                json_string(out, key)?;
                out.push(':');
                canonicalize_into(out, item, depth + 1)?;
            }
            out.push('}');
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Module layout
// ---------------------------------------------------------------------------

mod checks;
mod family;
#[cfg(test)]
mod tests;
mod validation;

pub use family::*;
pub use validation::{
    cloud_agent_message_sender_problem, cloud_family_command_problem, cloud_family_event_problem,
    cloud_family_info_problem, cloud_family_rows_problem, cloud_send_message_problem,
};
