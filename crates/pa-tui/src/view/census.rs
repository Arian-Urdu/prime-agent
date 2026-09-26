//! PROBE-ONLY (tui-memory lane, branch lane/hillclimb-tui-memory-probe): a
//! SIGUSR1-triggered census of the transcript state the TUI retains.
//! Never ships: this file exists to attribute RSS bytes to concrete
//! structures (chat entries, entry row caches, markdown block caches).
#![allow(dead_code)]

use super::AgentView;
use crate::chat::ChatEntry;
use serde_json::json;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};

pub static CENSUS_REQUESTED: AtomicBool = AtomicBool::new(false);

extern "C" fn census_signal_handler(_sig: i32) {
    CENSUS_REQUESTED.store(true, Ordering::Relaxed);
}

/// Install the SIGUSR1 census trigger when PA_TUI_CENSUS_FILE is set.
/// The handler only flips an atomic; the walk runs on the draw path.
pub fn install_probe() {
    if std::env::var("PA_TUI_CENSUS_FILE").is_ok() {
        unsafe {
            libc::signal(libc::SIGUSR1, census_signal_handler as usize);
        }
    }
}

/// (lines, spans, content_bytes) of a rendered row set.
fn line_stats(lines: &[crate::Line]) -> (usize, usize, usize) {
    let mut spans = 0usize;
    let mut bytes = 0usize;
    for line in lines {
        spans += line.len();
        for span in line {
            bytes += span.content.len();
        }
    }
    (lines.len(), spans, bytes)
}

fn value_bytes(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::String(s) => s.len(),
        serde_json::Value::Number(_) | serde_json::Value::Bool(_) => 8,
        serde_json::Value::Null => 0,
        serde_json::Value::Array(items) => 16 + items.iter().map(value_bytes).sum::<usize>(),
        serde_json::Value::Object(map) => {
            16 + map
                .iter()
                .map(|(k, v)| 16 + k.len() + value_bytes(v))
                .sum::<usize>()
        }
    }
}

fn entry_kind(entry: &ChatEntry) -> &'static str {
    match entry {
        ChatEntry::Status { .. } => "status",
        ChatEntry::User { .. } => "user",
        ChatEntry::SlashCommand { .. } => "slash",
        ChatEntry::CompactionSummary { .. } => "compaction",
        ChatEntry::Assistant(_) => "assistant",
        ChatEntry::Tool(_) => "tool",
        ChatEntry::AgentMessage(_) => "agent_message",
        ChatEntry::SkillInvocation(_) => "skill_invocation",
        ChatEntry::InjectedPrompt(_) => "injected_prompt",
        ChatEntry::BashExecution(_) => "bash_execution",
        ChatEntry::ShellCompletion(_) => "shell_completion",
        ChatEntry::RefinementOutcome(_) => "refinement_outcome",
        ChatEntry::CustomPanel(_) => "custom_panel",
        ChatEntry::ClientMarkdown { .. } => "client_markdown",
        ChatEntry::ClientText { .. } => "client_text",
        ChatEntry::ChangelogPanel { .. } => "changelog_panel",
    }
}

fn entry_content_bytes(entry: &ChatEntry) -> usize {
    match entry {
        ChatEntry::Status { text, .. }
        | ChatEntry::User { text }
        | ChatEntry::SlashCommand { text } => text.len(),
        ChatEntry::CompactionSummary {
            summary,
            custom_instructions,
            ..
        } => {
            summary.len()
                + custom_instructions
                    .as_ref()
                    .map(|s| s.len())
                    .unwrap_or(0)
        }
        ChatEntry::Assistant(message) => {
            let blocks = message
                .blocks
                .iter()
                .map(|block| match block {
                    crate::chat::MessageBlock::Thinking(text)
                    | crate::chat::MessageBlock::Text(text) => 16 + text.len(),
                })
                .sum::<usize>();
            blocks + message.error.as_ref().map(|s| s.len()).unwrap_or(0)
        }
        ChatEntry::Tool(card) => {
            16
                + card.id.len()
                + card.name.len()
                + value_bytes(&card.args)
                + card
                    .result
                    .as_ref()
                    .map(|result| {
                        16
                            + result.content.iter().map(value_bytes).sum::<usize>()
                            + value_bytes(&result.details)
                    })
                    .unwrap_or(0)
        }
        ChatEntry::BashExecution(card) => format!("{card:?}").len(),
        other => format!("{other:?}").len(),
    }
}

impl AgentView {
    pub(crate) fn census(&self) -> serde_json::Value {
        let mut kinds = std::collections::BTreeMap::new();
        let mut content_bytes = 0usize;
        for entry in &self.chat {
            let kind = entry_kind(entry);
            *kinds.entry(kind).or_insert(0usize) += 1;
            content_bytes += entry_content_bytes(entry);
        }

        let mut layout_slots = 0usize;
        let mut layout_lines = 0usize;
        let mut layout_spans = 0usize;
        let mut layout_content_bytes = 0usize;
        let mut layout_by_detail = [0usize; 3];
        let mut largest_layout: Vec<(usize, usize, usize)> = Vec::new();
        for (index, slots) in self.entry_layout.iter().enumerate() {
            for (detail, slot) in slots.iter().enumerate() {
                if let Some(layout) = slot {
                    layout_slots += 1;
                    layout_by_detail[detail] += 1;
                    let (lines, spans, bytes) = line_stats(&layout.rows);
                    layout_lines += lines;
                    layout_spans += spans;
                    layout_content_bytes += bytes;
                    largest_layout.push((index, detail, bytes));
                }
            }
        }
        largest_layout.sort_by_key(|(_, _, bytes)| usize::MAX - *bytes);

        let mut heights_some = [0usize; 3];
        for slots in &self.entry_heights {
            for (detail, slot) in slots.iter().enumerate() {
                if slot.is_some() {
                    heights_some[detail] += 1;
                }
            }
        }

        let md = self.md_caches.borrow();
        let md_cache_entries = md.len();
        let mut md_blocks = 0usize;
        let mut md_key_bytes = 0usize;
        let mut md_lines = 0usize;
        let mut md_spans = 0usize;
        let mut md_content_bytes = 0usize;
        let mut md_by_entry = Vec::new();
        for (index, cache) in md.iter() {
            let (blocks, key_bytes, lines, spans, bytes) = cache.stats();
            md_blocks += blocks;
            md_key_bytes += key_bytes;
            md_lines += lines;
            md_spans += spans;
            md_content_bytes += bytes;
            md_by_entry.push((*index, bytes));
        }
        md_by_entry.sort_by_key(|(_, bytes)| usize::MAX - *bytes);

        let flushed_rows = self.flushed_frame.len();
        let flushed_bytes = self.flushed_frame.iter().map(|s| s.len()).sum::<usize>();
        let osc_rows = self.osc_last_rows.len();
        let osc_bytes = self.osc_last_rows.values().map(|s| s.len()).sum::<usize>();

        json!({
            "ts": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(0),
            "chat_entries": self.chat.len(),
            "chat_kinds": kinds,
            "chat_content_bytes": content_bytes,
            "layout_slots": layout_slots,
            "layout_slots_by_detail": layout_by_detail,
            "layout_lines": layout_lines,
            "layout_spans": layout_spans,
            "layout_content_bytes": layout_content_bytes,
            "layout_top10_by_bytes": largest_layout.iter().take(10)
                .map(|(i, d, b)| json!({"entry": i, "detail": d, "content_bytes": b}))
                .collect::<Vec<_>>(),
            "heights_some_by_detail": heights_some,
            "md_cache_entries": md_cache_entries,
            "md_blocks": md_blocks,
            "md_key_bytes": md_key_bytes,
            "md_lines": md_lines,
            "md_spans": md_spans,
            "md_content_bytes": md_content_bytes,
            "md_top10_by_bytes": md_by_entry.iter().take(10)
                .map(|(i, b)| json!({"entry": i, "content_bytes": b}))
                .collect::<Vec<_>>(),
            "sparse_entries": self.sparse_entries.len(),
            "sparse_window": self.sparse_window.is_some(),
            "layout_width": self.layout_width,
            "flushed_frame": {"rows": flushed_rows, "bytes": flushed_bytes},
            "osc_last_rows": {"rows": osc_rows, "bytes": osc_bytes},
        })
    }
}

/// Run the census if a probe request is pending (draw-path, main thread).
pub fn maybe_census(view: &AgentView) {
    if !CENSUS_REQUESTED.swap(false, Ordering::Relaxed) {
        return;
    }
    let Ok(path) = std::env::var("PA_TUI_CENSUS_FILE") else {
        return;
    };
    let census = view.census();
    let line = format!("{census}\n");
    let _ = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .and_then(|mut file| file.write_all(line.as_bytes()));
}
