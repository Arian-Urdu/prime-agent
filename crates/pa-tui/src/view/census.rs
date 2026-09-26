//! PROBE-ONLY (tui-scroll-retain lane, branch lane/hillclimb-tui-scroll-probe):
//! a kick-file-triggered census of the transcript state the TUI retains,
//! focused on the POST-SCROLL retention: which entry_layout slots survive
//! the walk, their span-fragment structure (spans per line, capacity slack,
//! adjacent same-style merge potential), and whether heights/md caches leave
//! orphans. Never ships: this file exists to attribute retained RSS bytes
//! to concrete structures before any product edit.
#![allow(dead_code)]

use super::AgentView;
use crate::chat::ChatEntry;
use serde_json::json;
use std::io::Write;

/// Raw retained-structure counters of a rendered row set: line counts,
/// span counts, content bytes vs capacities (slack), zero-length spans,
/// adjacent same-style pairs, and the span count after coalescing every
/// adjacent equal-style run (dropping zero-length spans).
#[derive(Default, Clone, Copy)]
pub(crate) struct SpanStats {
    pub(crate) lines: usize,
    pub(crate) nonempty_lines: usize,
    pub(crate) spans: usize,
    pub(crate) content_len: usize,
    pub(crate) content_cap: usize,
    pub(crate) line_cap_slots: usize,
    pub(crate) rows_cap: usize,
    pub(crate) zero_len: usize,
    pub(crate) adj_same: usize,
    pub(crate) coalesced_spans: usize,
    /// adjacent same-style pairs where BOTH spans are default-styled
    /// (no fg/bg/modifier): merging these is byte-identical on the
    /// exit-flush ANSI path too (line_to_ansi emits no SGR for them).
    pub(crate) adj_same_default: usize,
    /// span count after coalescing ONLY default-styled runs (the
    /// byte-safe-everywhere compaction).
    pub(crate) coalesced_spans_default: usize,
    /// adjacent same-style pairs where both spans carry a style: merging
    /// them is frame-identical but changes exit-flush ANSI bytes (the
    /// duplicate SGR a merged span would drop).
    pub(crate) adj_same_styled: usize,
}

/// Whether a span is default-styled (line_to_ansi emits no SGR for it):
/// fg/bg unset and no add modifier.
fn is_default_styled(span: &crate::Span) -> bool {
    span.style.fg.is_none()
        && span.style.bg.is_none()
        && span.style.add_modifier.is_empty()
}

fn line_stats(rows: &Vec<crate::Line>) -> SpanStats {
    let mut s = SpanStats::default();
    s.lines = rows.len();
    s.rows_cap = rows.capacity();
    for line in rows {
        s.line_cap_slots += line.capacity();
        if !line.is_empty() {
            s.nonempty_lines += 1;
        }
        let mut prev: Option<(bool, ratatui::style::Style)> = None;
        for span in line {
            s.spans += 1;
            s.content_len += span.content.len();
            s.content_cap += span.content.capacity();
            if span.content.is_empty() {
                s.zero_len += 1;
            }
            let pair = (is_default_styled(span), span.style);
            if let Some(p) = prev {
                if p.1 == span.style {
                    s.adj_same += 1;
                    if p.0 {
                        s.adj_same_default += 1;
                    } else {
                        s.adj_same_styled += 1;
                    }
                }
            }
            prev = Some(pair);
        }
        // the coalesced counts: one span per maximal same-style run
        // (any-style merge) and one per maximal default-styled run (the
        // merge that is byte-identical on every downstream byte path).
        let mut runs = 0usize;
        let mut run_style: Option<ratatui::style::Style> = None;
        let mut default_runs = 0usize;
        let mut in_default_run = false;
        for span in line {
            if span.content.is_empty() {
                continue;
            }
            if run_style != Some(span.style) {
                runs += 1;
                run_style = Some(span.style);
            }
            let is_default = is_default_styled(span);
            if is_default && !in_default_run {
                default_runs += 1;
            }
            in_default_run = is_default;
        }
        s.coalesced_spans += runs;
        s.coalesced_spans_default += default_runs;
    }
    s
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
        } => summary.len() + custom_instructions.as_ref().map(|s| s.len()).unwrap_or(0),
        ChatEntry::Assistant(message) => message
            .blocks
            .iter()
            .map(|block| match block {
                crate::chat::MessageBlock::Thinking(text)
                | crate::chat::MessageBlock::Text(text) => 16 + text.len(),
            })
            .sum::<usize>()
            + message.error.as_ref().map(|s| s.len()).unwrap_or(0),
        ChatEntry::Tool(card) => {
            16 + card.id.len() + card.name.len() + value_bytes(&card.args)
                + card
                    .result
                    .as_ref()
                    .map(|result| {
                        16 + result.content.iter().map(value_bytes).sum::<usize>()
                            + value_bytes(&result.details)
                    })
                    .unwrap_or(0)
        }
        other => format!("{other:?}").len(),
    }
}

impl AgentView {
    pub(crate) fn census(&self) -> serde_json::Value {
        let mut kinds = std::collections::BTreeMap::new();
        let mut content_bytes = 0usize;
        for entry in &self.chat {
            *kinds.entry(entry_kind(entry)).or_insert(0usize) += 1;
            content_bytes += entry_content_bytes(entry);
        }

        let mut heights_some = [0usize; 3];
        for slots in &self.entry_heights {
            for (detail, slot) in slots.iter().enumerate() {
                if slot.is_some() {
                    heights_some[detail] += 1;
                }
            }
        }
        let mut layout_by_detail = [0usize; 3];
        let mut per_detail: Vec<SpanStats> = vec![SpanStats::default(); 3];
        let mut per_kind: std::collections::BTreeMap<&'static str, SpanStats> =
            std::collections::BTreeMap::new();
        let mut largest_layout: Vec<(usize, usize, usize)> = Vec::new();
        let mut layout_no_height = [0usize; 3];
        let mut height_no_layout = [0usize; 3];
        let mut samples: Vec<serde_json::Value> = Vec::new();
        let mut layout_slots = 0usize;
        for (index, slots) in self.entry_layout.iter().enumerate() {
            let kind = self.chat.get(index).map(entry_kind).unwrap_or("?");
            for (detail, slot) in slots.iter().enumerate() {
                let Some(layout) = slot else {
                    if self.entry_heights.get(index).and_then(|h| h.get(detail)).is_some() {
                        height_no_layout[detail] += 1;
                    }
                    continue;
                };
                layout_slots += 1;
                layout_by_detail[detail] += 1;
                if self.entry_heights.get(index).and_then(|h| h.get(detail)).is_none() {
                    layout_no_height[detail] += 1;
                }
                let s = line_stats(&layout.rows);
                let d = &mut per_detail[detail];
                d.lines += s.lines;
                d.nonempty_lines += s.nonempty_lines;
                d.spans += s.spans;
                d.content_len += s.content_len;
                d.content_cap += s.content_cap;
                d.line_cap_slots += s.line_cap_slots;
                d.rows_cap += s.rows_cap;
                d.zero_len += s.zero_len;
                d.adj_same += s.adj_same;
                d.adj_same_default += s.adj_same_default;
                d.adj_same_styled += s.adj_same_styled;
                d.coalesced_spans += s.coalesced_spans;
                d.coalesced_spans_default += s.coalesced_spans_default;
                let k = per_kind.entry(kind).or_default();
                k.lines += s.lines;
                k.nonempty_lines += s.nonempty_lines;
                k.spans += s.spans;
                k.content_len += s.content_len;
                k.content_cap += s.content_cap;
                k.line_cap_slots += s.line_cap_slots;
                k.rows_cap += s.rows_cap;
                k.zero_len += s.zero_len;
                k.adj_same += s.adj_same;
                k.adj_same_default += s.adj_same_default;
                k.adj_same_styled += s.adj_same_styled;
                k.coalesced_spans += s.coalesced_spans;
                k.coalesced_spans_default += s.coalesced_spans_default;
                largest_layout.push((index, detail, s.content_len));
                for line in layout.rows.iter() {
                    if line.len() >= 10 && samples.len() < 12 {
                        let mut styles = std::collections::BTreeSet::new();
                        for span in line {
                            styles.insert(format!("{:?}", span.style));
                        }
                        samples.push(json!({
                            "kind": kind,
                            "spans": line.len(),
                            "distinct_styles": styles.len(),
                            "content_len": line.iter().map(|sp| sp.content.len()).sum::<usize>(),
                            "styles": styles.into_iter().take(12).collect::<Vec<_>>(),
                        }));
                    }
                }
            }
        }
        largest_layout.sort_by_key(|(_, _, bytes)| usize::MAX - *bytes);

        let md = self.md_caches.borrow();
        let mut md_cache_entries = 0usize;
        let mut md_blocks = 0usize;
        let mut md_key_bytes = 0usize;
        let mut md_stats = SpanStats::default();
        for (_index, cache) in md.iter() {
            md_cache_entries += 1;
            for (key, key_lines) in cache.iter() {
                md_blocks += 1;
                md_key_bytes += key.len();
                let s = line_stats(key_lines);
                md_stats.lines += s.lines;
                md_stats.spans += s.spans;
                md_stats.content_len += s.content_len;
                md_stats.content_cap += s.content_cap;
                md_stats.line_cap_slots += s.line_cap_slots;
                md_stats.coalesced_spans += s.coalesced_spans;
            }
        }

        let flushed_rows = self.flushed_frame.len();
        let flushed_bytes = self.flushed_frame.iter().map(|s| s.len()).sum::<usize>();
        let osc_rows = self.osc_last_rows.len();
        let osc_bytes = self.osc_last_rows.values().map(|s| s.len()).sum::<usize>();

        let kind_json: Vec<serde_json::Value> = per_kind
            .iter()
            .map(|(kind, s)| {
                json!({
                    "kind": kind,
                    "lines": s.lines,
                    "spans": s.spans,
                    "coalesced_spans": s.coalesced_spans,
                    "content_len": s.content_len,
                    "content_cap": s.content_cap,
                    "line_cap_slots": s.line_cap_slots,
                    "adj_same": s.adj_same,
                    "adj_same_default": s.adj_same_default,
                    "adj_same_styled": s.adj_same_styled,
                    "coalesced_spans": s.coalesced_spans,
                    "coalesced_spans_default_only": s.coalesced_spans_default,
                    "zero_len": s.zero_len,
                })
            })
            .collect();
        let detail_json: Vec<serde_json::Value> = (0..3)
            .map(|d| {
                let s = &per_detail[d];
                json!({
                    "slots": layout_by_detail[d],
                    "lines": s.lines,
                    "spans": s.spans,
                    "coalesced_spans": s.coalesced_spans,
                    "content_len": s.content_len,
                    "content_cap": s.content_cap,
                    "rows_cap": s.rows_cap,
                })
            })
            .collect();

        let layout = &per_detail[0];
        let mut total = SpanStats::default();
        for s in per_detail.iter() {
            total.lines += s.lines;
            total.nonempty_lines += s.nonempty_lines;
            total.spans += s.spans;
            total.content_len += s.content_len;
            total.content_cap += s.content_cap;
            total.line_cap_slots += s.line_cap_slots;
            total.rows_cap += s.rows_cap;
            total.zero_len += s.zero_len;
            total.adj_same += s.adj_same;
            total.adj_same_default += s.adj_same_default;
            total.adj_same_styled += s.adj_same_styled;
            total.coalesced_spans += s.coalesced_spans;
            total.coalesced_spans_default += s.coalesced_spans_default;
        }
        let _ = layout;

        json!({
            "ts": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(0),
            "sizes": {
                "span": std::mem::size_of::<crate::Span>(),
                "line": std::mem::size_of::<crate::Line>(),
                "style": std::mem::size_of::<ratatui::style::Style>(),
            },
            "chat_entries": self.chat.len(),
            "chat_kinds": kinds,
            "chat_content_bytes": content_bytes,
            "layout_slots": layout_slots,
            "layout_slots_by_detail": layout_by_detail,
            "layout_lines": total.lines,
            "layout_nonempty_lines": total.nonempty_lines,
            "layout_spans": total.spans,
            "layout_content_len_bytes": total.content_len,
            "layout_content_cap_bytes": total.content_cap,
            "layout_content_slack_bytes": total.content_cap.saturating_sub(total.content_len),
            "layout_line_cap_slots": total.line_cap_slots,
            "layout_line_cap_slack_slots": total.line_cap_slots.saturating_sub(total.spans),
            "layout_rows_cap": total.rows_cap,
            "layout_zero_len_spans": total.zero_len,
            "layout_adj_same_style_pairs": total.adj_same,
            "layout_adj_same_default_pairs": total.adj_same_default,
            "layout_adj_same_styled_pairs": total.adj_same_styled,
            "layout_coalesced_spans": total.coalesced_spans,
            "layout_coalesced_spans_default_only": total.coalesced_spans_default,
            "layout_per_detail": detail_json,
            "layout_per_kind": kind_json,
            "layout_top10_by_content": largest_layout.iter().take(10)
                .map(|(i, d, b)| json!({"entry": i, "detail": d, "content_bytes": b}))
                .collect::<Vec<_>>(),
            "layout_fragment_samples": samples,
            "layout_no_height": layout_no_height,
            "height_no_layout": height_no_layout,
            "heights_some_by_detail": heights_some,
            "md_cache_entries": md_cache_entries,
            "md_blocks": md_blocks,
            "md_key_bytes": md_key_bytes,
            "md_lines": md_stats.lines,
            "md_spans": md_stats.spans,
            "md_content_len_bytes": md_stats.content_len,
            "md_content_cap_bytes": md_stats.content_cap,
            "md_coalesced_spans": md_stats.coalesced_spans,
            "sparse_entries": self.sparse_entries.len(),
            "sparse_window": self.sparse_window.is_some(),
            "layout_width": self.layout_width,
            "flushed_frame": {"rows": flushed_rows, "bytes": flushed_bytes},
            "osc_last_rows": {"rows": osc_rows, "bytes": osc_bytes},
        })
    }
}

/// The TUI process's own memory map summary (probe): total RSS, the
/// [heap] mapping, and every anonymous mapping >= 128KB (glibc mmaps
/// large Vec/String allocations directly; they never live in [heap]).
fn self_smaps_summary() -> serde_json::Value {
    let mut rss_kb = 0usize;
    let mut heap_kb = 0usize;
    let mut small_anon_kb = 0usize;
    let mut big_anon: Vec<serde_json::Value> = Vec::new();
    let Ok(text) = std::fs::read_to_string("/proc/self/smaps") else {
        return serde_json::Value::Null;
    };
    let mut in_heap = false;
    let mut is_anon = false;
    let mut map_kb = 0usize;
    let mut map_label = String::new();
    for line in text.lines() {
        let head = line.split_whitespace().collect::<Vec<_>>();
        if head.len() >= 5 && head[0].contains('-') && !head[0].contains(':') {
            if is_anon && map_kb > 0 {
                if map_kb >= 128 {
                    big_anon.push(serde_json::json!({"kb": map_kb, "label": map_label}));
                } else {
                    small_anon_kb += map_kb;
                }
            }
            let path_part = if head.len() > 5 { head[5..].join(" ") } else { String::new() };
            in_heap = path_part.contains("[heap]");
            is_anon = path_part.is_empty() && head[1].contains('p');
            map_kb = 0;
            map_label = head[0].to_string();
        } else if line.starts_with("Rss:") {
            let kb: usize = head[1].parse().unwrap_or(0);
            rss_kb += kb;
            map_kb += kb;
            if in_heap {
                heap_kb += kb;
            }
        }
    }
    if is_anon && map_kb > 0 {
        if map_kb >= 128 {
            big_anon.push(serde_json::json!({"kb": map_kb, "label": map_label}));
        } else {
            small_anon_kb += map_kb;
        }
    }
    serde_json::json!({
        "self_rss_kb": rss_kb,
        "self_heap_kb": heap_kb,
        "self_small_anon_kb": small_anon_kb,
        "self_big_anon_kb": big_anon.iter().map(|m| m["kb"].as_u64().unwrap_or(0)).sum::<u64>(),
        "self_big_anon": big_anon,
    })
}

/// Run the census if a probe request is pending (draw-path, main thread).
/// `<PA_TUI_CENSUS_FILE>.kick` = plain census;
/// `<PA_TUI_CENSUS_FILE>.kick.trim` = trim freed heap first (slack probe).
pub fn maybe_census(view: &AgentView) {
    let Ok(path) = std::env::var("PA_TUI_CENSUS_FILE") else {
        return;
    };
    let plain = format!("{path}.kick");
    let trim = format!("{path}.kick.trim");
    let mode = if std::path::Path::new(&trim).exists() {
        let _ = std::fs::remove_file(&trim);
        pa_types::memory_release::trim_freed_heap();
        Some(true)
    } else if std::path::Path::new(&plain).exists() {
        let _ = std::fs::remove_file(&plain);
        Some(false)
    } else {
        None
    };
    let Some(trimmed) = mode else {
        return;
    };
    let mut census = view.census();
    if let serde_json::Value::Object(map) = &mut census {
        map.insert("trimmed".into(), serde_json::json!(trimmed));
        map.insert("smaps".into(), self_smaps_summary());
    }
    let line = format!("{census}\n");
    let _ = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .and_then(|mut file| file.write_all(line.as_bytes()));
}
