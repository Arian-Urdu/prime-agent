//! PROBE-ONLY echo-path timing marks. NEVER SHIPS: this module exists only
//! on probe branches (PROBE-labeled builds, never a product baseline).
//! Gated by PA_TUI_ECHO_TRACE=<path>: absent the env var every call is a
//! single atomic pointer load and the product behavior is untouched; set,
//! each mark appends one JSON line carrying a CLOCK_REALTIME microsecond
//! stamp (joinable with the driver's own send/echo stamps) and a
//! monotonic-relative stamp (safe in-process segment deltas) plus an
//! optional key character so a keystroke's marks pair across threads.
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::{Mutex, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

static TRACE: OnceLock<Option<Mutex<std::fs::File>>> = OnceLock::new();
static BASE: OnceLock<Instant> = OnceLock::new();

fn open() -> Option<Mutex<std::fs::File>> {
    let path = std::env::var("PA_TUI_ECHO_TRACE").ok()?;
    let file = OpenOptions::new().create(true).append(true).open(&path).ok()?;
    let _ = BASE.set(Instant::now());
    Some(Mutex::new(file))
}

/// One timing mark. `key` is the typed character when the stage belongs to
/// a keystroke (pairing anchor); `None` for structural stages.
pub fn mark(stage: &str, key: Option<char>) {
    let Some(file) = TRACE.get_or_init(open) else {
        return;
    };
    let real = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let mono = BASE.get().map(|base| base.elapsed()).unwrap_or_default();
    let line = match key {
        Some(k) => format!(
            "{{\"t_us\":{},\"m_us\":{},\"ev\":\"{stage}\",\"k\":\"{k}\"}}\n",
            real.as_micros(),
            mono.as_micros()
        ),
        None => format!(
            "{{\"t_us\":{},\"m_us\":{},\"ev\":\"{stage}\"}}\n",
            real.as_micros(),
            mono.as_micros()
        ),
    };
    if let Ok(mut file) = file.lock() {
        let _ = file.write_all(line.as_bytes());
    }
}

/// The keystroke character for a crossterm key event, when it is a plain
/// printable press (the probe pairs typing-token keys only).
pub fn plain_char(event: &crossterm::event::Event) -> Option<char> {
    match event {
        crossterm::event::Event::Key(key) => match key.code {
            crossterm::event::KeyCode::Char(c) => Some(c),
            _ => None,
        },
        _ => None,
    }
}

/// The character of a forwarded key event (the post-filter pairing anchor).
pub fn key_char(key: &crossterm::event::KeyEvent) -> Option<char> {
    match key.code {
        crossterm::event::KeyCode::Char(c) => Some(c),
        _ => None,
    }
}
