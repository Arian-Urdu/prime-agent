# w4-docs-2 — computer-use doc lane (SKILL.md + api.md)

Lane: doc updates for the computer-use skill after Kevin's live test with GLM
(non-vision) and the four fixes that landed in this worktree
(`activate()`, post-action settle, window-server bounds for Electron
windows, `PRIME_AGENT_CODING_AGENT_DIR` path derivation).
Branch: `skill/computer-use-core` (worktree `~/pi/.worktrees/cua-skill-core`).
Only owned doc files touched: `SKILL.md`, `references/api.md`.
`references/safety.md` unchanged. No source, tests, or integration files
touched.

## SKILL.md

- **Focus and keyboard shortcuts** (new section, after Screenshots /
  Non-vision models, before Typing hazards): keystrokes (`type_text`,
  `press_key`, `paste`) always reach the bound app's process because they are
  posted per-pid, but app-scoped shortcuts (menus, quick switchers, composer
  keys) only fire while the app is key — call `await app.activate()` before
  shortcut-driven flows; `activate()` is the only supported way to take
  focus and bash workarounds (`open -a`, osascript, `screencapture`) are
  forbidden (the live-test failure mode). Second bullet: after
  reload-triggering or slow actions, a loading indicator is not a failure —
  re-observe a few more times with `get_ax_state()` until it clears (bounded
  patience), then judge the flow.
- **Non-vision models** (new section, ~10 lines as briefed): when the model
  cannot see images, AX text and its diff announcements stay the primary
  source; `get_text_regions()` replaces the screenshot path — window-scoped
  OCR, normalized region list, window-relative pixel coordinates in the
  same space as screenshot pixel targets (regions clickable directly); use
  it wherever a vision model would take a screenshot; never improvise
  full-screen `screencapture` or external OCR scripts — observation stays
  window-scoped through the API.
- **AX-first loop bullet** amended for consistency: a loading indicator is
  the exception to "the next `get_ax_state()` shows the settled state" —
  re-observe until it clears (points at the new section). Full action
  surface list now includes `activate`.
- **Allowlist bullet**: `~/.prime/agent` is called out as the default agent
  dir with `PRIME_AGENT_CODING_AGENT_DIR` overriding it.

## references/api.md

- **App table**: `await get_text_regions()` row — window-scoped OCR over the
  focused window, normalized list of text regions, window-relative pixel
  coordinates (same space as screenshot `(x, y)` targets, clickable),
  screen-reading path for non-vision models, also reads image/canvas text
  the AX tree cannot expose. `await activate()` row — foregrounds the
  frontmost window and makes the app key; keystrokes always reach the
  process but app-scoped shortcuts only fire while key; never fake focus
  from bash with `open -a`, osascript, or `screencapture`.
- **After the every-action-errors paragraph**: web views (Electron apps)
  often omit per-element geometry; element clicks there raise
  `ACTION_UNSUPPORTED` — switch to keyboard navigation or `(x, y)`
  window-screenshot coordinates (`get_text_regions()` supplies them for
  non-vision models).
- **Policy files lead-in**: settings, approvals, and screenshot tmp files
  all derive from the agent state dir — `~/.prime/agent` default,
  `PRIME_AGENT_CODING_AGENT_DIR` env override when set.
- **Telemetry**: `activate` added to the documented `computer_use_action`
  action list (the code emits it via `_action("activate", ...)`).

## Decisions and assumptions

- **safety.md unchanged**: its scope line already binds "any other way of
  driving a UI", and the new prohibitions (no bash focus workarounds, no
  full-screen screencapture, no external OCR) are operational correctness
  rules that live in SKILL.md/api.md where the flow reads them. Say the
  word if you want them mirrored there.
- **"announcements"** (parent brief: "the AX text + announcements remain
  the primary source") was interpreted as the `get_ax_state` diff
  announcements — what changed after each action. Phrased as "the AX text
  and its diff announcements stay the primary source" in the Non-vision
  section.
- **get_text_regions is documented from the brief**, not from code — the
  stacked ocr lane has not landed the implementation yet (grep: zero hits
  in the worktree). Documented as a no-arg call returning a normalized
  region list with window-relative pixel coords. If the landing
  implementation uses different dict keys or takes parameters, the two
  places to reconcile are the api.md App-table row and the SKILL.md
  Non-vision bullet. Its telemetry action (if any) was deliberately not
  added to the api.md action list — the ocr lane owns that.
- The Electron window-bounds fix (`_window_server_rect`) is code-only; the
  docs describe the user-visible symptom (web views omit per-element
  geometry → `ACTION_UNSUPPORTED` on element clicks) and the recovery, per
  the parent's second message.
- No Screen-Recording-grant claim was made for `get_text_regions` — the ocr
  lane's grant behavior is not visible yet; if it captures like
  `get_screenshot` it will want a permissions.md line (not my file).

## Verification

- Frontmatter description byte-identical: sha256 of the `description:` line
  before and after = `c46458cac6483f986ae6a833218cc13f18ff32e7200acdbb8858e1d049295c78`.
- `git diff` reviewed for both files; only the sections above changed.
- No tests read the doc files (grep of tests/ for SKILL.md/api.md/safety.md:
  no hits), so no test gate is affected by the doc lane.
- Cross-references match headings: "Focus and keyboard shortcuts",
  "Non-vision models".

## Addendum: background-first posture (second parent instruction round)

Src landed `open -g` launches and `App.is_frontmost()`; docs updated to
match (SKILL.md + api.md only).

- **SKILL.md — "Focus and keyboard shortcuts" rewritten to lead with the
  background posture**: launches are hidden (`open -g`) and AX actions,
  `set_value`, `select_text`, and observation all work in the background.
  `activate()` is now framed as the explicit, user-visible takeover
  reserved for shortcut flows (Electron menus, quick switchers, composer
  keys): check `app.is_frontmost()` first and skip it when already key,
  and announce the takeover before calling it ("I'm bringing Slack to the
  foreground to use its shortcuts"). The no-bash-workarounds bullet and
  the loading-indicator bullet are unchanged.
- **api.md — `get_app` paragraph**: binding a not-running app launches it
  in the background (`open -g`), binds and observes without taking over
  the screen; only an explicit `App.activate()` brings it forward; failed
  start still raises `APP_LAUNCH_FAILED`.
- **api.md — App table**: new `is_frontmost()` row — sync (no `await`),
  pre-flight check before shortcut flows; `True` means shortcuts fire
  without `activate()`. The `activate()` row is unchanged (announce
  guidance lives in SKILL.md).
- Telemetry unchanged in this round: `is_frontmost()` is a plain query in
  code (not routed through `_action`), so no action event and no list
  change. Frontmatter description still byte-identical (sha256
  `c46458cac6483f986ae6a833218cc13f18ff32e7200acdbb8858e1d049295c78`).
