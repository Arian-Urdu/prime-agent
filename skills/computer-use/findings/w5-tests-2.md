# w5 test lane, batch 2 — the four core fixes

Worktree `~/pi/.worktrees/cua-skill-core`, skill `skills/computer-use/`. Test-only
lane: no `src/` file was touched. Changes: new `tests/test_w5_core.py` (20 tests)
plus a minimal `tests/fakes.py` extension for the `_activate` seam.

## How to run (everything -B)

```sh
cd skills/computer-use/tests && python3 -B -m unittest discover
# or from the skill root:
cd skills/computer-use && python3 -B -m unittest discover -s tests
```

## Fix 1 — policy/capture paths derive from PRIME_AGENT_CODING_AGENT_DIR

`AgentDirTests` (policy exposes `_agent_dir()`):
- `test_agent_dir_reads_the_env_override`
- `test_agent_dir_expands_a_tilde_override`
- `test_agent_dir_falls_back_to_prime_agent_home`

`PolicyPathTests` (module reload with patched env):
- `test_paths_derive_from_the_env_override_on_load` — reload with the env var set:
  `SETTINGS_PATH = <dir>/settings/computer-use.toml`, `STATE_DIR = <dir>/state/computer-use`
- `test_paths_fall_back_to_prime_agent_home_without_override` — reload with the var popped:
  both paths under `~/.prime/agent`

`ScreenshotsDirTests` (recompute at call time + reload for the module constant):
- `test_screenshots_dir_reads_the_env_override` — `<dir>/tmp/computer-use`
- `test_screenshots_dir_falls_back_to_prime_agent_home`
- `test_module_dir_constant_derives_from_the_env_override_on_load` — `capture._SCREENSHOTS_DIR`
  reloads from the env too; a `finally` reload restores the ambient state for later tests.

Note: this host's ambient env has `PRIME_AGENT_CODING_AGENT_DIR=/Users/kevin/.prime/agent`
set, so the fallback tests pop it explicitly inside `mock.patch.dict(os.environ)`.

## Fix 2 — ax._window_server_rect(window_id)

`WindowServerRectTests` fakes the quartz seam (`_require_mac` -> `.quartz` with a
`CGWindowListCopyWindowInfo` returning canned info dicts):
- `test_valid_bounds_return_as_a_float_rect` — `(100.5, 50.25, 400.0, 300.0)` all floats;
  also pins the call: `options == kCGWindowListOptionOnScreenOnly | kCGWindowListExcludeDesktopElements`,
  window_id passthrough
- `test_partial_bounds_default_missing_keys_to_zero` — `(12.0, 0.0, 0.0, 0.0)`
- `test_none_window_id_returns_none_without_touching_quartz` — None id never calls the seam
- `test_entries_without_bounds_are_skipped_for_a_later_one` — no-key and None bounds are skipped
- `test_no_bounds_in_any_entry_returns_none`
- `test_framework_exception_returns_none` — the quartz call raises
- `test_missing_framework_returns_none` — `_require_mac` raises TRANSPORT_ERROR (off-darwin path)

## Fix 3 — App.activate() dispatches apps._activate(pid)

`fakes.py`: `RecordingBackend._activate(pid)` records `("activate", {"pid": pid})`;
`AppEnvironment._activate` mirrors the real seam (absent pid -> `APP_NOT_RUNNING`,
present -> records through the recorder) and is bound onto `apps._activate` in
`__enter__`; `APPS_SEAMS = ("_activate",)` extends the `recording_backend()` CM.

`AppActivateTests`:
- `test_activate_dispatches_apps_activate_with_the_bound_pid` — recorder has
  `{"pid": <bound pid>}`; telemetry `computer_use_action` ok for action "activate"
- `test_activate_vanished_pid_raises_app_not_running_without_dispatch` — app quits ->
  APP_NOT_RUNNING, no dispatch recorded
- `test_fake_activate_seam_fails_closed_for_an_absent_pid` — pins the fake's own contract

## Fix 4 — _ACTION_SETTLE_SECONDS post-action settle

`ActionSettleTests` (timing on the fake backend):
- `test_one_action_settles_after_the_dispatch` — one `press_key` action:
  dispatch recorded, elapsed >= 0.10 (loose bound under the ~0.12s settle)
- `test_failed_action_does_not_settle` — a dispatch-level failure (stale index)
  completes under 0.10, pinning that the settle only follows a successful dispatch

## Suite tail (full run, -B, from tests/)

```
$ python3 -B -m unittest discover
...
Ran 230 tests in 3.313s

OK (skipped=5)

```

Baseline before this lane was 210 tests OK (skipped=5); the 20 new tests keep the
suite green (the 5 skips are the opt-in `PRIME_CUA_LIVE=1` smokes). Two consecutive
full runs plus five repeats of the timing class all passed; no `__pycache__` was
written (`-B` everywhere).

## Addendum — two later src changes (same lane, still test-only)

### Fix 5 — _open_command launches with -g

- `OpenCommandTests.test_open_command_carries_g_for_every_spec_kind` — full argv
  for all five spec shapes: `"com.example.app"` -> `["open", "-g", "-b", ...]`,
  `"Slack"` -> `["open", "-g", "-a", ...]`, and the bundle_id/name/path dict specs
  -> `["open", "-g", "-b", ...]` / `["open", "-g", "-a", ...]` / `["open", "-g", <path>]`.
  No existing test asserted the old argv, so nothing needed updating.

### Fix 6 — App.is_frontmost() via apps._frontmost_pid()

`fakes.py`: `AppEnvironment.frontmost` (int | None, default None) +
`AppEnvironment._frontmost_pid()` returning it, patched onto `apps._frontmost_pid`
in `__enter__` (strict: a missing src seam raises at enter).

- `AppFrontmostTests.test_is_frontmost_true_when_the_bound_pid_is_frontmost`
- `AppFrontmostTests.test_is_frontmost_false_when_another_pid_is_frontmost`
  (covers both another pid and None)

### Suite tail after the addendum (full run, -B, from tests/)

```
$ python3 -B -m unittest discover
...
Ran 233 tests in 3.315s

OK (skipped=5)

```
