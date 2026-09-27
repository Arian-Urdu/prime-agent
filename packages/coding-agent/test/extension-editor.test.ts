import type { TUI } from "@earendil-works/pi-tui";
import { afterEach, expect, it, vi } from "vitest";
import { KeybindingsManager } from "../src/core/keybindings.js";
import { ExtensionEditorComponent } from "../src/modes/interactive/components/extension-editor.js";
import { initTheme } from "../src/modes/interactive/theme/theme.js";

afterEach(() => vi.unstubAllEnvs());

it("restores fullscreen after the external editor returns", () => {
	initTheme("dark");
	vi.stubEnv("VISUAL", "true"); // exits 0 without editing
	const noop = () => {};
	const calls: string[] = [];
	const record = (name: string) => () => calls.push(name);
	const tui = { start: record("start"), stop: record("stop"), requestRender: record("render") } as unknown as TUI;
	const keys = new KeybindingsManager();
	const fullscreen = record("fullscreen");
	const editor = new ExtensionEditorComponent(tui, keys, "Title", "", noop, noop, undefined, fullscreen);
	editor.handleInput("\x07"); // ctrl+g = app.editor.external
	expect(calls.slice(-3)).toEqual(["start", "fullscreen", "render"]);
});
