#!/usr/bin/env node
/**
 * Factory DAG capability evaluation — PR H of the factory DAG feature set.
 * Spec: "Factory DAGs: declarative orchestration in Continual Harness"
 * https://app.notion.com/p/3da72940136f81a88554e6ec7119270e — section "Proposed evaluation".
 *
 * Runs the capability layer against REAL sessions with live models:
 *   - three reference factories (review-sweep, n-wide-builder, resident-watcher), each
 *     paired with a hand-written manual-orchestration baseline (rlm.spawn + rlm.collect)
 *     for the same topology, inputs, model, and declared budget;
 *   - one escalation trial of review-sweep with a planted failing reviewer (the declared
 *     escalate policy must pause the run and start nothing further);
 *   - one dry-run rejection trial (a broken spec must make rlm.factory.run raise and
 *     start no node).
 *
 * Plus the deterministic replay check: --replay <report.json|ledger.json> re-verifies
 * saved run ledgers for stable event identities and complete resource accounting.
 *
 * This script spends real model tokens. It NEVER runs in CI; the deterministic pieces
 * (spec shapes, prompt builders, answer checkers, replay checker, report renderer) are
 * unit-tested in test/factory-eval.test.ts. The live run is the reviewer's call.
 *
 * Usage:
 *   npx tsx scripts/factory-eval.ts \
 *     --model internal/glm-5.2-fast --factories review-sweep,builder,resident-watcher \
 *     --width 6 --trials 1 --out ./factory-dag-eval-reports
 *   npx tsx scripts/factory-eval.ts --replay ./factory-dag-eval-reports/report.json
 */

import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { AuthStorage } from "../src/core/auth-storage.js";
import { calculateContextTokens } from "../src/core/compaction/compaction.js";
import { FACTORY_PROGRESS_NOTICE_CUSTOM_TYPE } from "../src/core/messages.js";
import { ModelRegistry } from "../src/core/model-registry.js";
import { getSessionArtifactPath, SessionManager } from "../src/core/session-manager.js";
import { SettingsManager } from "../src/core/settings-manager.js";
import { createAgentSession } from "../src/core/sdk.js";
import { getAgentDir } from "../src/config.js";

// ---------------------------------------------------------------------------
// Spec types (mirror the kernel-side factory dag schema; the kernel validates on
// write at run time, so the TS side only builds and shapes-checks them).
// ---------------------------------------------------------------------------

export type PortType = "text" | "json";

export interface FactoryDagPort {
	name: string;
	type: PortType;
	from?: string;
}

export interface FactoryDagOutputPort {
	name: string;
	type: PortType;
}

export interface FactoryDagForeach {
	over: string;
	max: number;
}

export type FailurePolicy = "fail_fast" | "continue" | "escalate";

export interface InlineSubagent {
	prompt: string;
	name?: string;
	model?: string;
	thinking?: string;
}

export interface FactoryDagNodeSpec {
	id: string;
	subagent: string | InlineSubagent;
	lifecycle?: "task" | "resident";
	depends_on?: string[];
	inputs?: FactoryDagPort[];
	outputs?: FactoryDagOutputPort[];
	budget_ms?: number;
	retries?: number;
	failure_policy?: FailurePolicy;
	foreach?: FactoryDagForeach;
}

export interface FactoryDagRunSpec {
	budget_ms?: number;
	failure_policy?: FailurePolicy;
	max_parallel?: number;
}

export interface FactoryDagSpec {
	run?: FactoryDagRunSpec;
	nodes: FactoryDagNodeSpec[];
}

export type ReferenceFactoryKind =
	| "review-sweep"
	| "builder"
	| "resident-watcher"
	| "review-sweep-fail"
	| "dry-run-reject";

export interface ReferenceFactory {
	id: string;
	kind: ReferenceFactoryKind;
	title: string;
	description: string;
	dag: FactoryDagSpec;
	declaredBudgetMs: number;
	declaredFanIn: number;
	width?: number;
}

// ---------------------------------------------------------------------------
// Constants shared by the factories, their baselines, and the checkers.
// ---------------------------------------------------------------------------

/** Per-node wall-clock budget (admission to settlement) declared on every task node. */
export const NODE_BUDGET_MS = 240_000;
/** Whole-run wall-clock budget declared on every reference factory. */
export const RUN_BUDGET_MS = 900_000;
export const REVIEW_FOREACH_MAX = 8;
export const DEFAULT_WIDTH = 6;
export const MAX_WIDTH = 12;
export const DEFAULT_MODEL = "internal/glm-5.2-fast";
export const RESIDENT_WATCHER_SLEEP_SECONDS = 900;

export interface ReviewFile {
	name: string;
	code: string;
	issueId: string;
	audit: string;
}

/** Four review files, each with one real, checkable planted defect and its audit id. */
export const REVIEW_FILES: ReviewFile[] = [
	{
		name: "fa",
		code: "export function clampUpper(value, max) {\n\treturn Math.max(value, max);\n}",
		issueId: "AUDIT-A1",
		audit: "callers expect the value capped at max, but Math.max returns the larger operand, so values above max pass through unclamped",
	},
	{
		name: "fb",
		code: "export function medianSorted(values) {\n\treturn values[Math.floor(values.length / 2)];\n}",
		issueId: "AUDIT-B1",
		audit: "the median of an even-length sorted list is the average of the two middle values, but this returns only the upper middle",
	},
	{
		name: "fc",
		code: 'export function isBlank(text) {\n\treturn text === "";\n}',
		issueId: "AUDIT-C1",
		audit: "whitespace-only strings should count as blank, but the strict equality comparison misses them",
	},
	{
		name: "fd",
		code: "const RETRY_DELAY_SECONDS = 30;\nexport const RETRY_DELAY_MS = RETRY_DELAY_SECONDS;",
		issueId: "AUDIT-D1",
		audit: "RETRY_DELAY_MS is documented as milliseconds but the constant was meant as seconds; the unit conversion is missing",
	},
];

export const REVIEW_FILE_NAMES = REVIEW_FILES.map((file) => file.name);
export const REVIEW_ISSUE_IDS = REVIEW_FILES.map((file) => file.issueId);

export const BUILDER_MARKER = (index: number) => `swb-marker-${index}`;
export const TASK_MARKER_A = "swt-1";
export const TASK_MARKER_B = "swt-2";
export const WATCHER_MARKER = "swr-marker";

function miniRepoListing(): string {
	return REVIEW_FILES.map((file) => `[${file.name}] ${file.code} // audit ${file.issueId}: ${file.audit}`).join("\n");
}

// ---------------------------------------------------------------------------
// Child prompt builders (shared verbatim between factory nodes and baselines).
// ---------------------------------------------------------------------------

export function buildFilesNodePrompt(): string {
	const json = JSON.stringify({ files: REVIEW_FILE_NAMES });
	return [
		"You are the source node of a pull-request review sweep. Reply with exactly one fenced json block and nothing else:",
		"",
		"```json",
		json,
		"```",
	].join("\n");
}

/** Reviewer prompt for one file; `{files}` is the foreach placeholder. */
export function buildReviewerPromptTemplate(): string {
	return [
		"You are one code reviewer in a pull-request review sweep.",
		"",
		"Mini-repo under review (four files, one planted defect each, audit note included):",
		"",
		miniRepoListing(),
		"",
		"Your assigned file is {files}. Verify its defect is real, then reply with exactly one line:",
		"FOUND <the audit id of your assigned file>",
		"Output that single line and nothing else, then end your turn.",
	].join("\n");
}

/** Concrete per-file reviewer prompt (baseline arms render the same template). */
export function buildReviewerPromptFor(fileName: string): string {
	return buildReviewerPromptTemplate().replaceAll("{files}", fileName);
}

export function buildReportNodePrompt(): string {
	return [
		"You are the aggregation node of a pull-request review sweep.",
		"",
		"Files under review (json): {file_list}",
		"Reviewer reports (one FOUND line per file): {found}",
		"",
		"Every file has exactly one audit id. Cross-check that every reviewer report carries one, then reply with exactly one fenced json block and nothing else:",
		"",
		'```json\n{"issues": [<every audit id found, in file order>]}\n```',
	].join("\n");
}

export function buildBrokenReviewerPrompt(): string {
	return [
		"You are one code reviewer in a pull-request review sweep. Your assigned file is fa. Verify its defect, then reply with exactly one line:",
		"FOUND AUDIT-A1",
		"Output that single line and nothing else, then end your turn.",
	].join("\n");
}

export function buildBuilderNodePrompt(index: number): string {
	return [
		`You are builder node ${index} of a wide build. Reply with exactly one line:`,
		`BUILT ${BUILDER_MARKER(index)}`,
		"Output that single line and nothing else, then end your turn.",
	].join("\n");
}

export function buildCollectorPrompt(width: number): string {
	const lines = Array.from({ length: width }, (_, i) => `{line-${i + 1}}`).join("\n");
	const markers = Array.from({ length: width }, (_, i) => BUILDER_MARKER(i + 1)).join(" ");
	return [
		`You are the collector of a ${width}-wide build. One line per builder node arrived:`,
		"",
		lines,
		"",
		"Merge them. Reply with exactly one line:",
		`COLLECTED <every swb-marker from the lines above, space-separated, in ascending node order> (expected form: COLLECTED ${markers})`,
		"Output that single line and nothing else, then end your turn.",
	].join("\n");
}

export function buildWatcherPrompt(): string {
	return [
		"You are a resident watcher node attached to an orchestration run. Do exactly this, in order:",
		"",
		'1. In the ipython tool, send your parent one message with exactly this text:',
		`   await agent_message.send("WATCHER-UP ${WATCHER_MARKER}", receiver_role="parent")`,
		"2. Then, still in the ipython tool, run:",
		"   import asyncio",
		`   await asyncio.sleep(${RESIDENT_WATCHER_SLEEP_SECONDS})`,
		"   and stay idle. Do not end your turn before the sleep finishes. Do not send more messages. Do nothing else.",
	].join("\n");
}

export function buildTaskAPrompt(): string {
	return [
		"You are task node A of a tiny two-step chain. Reply with exactly one line:",
		`STEP ${TASK_MARKER_A}`,
		"Output that single line and nothing else, then end your turn.",
	].join("\n");
}

export function buildTaskBPromptTemplate(): string {
	return [
		"You are task node B of a tiny two-step chain. The previous step reported: {prev}",
		"Reply with exactly one line:",
		`STEP ${TASK_MARKER_B}`,
		"Output that single line and nothing else, then end your turn.",
	].join("\n");
}

// ---------------------------------------------------------------------------
// Reference factory spec builders.
// ---------------------------------------------------------------------------

export function buildReviewSweepDag(): FactoryDagSpec {
	return {
		run: { budget_ms: RUN_BUDGET_MS, failure_policy: "escalate", max_parallel: REVIEW_FOREACH_MAX },
		nodes: [
			{
				id: "files",
				subagent: { prompt: buildFilesNodePrompt(), name: "files-source" },
				outputs: [{ name: "files", type: "json" }],
				budget_ms: NODE_BUDGET_MS,
			},
			{
				id: "review",
				subagent: { prompt: buildReviewerPromptTemplate(), name: "file-reviewer" },
				inputs: [{ name: "files", type: "json", from: "files.files" }],
				outputs: [{ name: "found", type: "text" }],
				foreach: { over: "files", max: REVIEW_FOREACH_MAX },
				budget_ms: NODE_BUDGET_MS,
			},
			{
				id: "report",
				subagent: { prompt: buildReportNodePrompt(), name: "review-aggregator" },
				inputs: [
					{ name: "file_list", type: "json", from: "files.files" },
					{ name: "found", type: "text", from: "review.found" },
				],
				outputs: [{ name: "issues", type: "json" }],
				budget_ms: NODE_BUDGET_MS,
			},
		],
	};
}

/** review-sweep plus a planted failing reviewer: admission of an unresolvable model pin. */
export function buildReviewSweepFailDag(): FactoryDagSpec {
	const dag = buildReviewSweepDag();
	const nodes = [...dag.nodes];
	// Insert after the foreach so it starts once the sweep is under way.
	nodes.splice(2, 0, {
		id: "review-broken",
		subagent: { prompt: buildBrokenReviewerPrompt(), model: "internal/no-such-model-for-eval", name: "broken-reviewer" },
		depends_on: ["files"],
		budget_ms: NODE_BUDGET_MS,
		failure_policy: "escalate",
	});
	return { ...dag, nodes };
}

export function buildBuilderDag(width: number): FactoryDagSpec {
	const nodes: FactoryDagNodeSpec[] = Array.from({ length: width }, (_, i) => ({
		id: `builder-${i + 1}`,
		subagent: { prompt: buildBuilderNodePrompt(i + 1), name: `builder-${i + 1}` },
		outputs: [{ name: "line", type: "text" }],
		budget_ms: NODE_BUDGET_MS,
	}));
	nodes.push({
		id: "collector",
		subagent: { prompt: buildCollectorPrompt(width), name: "build-collector" },
		inputs: Array.from({ length: width }, (_, i) => ({
			name: `line-${i + 1}`,
			type: "text",
			from: `builder-${i + 1}.line`,
		})),
		outputs: [{ name: "merged", type: "text" }],
		budget_ms: NODE_BUDGET_MS,
	});
	return { run: { budget_ms: RUN_BUDGET_MS, failure_policy: "escalate", max_parallel: 8 }, nodes };
}

export function buildResidentWatcherDag(): FactoryDagSpec {
	return {
		run: { budget_ms: RUN_BUDGET_MS, failure_policy: "escalate", max_parallel: 8 },
		nodes: [
			{
				id: "watcher",
				subagent: { prompt: buildWatcherPrompt(), name: "resident-watcher" },
				lifecycle: "resident",
			},
			{
				id: "task-a",
				subagent: { prompt: buildTaskAPrompt(), name: "chain-step-a" },
				outputs: [{ name: "step", type: "text" }],
				budget_ms: NODE_BUDGET_MS,
			},
			{
				id: "task-b",
				subagent: { prompt: buildTaskBPromptTemplate(), name: "chain-step-b" },
				inputs: [{ name: "prev", type: "text", from: "task-a.step" }],
				outputs: [{ name: "step", type: "text" }],
				budget_ms: NODE_BUDGET_MS,
			},
		],
	};
}

/** Structurally valid but unresolvable: references a subagent entry that does not exist. */
export function buildBrokenDag(): FactoryDagSpec {
	return {
		run: { budget_ms: RUN_BUDGET_MS, failure_policy: "escalate", max_parallel: 8 },
		nodes: [{ id: "broken-source", subagent: "no-such-subagent-entry" }],
	};
}

export const FACTORY_ENTRY_IDS = {
	reviewSweep: "factory-dag-eval-review-sweep",
	reviewSweepFail: "factory-dag-eval-review-fail",
	builder: "factory-dag-eval-builder",
	residentWatcher: "factory-dag-eval-resident-watcher",
	broken: "factory-dag-eval-broken",
} as const;

export function buildReferenceFactories(width: number): ReferenceFactory[] {
	return [
		{
			id: FACTORY_ENTRY_IDS.reviewSweep,
			kind: "review-sweep",
			title: "review-sweep",
			description:
				"Reference factory: pull-request review sweep with typed fan-in and escalation (capability eval).",
			dag: buildReviewSweepDag(),
			declaredBudgetMs: RUN_BUDGET_MS,
			declaredFanIn: REVIEW_FILES.length,
		},
		{
			id: FACTORY_ENTRY_IDS.builder,
			kind: "builder",
			title: "builder",
			description: `Reference factory: ${width}-wide builder run with per-node budgets (capability eval).`,
			dag: buildBuilderDag(width),
			declaredBudgetMs: RUN_BUDGET_MS,
			declaredFanIn: width,
			width,
		},
		{
			id: FACTORY_ENTRY_IDS.residentWatcher,
			kind: "resident-watcher",
			title: "resident-watcher",
			description: "Reference factory: resident watcher that starts a bounded task DAG (capability eval).",
			dag: buildResidentWatcherDag(),
			declaredBudgetMs: RUN_BUDGET_MS,
			declaredFanIn: 1,
		},
		{
			id: FACTORY_ENTRY_IDS.reviewSweepFail,
			kind: "review-sweep-fail",
			title: "review-sweep-fail",
			description:
				"Escalation probe: review sweep with one planted failing reviewer; the declared escalate policy must pause the run.",
			dag: buildReviewSweepFailDag(),
			declaredBudgetMs: RUN_BUDGET_MS,
			declaredFanIn: REVIEW_FILES.length,
		},
		{
			id: FACTORY_ENTRY_IDS.broken,
			kind: "dry-run-reject",
			title: "broken",
			description: "Dry-run probe: structurally valid spec that references an unknown subagent.",
			dag: buildBrokenDag(),
			declaredBudgetMs: RUN_BUDGET_MS,
			declaredFanIn: 0,
		},
	];
}

export function findReferenceFactory(factories: ReferenceFactory[], kind: ReferenceFactoryKind): ReferenceFactory {
	const factory = factories.find((entry) => entry.kind === kind);
	if (!factory) throw new Error(`unknown reference factory kind ${kind}`);
	return factory;
}

// ---------------------------------------------------------------------------
// Harness-state seeding: write the harness_state.json the kernel will load
// (refinement.ts getLocalHarnessStateDir + agent-session.ts RLM_HARNESS_STATE_DIR).
// ---------------------------------------------------------------------------

export interface HarnessEntryJson {
	id: string;
	kind: "factory";
	title: string;
	content: string;
	path: string;
	scope: "local";
	reference: Record<string, unknown>;
	arguments: { dag: FactoryDagSpec };
	metadata: Record<string, unknown>;
	source: string;
	created_at: string;
	updated_at: string;
	version: number;
}

/** Full harness_state.json file body seeding the given factory entries. */
export function buildHarnessStateFile(specs: ReferenceFactory[], now = new Date()): string {
	const entries: Record<string, HarnessEntryJson> = {};
	for (const spec of specs) {
		entries[spec.id] = {
			id: spec.id,
			kind: "factory",
			title: spec.title,
			content: spec.description,
			path: "factory-dag-eval",
			scope: "local",
			reference: {},
			arguments: { dag: spec.dag },
			metadata: { evalKind: spec.kind, source: "factory-dag-eval" },
			source: "agent",
			created_at: now.toISOString(),
			updated_at: now.toISOString(),
			version: 1,
		};
	}
	return `${JSON.stringify({ schema: 1, entries: { prompt: {}, memory: {}, skill: {}, subagent: {}, factory: entries }, refinements: [] }, null, 2)}\n`;
}

/** Seed the local harness dir for this session so rlm.factory.run sees the specs. */
export function seedHarnessState(sessionManager: SessionManager, specs: ReferenceFactory[]): string {
	const artifactDir = getSessionArtifactPath(sessionManager.getSessionDir(), sessionManager.getSessionId());
	const harnessDir = join(artifactDir, "harness");
	mkdirSync(harnessDir, { recursive: true });
	const statePath = join(harnessDir, "harness_state.json");
	writeFileSync(statePath, buildHarnessStateFile(specs));
	return statePath;
}

// ---------------------------------------------------------------------------
// Parent prompts.
// ---------------------------------------------------------------------------

function pollCode(breakStates: string): string {
	return [
		"import asyncio, json",
		"started = await rlm.factory.run('<ID>')",
		'run_id = started["run_id"]',
		"while True:",
		"\tstatus = await rlm.factory.status(run_id)",
		`\tif status["state"] in (${breakStates}):`,
		"\t\tbreak",
		"\tawait asyncio.sleep(5)",
	].join("\n");
}

function fill(prompt: string, factory: ReferenceFactory, ledgerPath: string, poll: string): string {
	return prompt
		.replace("<POLL>", poll)
		.replaceAll("<ID>", factory.id)
		.replaceAll("<LEDGER>", ledgerPath);
}

/**
 * Parent prompt for a factory trial. Contains NO spawn/collect instructions: the
 * verdict "no task-specific orchestration code in the parent" is checked against
 * this builder (see test/factory-eval.test.ts).
 */
export function buildFactoryParentPrompt(factory: ReferenceFactory, ledgerPath: string): string {
	const head = `Capability eval: factory DAG orchestration. The local harness state for this session seeds exactly one factory specification: "${factory.id}". Run it with the executor and report the outcome. Do not spawn subagents yourself; the factory executor owns the children.`;
	switch (factory.kind) {
		case "review-sweep":
			return fill(
				[
					head,
					"",
					"Step 1 — start the run and poll it to a terminal state in one ipython cell:",
					"",
					"<POLL>",
					"",
					'Step 2 — in the same or a new ipython cell, save the final status:',
					"",
					'\tjson.dump(status, open(r"<LEDGER>", "w"))',
					"",
					'Step 3 — the node with id "report" in status["nodes"] has an answer_preview containing a JSON object like {"issues": [...]}. Output exactly one line and nothing else:',
					"",
					'ANSWER: ISSUES: <every audit id inside the report answer_preview, comma-separated>; STATE: <status["state"]>',
				].join("\n"),
				factory,
				ledgerPath,
				pollCode('"done", "failed", "stopped", "paused"'),
			);
		case "builder":
			return fill(
				[
					head,
					"",
					"Step 1 — start the run and poll it to a terminal state in one ipython cell:",
					"",
					"<POLL>",
					"",
					'Step 2 — in the same or a new ipython cell, save the final status:',
					"",
					'\tjson.dump(status, open(r"<LEDGER>", "w"))',
					"",
					'Step 3 — the node with id "collector" in status["nodes"] has an answer_preview starting with COLLECTED and listing every swb-marker. Output exactly one line and nothing else:',
					"",
					'ANSWER: MARKERS: <every swb-marker from the collector answer_preview, comma-separated>; STATE: <status["state"]>',
				].join("\n"),
				factory,
				ledgerPath,
				pollCode('"done", "failed", "stopped", "paused"'),
			);
		case "resident-watcher":
			return fill(
				[
					head,
					"The watcher node is resident: the run reaches done while the watcher child stays alive; you must then stop the run to tear it down.",
					"",
					"Step 1 — start the run and poll until the declarative work is done (state done) in one ipython cell:",
					"",
					"<POLL>",
					"",
					"Step 2 — stop the run, save the final status, and report. In the same or a new ipython cell:",
					"",
					"\tstopped = await rlm.factory.stop(run_id)",
					"\tstatus = await rlm.factory.status(run_id)",
					'\tjson.dump(status, open(r"<LEDGER>", "w"))',
					"\tstopped",
					"",
					'Step 3 — the nodes "task-a" and "task-b" in status["nodes"] have answer_previews listing the step markers, and stopped["cancelled"] lists the torn-down resident. Output exactly one line and nothing else:',
					"",
					'ANSWER: MARKERS: <the two step markers, comma-separated>; STOPPED: <the cancelled node ids from stopped["cancelled"], comma-separated>; STATE: <status["state"]>',
				].join("\n"),
				factory,
				ledgerPath,
				pollCode('"done", "failed", "paused"'),
			);
		case "review-sweep-fail":
			return fill(
				[
					head,
					"One reviewer node in this factory is planted to fail (its subagent model reference cannot be resolved), so the declared escalate policy must pause the run.",
					"",
					"Step 1 — start the run and poll it to a terminal state in one ipython cell:",
					"",
					"<POLL>",
					"",
					'Step 2 — save the paused status, then stop the run to cancel the in-flight children. In the same or a new ipython cell:',
					"",
					'\tjson.dump(status, open(r"<LEDGER>", "w"))',
					"\tawait rlm.factory.stop(run_id)",
					"",
					'Step 3 — from the saved status: STATE is status["state"], FAILED-NODE is the id of the node whose status is "error", and REPORT-STATUS is the status of the node "report". Output exactly one line and nothing else:',
					"",
					"ANSWER: STATE: <state>; FAILED-NODE: <failing node id>; REPORT-STATUS: <status of the report node>",
				].join("\n"),
				factory,
				ledgerPath,
				pollCode('"done", "failed", "stopped", "paused"'),
			);
		case "dry-run-reject":
			return fill(
				[
					head,
					"This specification is intentionally INVALID: it references a harness subagent that does not exist. The run call must raise and start no node.",
					"",
					"Step 1 — in one ipython cell:",
					"",
					"\timport json",
					'\terror_message = "no error"',
					"\ttry:",
					"\t\tawait rlm.factory.run('<ID>')",
					"\texcept Exception as exc:",
					"\t\terror_message = str(exc)",
					"\tsubs = await rlm.list_subagents()",
					"\tprint(error_message)",
					"\tsubs",
					"",
					"Step 2 — output exactly one line and nothing else:",
					"",
					"ANSWER: REJECTED: <yes if the run call raised, otherwise no>; CHILDREN: <the number of entries in subs>; MESSAGE: <the error message>",
				].join("\n"),
				factory,
				ledgerPath,
				"",
			);
	}
}

function reviewerPromptBlock(fileName: string): string {
	const prompt = buildReviewerPromptFor(fileName);
	return `\t"${fileName}": """\n${prompt.replaceAll("\n", "\n\t")}""",`;
}

function budgetLine(factory: ReferenceFactory): string {
	return `Declared budget: complete the whole run within ${Math.round(factory.declaredBudgetMs / 60000)} minutes; each child within ${Math.round(NODE_BUDGET_MS / 60000)} minutes.`;
}

/** Baseline prompt: the identical task done with manual orchestration. */
export function buildBaselinePrompt(factory: ReferenceFactory, ledgerPath: string): string {
	switch (factory.kind) {
		case "review-sweep":
			return [
				"Capability eval: manual multi-agent orchestration (baseline). Do the identical pull-request review sweep by orchestrating the children yourself with rlm.spawn and rlm.collect. Do NOT use the factory executor.",
				budgetLine(factory),
				"",
				"Mini-repo under review (four files, one planted defect each):",
				"",
				miniRepoListing(),
				"",
				"Step 1 — spawn one reviewer child per file in one ipython cell, using the exact child prompts below. Do not set a model on the spawn; children inherit yours.",
				"",
				"import asyncio, json",
				"reviewer_prompts = {",
				...REVIEW_FILES.map((file) => reviewerPromptBlock(file.name)),
				"}",
				'handles = {name: await rlm.spawn(prompt, name=f"reviewer-{name}") for name, prompt in reviewer_prompts.items()}',
				'ids = [handle.rlm_child_id for handle in handles.values()]',
				"",
				"Step 2 — poll until all four children settle, then save their answers:",
				"",
				"while True:",
				"\tresults = await rlm.collect(ids, timeout_ms=2000)",
				"\tif all(r.settled for r in results):",
				"\t\tbreak",
				"\tawait asyncio.sleep(2)",
				"answers = {r.session_name: r.answer_preview for r in results}",
				`json.dump(answers, open(r"${ledgerPath}", "w"))`,
				"results",
				"",
				"Step 3 — aggregate the found audit ids from the four answer previews yourself and output exactly one line and nothing else:",
				"",
				"ANSWER: ISSUES: <every audit id found, comma-separated, sorted>; STATE: done",
			].join("\n");
		case "builder": {
			const width = factory.width ?? DEFAULT_WIDTH;
			const prompts = Array.from({ length: width }, (_, i) => {
				const prompt = buildBuilderNodePrompt(i + 1);
				return `\t"${i + 1}": """\n${prompt.replaceAll("\n", "\n\t")}""",`;
			});
			return [
				"Capability eval: manual multi-agent orchestration (baseline). Do the identical wide build by orchestrating the children yourself with rlm.spawn and rlm.collect. Do NOT use the factory executor.",
				budgetLine(factory),
				"",
				`Step 1 — spawn ${width} builder children in one ipython cell, using the exact child prompts below. Do not set a model on the spawn; children inherit yours.`,
				"",
				"import asyncio, json",
				"builder_prompts = {",
				...prompts,
				"}",
				'handles = {i: await rlm.spawn(prompt, name=f"builder-{i}") for i, prompt in builder_prompts.items()}',
				"ids = [handle.rlm_child_id for handle in handles.values()]",
				"",
				"Step 2 — poll until every child settles, then save their answers:",
				"",
				"while True:",
				"\tresults = await rlm.collect(ids, timeout_ms=2000)",
				"\tif all(r.settled for r in results):",
				"\t\tbreak",
				"\tawait asyncio.sleep(2)",
				"answers = {r.session_name: r.answer_preview for r in results}",
				`json.dump(answers, open(r"${ledgerPath}", "w"))`,
				"results",
				"",
				"Step 3 — merge the builder markers from the answer previews yourself and output exactly one line and nothing else:",
				"",
				"ANSWER: MARKERS: <every swb-marker found, comma-separated, in ascending node order>; STATE: done",
			].join("\n");
		}
		case "resident-watcher":
			return [
				"Capability eval: manual multi-agent orchestration (baseline). Run the identical resident-watcher topology by orchestrating the children yourself with rlm.spawn and rlm.collect. Do NOT use the factory executor.",
				budgetLine(factory),
				"",
				"Step 1 — spawn the watcher child and the task-a child in one ipython cell, using the exact child prompts below. Do not set a model on the spawn; children inherit yours.",
				"",
				"import asyncio, json",
				'watcher = await rlm.spawn("""',
				`\t${buildWatcherPrompt().replaceAll("\n", "\n\t")}`,
				'\t""", name="watcher")',
				'task_a = await rlm.spawn("""',
				`\t${buildTaskAPrompt().replaceAll("\n", "\n\t")}`,
				'\t""", name="task-a")',
				"",
				"Step 2 — poll until task-a settles and read its answer_preview:",
				"",
				"while True:",
				"\tresults = await rlm.collect([task_a.rlm_child_id], timeout_ms=2000)",
				"\tif all(r.settled for r in results):",
				"\t\tbreak",
				"\tawait asyncio.sleep(2)",
				"task_a_answer = results[0].answer_preview",
				"",
				"Step 3 — spawn task-b with the task-b prompt below, replacing the line that reads `The previous step reported: {prev}` with task_a_answer:",
				"",
				'task_b = await rlm.spawn("""',
				`\t${buildTaskBPromptTemplate().replaceAll("\n", "\n\t")}`,
				'\t""", name="task-b")',
				"",
				"Step 4 — poll until task-b settles, then save the answers and tear the watcher down:",
				"",
				"while True:",
				"\tresults = await rlm.collect([task_a.rlm_child_id, task_b.rlm_child_id], timeout_ms=2000)",
				"\tif all(r.settled for r in results):",
				"\t\tbreak",
				"\tawait asyncio.sleep(2)",
				'answers = {r.session_name: r.answer_preview for r in results}',
				`json.dump(answers, open(r"${ledgerPath}", "w"))`,
				"await rlm.delete_subagent(watcher.rlm_child_id)",
				"",
				"Step 5 — output exactly one line and nothing else:",
				"",
				"ANSWER: MARKERS: <the two step markers from the answers, comma-separated>; STOPPED: watcher; STATE: done",
			].join("\n");
		default:
			throw new Error(`no baseline exists for reference factory kind ${factory.kind}`);
	}
}

// ---------------------------------------------------------------------------
// Answer parsing and task-success checking.
// ---------------------------------------------------------------------------

export interface ParsedAnswer {
	issues: string[];
	markers: string[];
	state: string | null;
	stopped: string[];
	failedNode: string | null;
	reportStatus: string | null;
	rejected: boolean | null;
	children: number | null;
	message: string | null;
}

/** Parse the single ANSWER line from the parent's final assistant text. */
export function parseAnswerLine(text: string | undefined): ParsedAnswer | null {
	const match = /ANSWER:\s*(.+?)(?:\r?\n|$)/i.exec(text ?? "");
	if (!match) return null;
	const parsed: ParsedAnswer = {
		issues: [],
		markers: [],
		state: null,
		stopped: [],
		failedNode: null,
		reportStatus: null,
		rejected: null,
		children: null,
		message: null,
	};
	const list = (value: string): string[] =>
		value
			.split(",")
			.map((entry) => entry.trim())
			.filter((entry) => entry.length > 0);
	for (const part of match[1].split(";")) {
		const separator = part.indexOf(":");
		if (separator < 0) continue;
		const key = part.slice(0, separator).trim().toUpperCase();
		const value = part.slice(separator + 1).trim();
		if (!key || !value) continue;
		switch (key) {
			case "ISSUES":
				parsed.issues = list(value);
				break;
			case "MARKERS":
				parsed.markers = list(value);
				break;
			case "STATE":
				parsed.state = value;
				break;
			case "STOPPED":
				parsed.stopped = list(value);
				break;
			case "FAILED-NODE":
				parsed.failedNode = value;
				break;
			case "REPORT-STATUS":
				parsed.reportStatus = value;
				break;
			case "REJECTED":
				parsed.rejected = value.toLowerCase() === "yes";
				break;
			case "CHILDREN":
				parsed.children = Number(value);
				break;
			case "MESSAGE":
				parsed.message = value;
				break;
		}
	}
	return parsed;
}

// ---------------------------------------------------------------------------
// Ledger shapes (the executor's status() payload) and the replay checker.
// ---------------------------------------------------------------------------

export interface FactoryLedgerInstance {
	index: number;
	status: string;
	attempt: number;
	child?: string | null;
	duration_ms?: number | null;
	error?: string | null;
}

export interface FactoryLedgerNode {
	id: string;
	status: string;
	lifecycle: string;
	attempts: number;
	instances: FactoryLedgerInstance[];
	answer_preview?: string;
	error?: string;
}

export interface FactoryLedgerEvent {
	seq: number;
	kind: string;
	stage?: string;
	node?: string;
	instance?: number;
	detail?: string;
	duration_ms?: number | null;
	status?: string;
	error?: string;
	milestone?: string;
}

export interface FactoryLedgerUsage {
	spawns: number;
	settled: number;
	tool_uses: number;
	max_parallel: number;
	running: number;
}

export interface FactoryStatusLedger {
	run_id: string;
	spec_id: string;
	name: string | null;
	state: string;
	nodes: FactoryLedgerNode[];
	events: FactoryLedgerEvent[];
	elapsed_ms: number;
	usage: FactoryLedgerUsage;
}

export const KNOWN_FACTORY_EVENT_KINDS = [
	"run_started",
	"node_ready",
	"spawned",
	"spawn_backoff",
	"spawn_deferred",
	"settled",
	"answer_captured",
	"retry",
	"node_error",
	"node_cancelled",
	"cancelled",
	"cancel_failed",
	"milestone",
	"run_stopped",
	"resumed",
	"executor_error",
] as const;

const KNOWN_STAGES = ["recorded", "arrived", "shown", "delivered"];
const LEDGER_STATES = ["running", "stopping", "paused", "done", "failed", "stopped"];
const INSTANCE_INSTANCE_PATTERN = /^[a-z0-9][a-z0-9-]{0,63}$/;

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isLedgerShape(value: unknown): value is FactoryStatusLedger {
	if (!isRecord(value)) return false;
	if (typeof value.run_id !== "string" || !value.run_id) return false;
	if (typeof value.spec_id !== "string" || !value.spec_id) return false;
	if (typeof value.state !== "string" || !LEDGER_STATES.includes(value.state)) return false;
	if (!Array.isArray(value.nodes) || value.nodes.length === 0) return false;
	if (!Array.isArray(value.events)) return false;
	if (typeof value.elapsed_ms !== "number" || value.elapsed_ms < 0) return false;
	if (!isRecord(value.usage)) return false;
	return true;
}

export interface LedgerCheckResult {
	ok: boolean;
	problems: string[];
}

/**
 * Deterministic replay check over one saved status ledger: stable event
 * identities (contiguous seq, closed kind/stage vocabularies, valid node refs)
 * and complete resource accounting (every spawned instance settled with a
 * duration, cancelled, or still in flight on a paused run; usage counts match
 * the event stream).
 */
export function checkReplayLedger(ledger: unknown): LedgerCheckResult {
	const problems: string[] = [];
	if (!isLedgerShape(ledger)) {
		return { ok: false, problems: ["ledger does not match the factory status shape"] };
	}
	const nodes = ledger.nodes;
	const nodeIds = new Set<string>();
	for (const node of nodes) {
		if (typeof node.id !== "string" || !INSTANCE_INSTANCE_PATTERN.test(node.id)) {
			problems.push(`ledger node has an invalid id: ${JSON.stringify(node.id)}`);
		} else if (nodeIds.has(node.id)) {
			problems.push(`ledger duplicates node id ${node.id}`);
		}
		nodeIds.add(node.id);
	}

	let truncated = false;
	let lastSeq = 0;
	const spawned = new Map<string, number>();
	const settled = new Map<string, number>();
	const cancelled = new Set<string>();
	const milestones: string[] = [];
	let runStopped = false;
	for (const [index, event] of ledger.events.entries()) {
		if (!isRecord(event)) {
			problems.push(`events[${index}] is not an object`);
			continue;
		}
		const seq = event.seq;
		if (typeof seq !== "number" || !Number.isInteger(seq) || seq <= lastSeq) {
			problems.push(`events[${index}] has a non-increasing seq: ${JSON.stringify(seq)}`);
			continue;
		}
		if (index === 0 && seq !== 1) truncated = true;
		lastSeq = seq;
		if (!KNOWN_FACTORY_EVENT_KINDS.includes(event.kind as (typeof KNOWN_FACTORY_EVENT_KINDS)[number])) {
			problems.push(`events[${index}] has an unknown kind: ${JSON.stringify(event.kind)}`);
		}
		if (event.stage !== undefined && !KNOWN_STAGES.includes(event.stage)) {
			problems.push(`events[${index}] has an unknown stage: ${JSON.stringify(event.stage)}`);
		}
		if (event.node !== undefined && !nodeIds.has(event.node)) {
			problems.push(`events[${index}] references unknown node ${JSON.stringify(event.node)}`);
		}
		const key = event.node !== undefined ? `${event.node}#${event.instance ?? -1}` : "";
		switch (event.kind) {
			case "spawned":
				if (key) spawned.set(key, (spawned.get(key) ?? 0) + 1);
				break;
			case "settled":
				if (key) settled.set(key, (settled.get(key) ?? 0) + 1);
				if (event.status === "done" && (typeof event.duration_ms !== "number" || event.duration_ms < 0)) {
					problems.push(`events[${index}] settles done without a duration_ms`);
				}
				if (event.status === "error" && typeof event.error !== "string") {
					problems.push(`events[${index}] settles error without an error message`);
				}
				if (event.status !== undefined && !["done", "error"].includes(event.status)) {
					problems.push(`events[${index}] has an invalid settled status: ${JSON.stringify(event.status)}`);
				}
				break;
			case "cancelled":
				if (key) cancelled.add(key);
				break;
			case "milestone":
				if (typeof event.milestone === "string") milestones.push(event.milestone);
				break;
			case "run_stopped":
				runStopped = true;
				break;
		}
	}
	if (truncated) {
		problems.push("event window is truncated (first seq is not 1); count assertions skipped");
	}

	if (!truncated) {
		// Complete resource accounting per instance.
		for (const node of nodes) {
			for (const instance of node.instances ?? []) {
				const key = `${node.id}#${instance.index ?? -1}`;
				const hasSpawn = (spawned.get(key) ?? 0) > 0;
				const settleCount = settled.get(key) ?? 0;
				if (instance.status === "done") {
					if (settleCount === 0) problems.push(`node ${node.id} instance ${instance.index} is done without a settle event`);
					else if (typeof instance.duration_ms !== "number" || instance.duration_ms < 0) {
						problems.push(`node ${node.id} instance ${instance.index} is done without a duration_ms`);
					}
				} else if (instance.status === "error") {
					if (settleCount === 0) problems.push(`node ${node.id} instance ${instance.index} errored without a settle event`);
					else if (typeof instance.error !== "string" || !instance.error) {
						problems.push(`node ${node.id} instance ${instance.index} errored without an error message`);
					}
					if (hasSpawn && instance.duration_ms == null) {
						problems.push(`node ${node.id} instance ${instance.index} errored after spawn without a duration_ms`);
					}
				} else if (instance.status === "cancelled") {
					if (!cancelled.has(key)) problems.push(`node ${node.id} instance ${instance.index} is cancelled without a cancel event`);
				} else if (instance.status === "running" || instance.status === "pending") {
					if (!["paused", "running", "stopping"].includes(ledger.state)) {
						problems.push(`node ${node.id} instance ${instance.index} is ${instance.status} in a ${ledger.state} ledger`);
					}
				}
			}
		}
		for (const [key, count] of spawned) {
			const settledCount = settled.get(key) ?? 0;
			if (settledCount === 0 && !cancelled.has(key)) {
				const node = nodes.find((entry) => key.startsWith(`${entry.id}#`));
				const stillInFlight =
					node !== undefined &&
					["paused", "running", "stopping"].includes(ledger.state) &&
					node.instances.some(
						(instance) => `${node.id}#${instance.index}` === key && ["pending", "running"].includes(instance.status),
					);
				if (!stillInFlight) problems.push(`spawned instance ${key} never settled or cancelled (${count} spawn event(s))`);
			}
		}
		// Admission failures settle without a spawn; every other settle must follow a spawn.
		for (const [key, count] of settled) {
			if ((spawned.get(key) ?? 0) === 0 && count > 0) {
				const node = nodes.find((entry) => key.startsWith(`${entry.id}#`));
				const admissionFailure =
					node !== undefined &&
					node.instances.some(
						(instance) => `${node.id}#${instance.index}` === key && instance.status === "error",
					);
				if (!admissionFailure) {
					problems.push(`instance ${key} settled without ever being spawned`);
				}
			}
		}
		const spawnEvents = [...spawned.values()].reduce((sum, count) => sum + count, 0);
		const collectSettles = [...settled.entries()].filter(([key]) => (spawned.get(key) ?? 0) > 0).length;
		if (ledger.usage.spawns !== spawnEvents) {
			problems.push(`usage.spawns ${ledger.usage.spawns} does not match ${spawnEvents} spawned event(s)`);
		}
		if (ledger.usage.settled !== collectSettles) {
			problems.push(`usage.settled ${ledger.usage.settled} does not match ${collectSettles} collect settlement(s)`);
		}
	}

	// Run-state milestone identities.
	if (!truncated) {
		if (ledger.state === "done" && !milestones.includes("finished")) {
			problems.push("run state done without a finished milestone");
		}
		if (ledger.state === "failed" && !milestones.includes("failed")) {
			problems.push("run state failed without a failed milestone");
		}
		if (
			ledger.state === "paused" &&
			!milestones.some((milestone) => milestone === "paused" || milestone === "budget_exceeded")
		) {
			problems.push("run state paused without a paused or budget_exceeded milestone");
		}
		if (ledger.state === "stopped" && !runStopped) {
			problems.push("run state stopped without a run_stopped event");
		}
	}
	return { ok: problems.length === 0, problems };
}

/** Run the replay check over a saved report.json or a single status ledger. */
export function runReplayChecks(data: unknown): { ok: boolean; ledgers: { id: string; result: LedgerCheckResult }[] } {
	const ledgers: { id: string; result: LedgerCheckResult }[] = [];
	if (isRecord(data) && Array.isArray(data.trials)) {
		for (const trial of data.trials) {
			if (!isRecord(trial)) continue;
			if (trial.ledger === null || trial.ledger === undefined) continue;
			ledgers.push({
				id: `${String(trial.factory)}/${String(trial.arm)}/trial-${String(trial.trial)}`,
				result: checkReplayLedger(trial.ledger),
			});
		}
	} else {
		ledgers.push({ id: "ledger", result: checkReplayLedger(data) });
	}
	return { ok: ledgers.length > 0 && ledgers.every((entry) => entry.result.ok), ledgers };
}

// ---------------------------------------------------------------------------
// Task-success checking (checkable answers + ledger cross-checks).
// ---------------------------------------------------------------------------

export function checkTaskSuccess(
	factory: ReferenceFactory,
	answer: ParsedAnswer | null,
	ledger: FactoryStatusLedger | null,
): { ok: boolean; problems: string[] } {
	const problems: string[] = [];
	if (answer === null) return { ok: false, problems: ["no ANSWER line in the parent's final text"] };
	const width = factory.width ?? DEFAULT_WIDTH;
	const nodeStatus = (id: string): FactoryLedgerNode | undefined => ledger?.nodes.find((node) => node.id === id);
	switch (factory.kind) {
		case "review-sweep": {
			for (const issueId of REVIEW_ISSUE_IDS) {
				if (!answer.issues.includes(issueId)) problems.push(`planted issue ${issueId} missing from the ANSWER line`);
			}
			if (answer.state !== "done") problems.push(`ANSWER state is ${answer.state ?? "unset"}, expected done`);
			if (ledger !== null) {
				if (ledger.state !== "done") problems.push(`ledger state is ${ledger.state}, expected done`);
				const report = nodeStatus("report");
				if (report?.status !== "done") problems.push("ledger report node is not done");
				for (const issueId of REVIEW_ISSUE_IDS) {
					if (!report?.answer_preview?.includes(issueId)) {
						problems.push(`planted issue ${issueId} missing from the report node answer preview`);
					}
				}
			}
			break;
		}
		case "builder": {
			for (let index = 1; index <= width; index++) {
				const marker = BUILDER_MARKER(index);
				if (!answer.markers.includes(marker)) problems.push(`${marker} missing from the ANSWER line`);
			}
			if (answer.state !== "done") problems.push(`ANSWER state is ${answer.state ?? "unset"}, expected done`);
			if (ledger !== null) {
				if (ledger.state !== "done") problems.push(`ledger state is ${ledger.state}, expected done`);
				const collector = nodeStatus("collector");
				if (collector?.status !== "done") problems.push("ledger collector node is not done");
				for (let index = 1; index <= width; index++) {
					const marker = BUILDER_MARKER(index);
					if (!collector?.answer_preview?.includes(marker)) {
						problems.push(`${marker} missing from the collector answer preview`);
					}
				}
			}
			break;
		}
		case "resident-watcher": {
			for (const marker of [TASK_MARKER_A, TASK_MARKER_B]) {
				if (!answer.markers.includes(marker)) problems.push(`${marker} missing from the ANSWER line`);
			}
			if (!answer.stopped.includes("watcher")) problems.push("ANSWER does not report the resident watcher as stopped");
			if (ledger !== null) {
				if (ledger.state !== "stopped") problems.push(`ledger state is ${ledger.state}, expected stopped`);
				for (const id of ["task-a", "task-b"]) {
					if (nodeStatus(id)?.status !== "done") problems.push(`ledger ${id} node is not done`);
				}
				const watcher = nodeStatus("watcher");
				if (watcher?.status !== "cancelled") problems.push("ledger watcher node is not cancelled");
				const watcherInstance = watcher?.instances.find((instance) => instance.status === "cancelled");
				if (!watcherInstance) problems.push("ledger watcher instance is not cancelled");
			}
			break;
		}
		case "review-sweep-fail": {
			if (answer.state !== "paused") problems.push(`ANSWER state is ${answer.state ?? "unset"}, expected paused`);
			if (answer.failedNode !== "review-broken") {
				problems.push(`ANSWER failed node is ${answer.failedNode ?? "unset"}, expected review-broken`);
			}
			if (answer.reportStatus !== "pending") {
				problems.push(`ANSWER report status is ${answer.reportStatus ?? "unset"}, expected pending`);
			}
			if (ledger !== null) {
				if (ledger.state !== "paused") problems.push(`ledger state is ${ledger.state}, expected paused`);
				const broken = nodeStatus("review-broken");
				if (broken?.status !== "error") problems.push("ledger review-broken node is not error");
				if (!broken?.error) problems.push("ledger review-broken node has no error message");
				if (nodeStatus("report")?.status !== "pending") problems.push("ledger report node is not pending");
				if (ledger.events.some((event) => event.kind === "spawned" && event.node === "report")) {
					problems.push("report node started despite the escalation pause");
				}
				if (!ledger.events.some((event) => event.kind === "milestone" && event.milestone === "paused")) {
					problems.push("ledger has no paused milestone");
				}
			}
			break;
		}
		case "dry-run-reject": {
			if (answer.rejected !== true) problems.push("ANSWER does not report the run call as rejected");
			if (answer.children !== 0) problems.push(`ANSWER children count is ${answer.children ?? "unset"}, expected 0`);
			if (!answer.message?.includes("no-such-subagent-entry")) {
				problems.push("ANSWER message does not mention the unknown subagent reference");
			}
			break;
		}
	}
	return { ok: problems.length === 0, problems };
}

// ---------------------------------------------------------------------------
// Trial result, verdicts, and report rendering.
// ---------------------------------------------------------------------------

export interface FactoryDagEvalTrialResult {
	factory: string;
	arm: "factory" | "baseline";
	trial: number;
	model: string;
	taskSuccess: boolean;
	problems: string[];
	state: string | null;
	wallMs: number;
	contextTokens: number | null;
	totalTokens: number | null;
	declaredFanIn: number;
	queueLatencyMs: null;
	teardownLatencyMs: number | null;
	declaredBudgetMs: number;
	budgetOvershootMs: number;
	elapsedMs: number | null;
	spawns: number | null;
	settled: number | null;
	replayOk: boolean | null;
	replayProblems: string[];
	answer: ParsedAnswer | null;
	ledger: FactoryStatusLedger | null;
	verdict: "pass" | "fail";
}

export interface EvalVerdicts {
	noOrchestrationCode: boolean;
	failurePolicyMatched: boolean | null;
	dryRunRejected: boolean | null;
	budgetOvershootMs: number;
	budgetOvershootZero: boolean | null;
	contextPairs: {
		factory: string;
		factoryContextTokens: number | null;
		baselineContextTokens: number | null;
		lower: boolean | null;
		bothCorrect: boolean;
	}[];
}

export function computeVerdicts(results: FactoryDagEvalTrialResult[]): EvalVerdicts {
	const factoryArms = results.filter((row) => row.arm === "factory" && row.factory !== "review-sweep-fail" && row.factory !== "dry-run-reject");
	const escalation = results.find((row) => row.factory === "review-sweep-fail" && row.arm === "factory");
	const dryRun = results.find((row) => row.factory === "dry-run-reject" && row.arm === "factory");
	const budgetOvershootMs = factoryArms.reduce((sum, row) => sum + row.budgetOvershootMs, 0);
	const contextPairs: EvalVerdicts["contextPairs"] = [];
	for (const factory of ["review-sweep", "builder", "resident-watcher"]) {
		const factoryRows = factoryArms.filter((row) => row.factory === factory);
		const baselineRows = results.filter((row) => row.arm === "baseline" && row.factory === factory);
		if (factoryRows.length === 0 && baselineRows.length === 0) continue;
		const factoryContext = averageOrNull(factoryRows.map((row) => row.contextTokens));
		const baselineContext = averageOrNull(baselineRows.map((row) => row.contextTokens));
		contextPairs.push({
			factory,
			factoryContextTokens: factoryContext,
			baselineContextTokens: baselineContext,
			lower:
				factoryContext === null || baselineContext === null ? null : factoryContext < baselineContext,
			bothCorrect:
				factoryRows.every((row) => row.taskSuccess) && baselineRows.every((row) => row.taskSuccess),
		});
	}
	return {
		noOrchestrationCode: true, // Static: buildFactoryParentPrompt emits no spawn/collect calls (unit-tested).
		failurePolicyMatched: escalation ? escalation.verdict === "pass" : null,
		dryRunRejected: dryRun ? dryRun.verdict === "pass" : null,
		budgetOvershootMs,
		budgetOvershootZero: factoryArms.length === 0 ? null : budgetOvershootMs === 0,
		contextPairs,
	};
}

function averageOrNull(values: (number | null)[]): number | null {
	const present = values.filter((value): value is number => value !== null);
	if (present.length === 0) return null;
	return Math.round(present.reduce((sum, value) => sum + value, 0) / present.length);
}

export function renderMarkdownReport(results: FactoryDagEvalTrialResult[], config: EvalConfig): string {
	const header = [
		"# Factory DAG capability eval report",
		"",
		`- model: ${config.model}`,
		`- factories: ${config.factories.join(", ")}  |  width: ${config.width}  |  trials per pair: ${config.trials}`,
		`- declared budgets: run ${RUN_BUDGET_MS} ms, per task node ${NODE_BUDGET_MS} ms (same for each factory/baseline pair)`,
		"- queue latency: omitted (the executor event ledger carries no timestamps)",
		"- teardown latency: resident trial, finished-notice to final answer",
		"",
		"| factory | arm | trial | task | state | wall s | ctx tokens | total tokens | fan-in | teardown ms | over budget ms | replay | verdict |",
		"| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
	];
	const rows = results.map((row) =>
		[
			row.factory,
			row.arm,
			row.trial,
			row.taskSuccess ? "ok" : "failed",
			row.state ?? "n/a",
			(row.wallMs / 1000).toFixed(1),
			row.contextTokens ?? "n/a",
			row.totalTokens ?? "n/a",
			row.declaredFanIn,
			row.teardownLatencyMs ?? "n/a",
			row.budgetOvershootMs,
			row.replayOk === null ? "n/a" : row.replayOk ? "ok" : "failed",
			row.verdict,
		].join(" | "),
	);
	const verdicts = computeVerdicts(results);
	const pairs = verdicts.contextPairs.map((pair) =>
		`| ${[
			pair.factory,
			pair.factoryContextTokens ?? "n/a",
			pair.baselineContextTokens ?? "n/a",
			pair.lower === null ? "n/a" : pair.lower ? "yes" : "no",
			pair.bothCorrect ? "yes" : "no",
		].join(" | ")} |`,
	);
	const summary = [
		"",
		"## Factory vs hand-written baseline (parent context tokens)",
		"",
		"| factory | factory avg | baseline avg | lower | both task-correct |",
		"| --- | --- | --- | --- | --- |",
		...pairs,
		"",
		"## Verdict rules (Notion spec, Proposed evaluation)",
		"",
		`- no task-specific orchestration code in factory prompts: ${verdicts.noOrchestrationCode ? "PASS" : "FAIL"}`,
		`- declared failure policy matches observed behavior (escalation): ${renderVerdict(verdicts.failurePolicyMatched)}`,
		`- no node starts after a failed dry run: ${renderVerdict(verdicts.dryRunRejected)}`,
		`- total budget overshoot: ${verdicts.budgetOvershootMs} ms ${renderVerdict(verdicts.budgetOvershootZero)}`,
		"- parent context lower than baseline: reported per pair above (informational, not asserted)",
		"",
		"## Problems",
		"",
		...(results.flatMap((row) => [...row.problems, ...row.replayProblems].map((problem) => `- ${row.factory}/${row.arm}/${row.trial}: ${problem}`)).length > 0
			? results.flatMap((row) =>
					[...row.problems, ...row.replayProblems].map((problem) => `- ${row.factory}/${row.arm}/trial ${row.trial}: ${problem}`),
				)
			: ["- none"]),
		"",
	];
	return [...header, ...rows.map((row) => `| ${row} |`), ...summary].join("\n");
}

function renderVerdict(value: boolean | null): string {
	if (value === null) return "(not run)";
	return value ? "(PASS)" : "(FAIL)";
}

// ---------------------------------------------------------------------------
// Driver.
// ---------------------------------------------------------------------------

export type FactorySelection = "review-sweep" | "builder" | "resident-watcher";

export interface EvalConfig {
	model: string;
	factories: FactorySelection[];
	width: number;
	trials: number;
	timeoutMinutes: number;
	outDir: string;
}

export const DEFAULT_EVAL_CONFIG: Pick<EvalConfig, "factories" | "width" | "trials" | "timeoutMinutes"> = {
	factories: ["review-sweep", "builder", "resident-watcher"],
	width: DEFAULT_WIDTH,
	trials: 1,
	timeoutMinutes: 20,
};

export function parseEvalArgs(argv: string[], defaults = DEFAULT_EVAL_CONFIG): EvalConfig | { error: string } {
	const args: EvalConfig = {
		model: DEFAULT_MODEL,
		factories: [...defaults.factories],
		width: defaults.width,
		trials: defaults.trials,
		timeoutMinutes: defaults.timeoutMinutes,
		outDir: "",
	};
	const rest = [...argv];
	while (rest.length > 0) {
		const arg = rest.shift() as string;
		const value = (flag: string): string => {
			const next = rest.shift();
			if (next === undefined) throw new Error(`Missing value for ${flag}`);
			return next;
		};
		switch (arg) {
			case "--model":
				args.model = value(arg);
				break;
			case "--factories": {
				const known: FactorySelection[] = ["review-sweep", "builder", "resident-watcher"];
				const selected = value(arg)
					.split(",")
					.map((raw) => raw.trim())
					.filter((raw) => raw.length > 0)
					.filter((raw): raw is FactorySelection => known.includes(raw as FactorySelection));
				if (selected.length > 0) args.factories = selected;
				break;
			}
			case "--width":
				args.width = Math.min(MAX_WIDTH, Math.max(2, Number(value(arg))));
				break;
			case "--trials":
				args.trials = Math.max(1, Number(value(arg)));
				break;
			case "--timeout-minutes":
				args.timeoutMinutes = Math.max(1, Number(value(arg)));
				break;
			case "--out":
				args.outDir = value(arg);
				break;
			case "--help":
			case "-h":
				return { error: "help" };
			default:
				return { error: `Unknown argument: ${arg}` };
		}
	}
	if (!args.outDir) args.outDir = `factory-dag-eval-reports/${new Date().toISOString().replace(/[:.]/g, "-")}`;
	return args;
}

interface SessionBundle {
	session: Awaited<ReturnType<typeof createAgentSession>>["session"];
	sessionManager: SessionManager;
	tempRoot: string;
}

async function createEvalSession(config: EvalConfig, label: string): Promise<SessionBundle> {
	const realAgentDir = getAgentDir();
	const tempRoot = join(
		tmpdir(),
		`factory-dag-eval-${Date.now()}-${label}-${Math.random().toString(36).slice(2)}`,
	);
	mkdirSync(tempRoot, { recursive: true });
	const authStorage = AuthStorage.create(join(realAgentDir, "auth.json"));
	const modelRegistry = ModelRegistry.create(authStorage, join(realAgentDir, "models.json"));
	const settingsManager = SettingsManager.create(tempRoot, tempRoot);
	const sessionManager = SessionManager.create(tempRoot, join(tempRoot, "sessions"));
	const [provider, ...modelIdParts] = config.model.split("/");
	const model = modelRegistry.find(provider, modelIdParts.join("/"));
	if (!model) throw new Error(`Model ${config.model} not found in the registry`);
	const { session } = await createAgentSession({
		cwd: tempRoot,
		authStorage,
		modelRegistry,
		settingsManager,
		sessionManager,
		model,
		includeGoals: false,
	});
	return { session, sessionManager, tempRoot };
}

function lastAssistantContextTokens(session: SessionBundle["session"]): number | null {
	const messages = session.agent.state.messages as unknown as Array<Record<string, unknown>>;
	for (let index = messages.length - 1; index >= 0; index--) {
		const message = messages[index];
		if (message.role !== "assistant") continue;
		if (message.stopReason === "aborted" || message.stopReason === "error") continue;
		const usage = message.usage as Parameters<typeof calculateContextTokens>[0] | undefined;
		if (usage) return calculateContextTokens(usage);
	}
	return null;
}

function messageTimestampMs(message: Record<string, unknown>): number | null {
	const timestamp = message.timestamp;
	if (typeof timestamp === "number") return timestamp;
	if (typeof timestamp === "string") {
		const parsed = Date.parse(timestamp);
		return Number.isNaN(parsed) ? null : parsed;
	}
	if (timestamp instanceof Date) return timestamp.getTime();
	return null;
}

/** Resident teardown latency: the finished notice to the parent's final answer. */
function measureTeardownLatency(session: SessionBundle["session"], runId: string | null): number | null {
	if (!runId) return null;
	const messages = session.agent.state.messages as unknown as Array<Record<string, unknown>>;
	let finishedAt: number | null = null;
	for (const message of messages) {
		if (message.customType !== FACTORY_PROGRESS_NOTICE_CUSTOM_TYPE) continue;
		const content = typeof message.content === "string" ? message.content : "";
		if (!content.includes("[factory-progress") || !content.includes("finished")) continue;
		if (runId && !content.includes(runId)) continue;
		finishedAt = messageTimestampMs(message);
	}
	let answerAt: number | null = null;
	for (let index = messages.length - 1; index >= 0; index--) {
		if (messages[index].role !== "assistant") continue;
		answerAt = messageTimestampMs(messages[index]);
		break;
	}
	return finishedAt !== null && answerAt !== null ? Math.max(0, answerAt - finishedAt) : null;
}

function readLedger(ledgerPath: string): FactoryStatusLedger | null {
	if (!existsSync(ledgerPath)) return null;
	try {
		return JSON.parse(readFileSync(ledgerPath, "utf8")) as FactoryStatusLedger;
	} catch {
		return null;
	}
}

function readBaselineLedger(ledgerPath: string): Record<string, unknown> | null {
	if (!existsSync(ledgerPath)) return null;
	try {
		return JSON.parse(readFileSync(ledgerPath, "utf8")) as Record<string, unknown>;
	} catch {
		return null;
	}
}

async function promptWithTimeout(session: SessionBundle["session"], prompt: string, timeoutMs: number): Promise<void> {
	let timer: ReturnType<typeof setTimeout> | undefined;
	try {
		await Promise.race([
			session.prompt(prompt),
			new Promise((_, reject) => {
				timer = setTimeout(() => reject(new Error(`trial prompt timed out after ${timeoutMs} ms`)), timeoutMs);
			}),
		]);
	} finally {
		if (timer !== undefined) clearTimeout(timer);
	}
}

async function runFactoryTrial(
	config: EvalConfig,
	factory: ReferenceFactory,
	trial: number,
): Promise<FactoryDagEvalTrialResult> {
	const startedAt = Date.now();
	const bundle = await createEvalSession(config, factory.kind);
	const ledgerPath = join(bundle.tempRoot, "ledger.json");
	seedHarnessState(bundle.sessionManager, [factory]);
	const prompt = buildFactoryParentPrompt(factory, ledgerPath);
	try {
		const problems: string[] = [];
		try {
			await promptWithTimeout(bundle.session, prompt, config.timeoutMinutes * 60_000);
		} catch (error) {
			problems.push(`parent turn failed: ${error instanceof Error ? error.message : String(error)}`);
		}
		const answer = parseAnswerLine(bundle.session.getLastAssistantText());
		const ledger = readLedger(ledgerPath);
		const check = checkTaskSuccess(factory, answer, ledger);
		const replay = ledger !== null ? checkReplayLedger(ledger) : null;
		problems.push(...check.problems);
		if (ledger === null && factory.kind !== "dry-run-reject") {
			problems.push("status ledger was not written");
		}
		const contextTokens = lastAssistantContextTokens(bundle.session);
		const stats = bundle.session.getSessionStats();
		const elapsedMs = ledger?.elapsed_ms ?? null;
		const teardownLatencyMs =
			factory.kind === "resident-watcher" ? measureTeardownLatency(bundle.session, ledger?.run_id ?? null) : null;
		const budgetOvershootMs = elapsedMs !== null ? Math.max(0, elapsedMs - factory.declaredBudgetMs) : 0;
		const taskSuccess = check.ok;
		return {
			factory: factory.kind,
			arm: "factory",
			trial,
			model: config.model,
			taskSuccess,
			problems,
			state: ledger?.state ?? answer?.state ?? null,
			wallMs: Date.now() - startedAt,
			contextTokens,
			totalTokens: stats.tokens.total,
			declaredFanIn: factory.declaredFanIn,
			queueLatencyMs: null,
			teardownLatencyMs,
			declaredBudgetMs: factory.declaredBudgetMs,
			budgetOvershootMs,
			elapsedMs,
			spawns: ledger?.usage.spawns ?? null,
			settled: ledger?.usage.settled ?? null,
			replayOk: replay?.ok ?? null,
			replayProblems: replay?.problems ?? [],
			answer,
			ledger,
			verdict: taskSuccess && problems.length === 0 && (replay?.ok ?? true) ? "pass" : "fail",
		};
	} finally {
		await bundle.session.dispose();
		rmSync(bundle.tempRoot, { recursive: true, force: true });
	}
}

async function runBaselineTrial(
	config: EvalConfig,
	factory: ReferenceFactory,
	trial: number,
): Promise<FactoryDagEvalTrialResult> {
	const startedAt = Date.now();
	const bundle = await createEvalSession(config, `${factory.kind}-baseline`);
	const ledgerPath = join(bundle.tempRoot, "ledger.json");
	const prompt = buildBaselinePrompt(factory, ledgerPath);
	try {
		const problems: string[] = [];
		try {
			await promptWithTimeout(bundle.session, prompt, config.timeoutMinutes * 60_000);
		} catch (error) {
			problems.push(`parent turn failed: ${error instanceof Error ? error.message : String(error)}`);
		}
		const answer = parseAnswerLine(bundle.session.getLastAssistantText());
		const check = checkTaskSuccess(factory, answer, null);
		problems.push(...check.problems);
		const baselineLedger = readBaselineLedger(ledgerPath);
		if (baselineLedger === null) problems.push("baseline collect ledger missing (not fatal, recorded)");
		const contextTokens = lastAssistantContextTokens(bundle.session);
		const stats = bundle.session.getSessionStats();
		const wallMs = Date.now() - startedAt;
		// Wall clock vs declared run budget: an upper-bound overshoot for the arm.
		const budgetOvershootMs = Math.max(0, wallMs - factory.declaredBudgetMs);
		return {
			factory: factory.kind,
			arm: "baseline",
			trial,
			model: config.model,
			taskSuccess: check.ok,
			problems,
			state: answer?.state ?? null,
			wallMs,
			contextTokens,
			totalTokens: stats.tokens.total,
			declaredFanIn: factory.declaredFanIn,
			queueLatencyMs: null,
			teardownLatencyMs: null,
			declaredBudgetMs: factory.declaredBudgetMs,
			budgetOvershootMs,
			elapsedMs: null,
			spawns: null,
			settled: null,
			replayOk: null,
			replayProblems: [],
			answer,
			ledger: null,
			verdict: check.ok ? "pass" : "fail",
		};
	} finally {
		await bundle.session.dispose();
		rmSync(bundle.tempRoot, { recursive: true, force: true });
	}
}

async function main(argv: string[] = process.argv.slice(2)): Promise<void> {
	const replayIndex = argv.indexOf("--replay");
	if (replayIndex !== -1) {
		const replayPath = argv[replayIndex + 1];
		if (!replayPath) {
			console.error("--replay requires a path to a report.json or a single status ledger");
			process.exit(1);
		}
		let data: unknown;
		try {
			data = JSON.parse(readFileSync(replayPath, "utf8"));
		} catch (error) {
			console.error(`cannot read replay input: ${error instanceof Error ? error.message : String(error)}`);
			process.exit(1);
		}
		const replay = runReplayChecks(data);
		for (const entry of replay.ledgers) {
			console.log(`replay ${entry.id}: ${entry.result.ok ? "ok" : "failed"}`);
			for (const problem of entry.result.problems) console.log(`  - ${problem}`);
		}
		console.log(replay.ok ? "all ledgers replay cleanly" : "replay check failed");
		process.exit(replay.ok ? 0 : 1);
	}
	const config = parseEvalArgs(argv);
	if ("error" in config) {
		console.error(config.error === "help" ? "See the header of this file for usage." : config.error);
		process.exit(config.error === "help" ? 0 : 1);
	}
	const referenceFactories = buildReferenceFactories(config.width);
	const results: FactoryDagEvalTrialResult[] = [];
	for (const selection of config.factories) {
		const factory = findReferenceFactory(referenceFactories, selection);
		for (let trial = 1; trial <= config.trials; trial++) {
			console.log(`running ${factory.kind} factory trial ${trial}/${config.trials} on ${config.model}`);
			try {
				results.push(await runFactoryTrial(config, factory, trial));
			} catch (error) {
				console.error(`factory trial failed: ${error instanceof Error ? error.message : String(error)}`);
			}
			console.log(`running ${factory.kind} baseline trial ${trial}/${config.trials} on ${config.model}`);
			try {
				results.push(await runBaselineTrial(config, factory, trial));
			} catch (error) {
				console.error(`baseline trial failed: ${error instanceof Error ? error.message : String(error)}`);
			}
		}
	}
	const escalationFactory = findReferenceFactory(referenceFactories, "review-sweep-fail");
	console.log("running review-sweep escalation probe (one trial)");
	try {
		results.push(await runFactoryTrial(config, escalationFactory, 1));
	} catch (error) {
		console.error(`escalation trial failed: ${error instanceof Error ? error.message : String(error)}`);
	}
	const brokenFactory = findReferenceFactory(referenceFactories, "dry-run-reject");
	console.log("running dry-run rejection probe (one trial)");
	try {
		results.push(await runFactoryTrial(config, brokenFactory, 1));
	} catch (error) {
		console.error(`dry-run trial failed: ${error instanceof Error ? error.message : String(error)}`);
	}
	if (results.length === 0) {
		console.error("no trials completed");
		process.exit(1);
	}
	const markdown = renderMarkdownReport(results, config);
	mkdirSync(config.outDir, { recursive: true });
	writeFileSync(join(config.outDir, "report.md"), markdown);
	writeFileSync(
		join(config.outDir, "report.json"),
		JSON.stringify(
			{ config, generatedAt: new Date().toISOString(), results, verdicts: computeVerdicts(results) },
			null,
			2,
		),
	);
	console.log(markdown);
	console.log(`reports written to ${config.outDir}`);
}

if (import.meta.url === `file://${process.argv[1]}`) {
	void main().catch((error: unknown) => {
		console.error(error);
		process.exit(1);
	});
}
