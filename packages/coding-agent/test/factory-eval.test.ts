import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import {
	BUILDER_MARKER,
	buildBaselinePrompt,
	buildBrokenDag,
	buildBuilderDag,
	buildFactoryParentPrompt,
	buildHarnessStateFile,
	buildReferenceFactories,
	buildResidentWatcherDag,
	buildReviewSweepDag,
	buildReviewSweepFailDag,
	checkReplayLedger,
	checkTaskSuccess,
	type EvalConfig,
	type FactoryDagEvalTrialResult,
	type FactoryLedgerEvent,
	type FactoryLedgerInstance,
	type FactoryLedgerNode,
	type FactoryStatusLedger,
	type ParsedAnswer,
	parseAnswerLine,
	parseEvalArgs,
	REVIEW_FILES,
	REVIEW_ISSUE_IDS,
	renderMarkdownReport,
	runReplayChecks,
} from "../scripts/factory-eval.js";
import { loadHarnessState } from "../src/core/refinement/refinement.js";

const WIDTH = 4;
const referenceFactories = buildReferenceFactories(WIDTH);
const byKind = (kind: string) => {
	const factory = referenceFactories.find((entry) => entry.kind === kind);
	if (!factory) throw new Error(`missing reference factory ${kind}`);
	return factory;
};

describe("reference factory shapes", () => {
	it("builds the review sweep with typed fan-in and a bounded foreach", () => {
		const dag = buildReviewSweepDag();
		expect(dag.nodes.map((node) => node.id)).toEqual(["files", "review", "report"]);
		expect(dag.run).toEqual({ budget_ms: 900_000, failure_policy: "escalate", max_parallel: 8 });
		const [files, review, report] = dag.nodes;
		expect(files.outputs).toEqual([{ name: "files", type: "json" }]);
		expect(files.budget_ms).toBe(240_000);
		expect(review.inputs).toEqual([{ name: "files", type: "json", from: "files.files" }]);
		expect(review.foreach).toEqual({ over: "files", max: 8 });
		expect(review.outputs).toEqual([{ name: "found", type: "text" }]);
		expect(report.inputs).toEqual([
			{ name: "file_list", type: "json", from: "files.files" },
			{ name: "found", type: "text", from: "review.found" },
		]);
		expect(report.outputs).toEqual([{ name: "issues", type: "json" }]);
		for (const node of dag.nodes) {
			expect(node.id).toMatch(/^[a-z0-9][a-z0-9-]{0,63}$/);
			expect(typeof node.subagent).toBe("object");
		}
	});

	it("plants one real, checkable issue per review file", () => {
		expect(REVIEW_FILES).toHaveLength(4);
		expect(REVIEW_ISSUE_IDS).toEqual(["AUDIT-A1", "AUDIT-B1", "AUDIT-C1", "AUDIT-D1"]);
		for (const file of REVIEW_FILES) {
			expect(file.code.length).toBeGreaterThan(20);
			expect(file.audit.length).toBeGreaterThan(20);
			expect(file.code).not.toContain(file.issueId);
		}
	});

	it("plants a failing reviewer that cannot be admitted (escalation variant)", () => {
		const dag = buildReviewSweepFailDag();
		const broken = dag.nodes.find((node) => node.id === "review-broken");
		expect(broken).toBeDefined();
		expect(broken?.depends_on).toEqual(["files"]);
		expect(broken?.failure_policy).toBe("escalate");
		if (typeof broken?.subagent === "object") {
			expect(broken.subagent.model).toBe("internal/no-such-model-for-eval");
			expect(broken.subagent.prompt).toContain("AUDIT-A1");
		} else {
			throw new Error("review-broken must use an inline subagent");
		}
		expect(buildReviewSweepDag().nodes.map((node) => node.id)).not.toContain("review-broken");
	});

	it("builds the N-wide builder with per-node budgets and a typed fan-in collector", () => {
		const dag = buildBuilderDag(WIDTH);
		expect(dag.nodes.map((node) => node.id)).toEqual([
			...Array.from({ length: WIDTH }, (_, i) => `builder-${i + 1}`),
			"collector",
		]);
		const collector = dag.nodes[dag.nodes.length - 1];
		expect(collector.inputs).toEqual(
			Array.from({ length: WIDTH }, (_, i) => ({
				name: `line-${i + 1}`,
				type: "text",
				from: `builder-${i + 1}.line`,
			})),
		);
		for (const node of dag.nodes) expect(node.budget_ms).toBe(240_000);
		for (let i = 1; i <= WIDTH; i++) {
			const builder = dag.nodes[i - 1];
			expect(builder?.subagent).toBeTypeOf("object");
			if (typeof builder?.subagent === "object") {
				expect(builder.subagent.prompt).toContain(`BUILT ${BUILDER_MARKER(i)}`);
			}
		}
	});

	it("builds the resident watcher with a resident head and a task chain", () => {
		const dag = buildResidentWatcherDag();
		const [watcher, taskA, taskB] = dag.nodes;
		expect(watcher?.lifecycle).toBe("resident");
		expect(watcher?.outputs).toBeUndefined();
		expect(watcher?.foreach).toBeUndefined();
		expect(watcher?.depends_on).toBeUndefined();
		expect(watcher?.budget_ms).toBeUndefined();
		expect(taskA?.outputs).toEqual([{ name: "step", type: "text" }]);
		expect(taskB?.inputs).toEqual([{ name: "prev", type: "text", from: "task-a.step" }]);
		expect(taskB?.lifecycle ?? "task").toBe("task");
		expect(taskA?.budget_ms).toBe(240_000);
	});

	it("builds the broken spec as a structurally valid but unresolvable reference", () => {
		const dag = buildBrokenDag();
		expect(dag.nodes).toHaveLength(1);
		expect(dag.nodes[0]?.subagent).toBe("no-such-subagent-entry");
	});
});

describe("prompt invariants", () => {
	it("factory parent prompts contain no task-specific orchestration code", () => {
		for (const factory of referenceFactories) {
			const prompt = buildFactoryParentPrompt(factory, "/tmp/ledger.json");
			expect(prompt).toContain(`rlm.factory.run('${factory.id}')`);
			expect(prompt).not.toMatch(/rlm\.spawn/);
			expect(prompt).not.toMatch(/rlm\.collect/);
		}
	});

	it("baseline prompts orchestrate manually and never touch rlm.factory", () => {
		for (const kind of ["review-sweep", "builder", "resident-watcher"] as const) {
			const prompt = buildBaselinePrompt(byKind(kind), "/tmp/ledger.json");
			expect(prompt).toMatch(/rlm\.spawn/);
			expect(prompt).toMatch(/rlm\.collect/);
			expect(prompt).not.toMatch(/rlm\.factory/);
			expect(prompt).toContain("Declared budget");
		}
	});

	it("the reviewer template carries the foreach placeholder and every audit id", () => {
		const factory = byKind("review-sweep");
		const review = factory.dag.nodes.find((node) => node.id === "review");
		if (typeof review?.subagent !== "object") throw new Error("review must be inline");
		expect(review.subagent.prompt).toContain("{files}");
		for (const file of REVIEW_FILES) {
			expect(review.subagent.prompt).toContain(file.code);
			expect(review.subagent.prompt).toContain(file.issueId);
		}
	});

	it("the resident watcher prompt replies once and holds its turn open", () => {
		const factory = byKind("resident-watcher");
		const watcher = factory.dag.nodes.find((node) => node.id === "watcher");
		if (typeof watcher?.subagent !== "object") throw new Error("watcher must be inline");
		expect(watcher.subagent.prompt).toContain("agent_message.send");
		expect(watcher.subagent.prompt).toContain("asyncio.sleep(900)");
		expect(watcher.subagent.prompt).toContain("Do not end your turn");
	});
});

describe("harness state seeding", () => {
	it("seeds factory entries the TS host can load back", () => {
		const tempDir = mkdtempSync(join(tmpdir(), "factory-dag-eval-seed-"));
		try {
			const stateDir = join(tempDir, "harness");
			mkdirSync(stateDir, { recursive: true });
			writeFileSync(join(stateDir, "harness_state.json"), buildHarnessStateFile([byKind("review-sweep")]));
			const state = loadHarnessState(stateDir, "local");
			const entry = state.entries.factory["factory-dag-eval-review-sweep"];
			expect(entry).toBeDefined();
			expect(entry?.kind).toBe("factory");
			expect(entry?.scope).toBe("local");
			expect((entry?.arguments.dag as { nodes: { id: string }[] }).nodes.map((node) => node.id)).toEqual([
				"files",
				"review",
				"report",
			]);
			expect(state.entries.prompt).toEqual({});
			expect(state.entries.subagent).toEqual({});
		} finally {
			rmSync(tempDir, { recursive: true, force: true });
		}
	});

	it("writes one entry per seeded spec with the local schema", () => {
		const file = JSON.parse(buildHarnessStateFile(referenceFactories));
		expect(file.schema).toBe(1);
		expect(Object.keys(file.entries.factory)).toHaveLength(referenceFactories.length);
		expect(file.refinements).toEqual([]);
		for (const spec of referenceFactories) {
			expect(file.entries.factory[spec.id]?.kind).toBe("factory");
			expect(file.entries.factory[spec.id]?.arguments.dag).toEqual(spec.dag);
		}
	});
});

describe("answer parsing and task-success checks", () => {
	const answer = (overrides: Partial<ParsedAnswer>): ParsedAnswer => ({
		issues: [],
		markers: [],
		state: null,
		stopped: [],
		failedNode: null,
		reportStatus: null,
		rejected: null,
		children: null,
		message: null,
		...overrides,
	});

	it("parses every ANSWER format", () => {
		expect(parseAnswerLine("junk\nANSWER: ISSUES: AUDIT-A1, AUDIT-B1, AUDIT-C1, AUDIT-D1; STATE: done")).toEqual(
			answer({ issues: REVIEW_ISSUE_IDS, state: "done" }),
		);
		expect(parseAnswerLine("ANSWER: MARKERS: swb-marker-1,swb-marker-2; STOPPED: watcher; STATE: stopped")).toEqual(
			answer({ markers: ["swb-marker-1", "swb-marker-2"], stopped: ["watcher"], state: "stopped" }),
		);
		expect(parseAnswerLine("ANSWER: STATE: paused; FAILED-NODE: review-broken; REPORT-STATUS: pending")).toEqual(
			answer({ state: "paused", failedNode: "review-broken", reportStatus: "pending" }),
		);
		expect(
			parseAnswerLine(
				"ANSWER: REJECTED: yes; CHILDREN: 0; MESSAGE: node 'broken-source' references unknown subagent 'no-such-subagent-entry'",
			),
		).toEqual(
			answer({
				rejected: true,
				children: 0,
				message: "node 'broken-source' references unknown subagent 'no-such-subagent-entry'",
			}),
		);
		expect(parseAnswerLine("no answer here")).toBeNull();
		expect(parseAnswerLine(undefined)).toBeNull();
	});

	it("accepts a complete review sweep and rejects a missing planted issue", () => {
		const good = answer({ issues: REVIEW_ISSUE_IDS, state: "done" });
		expect(checkTaskSuccess(byKind("review-sweep"), good, null).ok).toBe(true);
		const missing = answer({ issues: REVIEW_ISSUE_IDS.slice(1), state: "done" });
		const check = checkTaskSuccess(byKind("review-sweep"), missing, null);
		expect(check.ok).toBe(false);
		expect(check.problems[0]).toContain("AUDIT-A1 missing");
	});

	it("cross-checks the review sweep ledger against the aggregator preview", () => {
		const ledger = statusLedgerFixture({
			state: "done",
			nodes: [
				nodeFixture("files", "done"),
				nodeFixture("review", "done"),
				nodeFixture("report", "done", {
					answer_preview: '```json\n{"issues": ["AUDIT-A1","AUDIT-B2"]}\n```',
				}),
			],
		});
		const good = answer({ issues: REVIEW_ISSUE_IDS, state: "done" });
		const check = checkTaskSuccess(byKind("review-sweep"), good, ledger);
		expect(check.ok).toBe(false);
		expect(check.problems.some((problem) => problem.includes("AUDIT-B1"))).toBe(true);
	});

	it("checks the builder markers against the collector preview", () => {
		const factory = byKind("builder");
		const markers = Array.from({ length: WIDTH }, (_, i) => BUILDER_MARKER(i + 1));
		expect(checkTaskSuccess(factory, answer({ markers, state: "done" }), null).ok).toBe(true);
		const ledger = statusLedgerFixture({
			state: "done",
			nodes: [nodeFixture("collector", "done", { answer_preview: "COLLECTED swb-marker-1" })],
		});
		const check = checkTaskSuccess(factory, answer({ markers, state: "done" }), ledger);
		expect(check.ok).toBe(false);
		expect(check.problems.some((problem) => problem.includes("swb-marker-2"))).toBe(true);
	});

	it("checks the resident teardown: tasks settled, watcher cancelled", () => {
		const factory = byKind("resident-watcher");
		const good = answer({ markers: ["swt-1", "swt-2"], stopped: ["watcher"], state: "stopped" });
		expect(checkTaskSuccess(factory, good, null).ok).toBe(true);
		expect(
			checkTaskSuccess(factory, answer({ markers: ["swt-1", "swt-2"], stopped: [], state: "stopped" }), null).ok,
		).toBe(false);
		const ledger = statusLedgerFixture({
			spec_id: "factory-dag-eval-resident-watcher",
			state: "stopped",
			nodes: [
				nodeFixture("watcher", "cancelled", {
					lifecycle: "resident",
					instances: [instanceFixture(-1, "cancelled")],
				}),
				nodeFixture("task-a", "done", { instances: [instanceFixture(-1, "done", { duration_ms: 5_000 })] }),
				nodeFixture("task-b", "done", { instances: [instanceFixture(-1, "done", { duration_ms: 4_000 })] }),
			],
		});
		expect(checkTaskSuccess(factory, good, ledger).ok).toBe(true);
		const badLedger = statusLedgerFixture({
			state: "done",
			nodes: [
				nodeFixture("watcher", "running", { lifecycle: "resident" }),
				nodeFixture("task-a", "done"),
				nodeFixture("task-b", "done"),
			],
		});
		const check = checkTaskSuccess(factory, good, badLedger);
		expect(check.ok).toBe(false);
		expect(check.problems.some((problem) => problem.includes("expected stopped"))).toBe(true);
		expect(check.problems.some((problem) => problem.includes("watcher node is not cancelled"))).toBe(true);
	});

	it("checks the escalation probe answer and ledger", () => {
		const factory = byKind("review-sweep-fail");
		const good = answer({ state: "paused", failedNode: "review-broken", reportStatus: "pending" });
		expect(checkTaskSuccess(factory, good, null).ok).toBe(true);
		const ledger = statusLedgerFixture({
			state: "paused",
			nodes: [
				nodeFixture("files", "done"),
				nodeFixture("review", "running", {
					instances: [instanceFixture(0, "running"), instanceFixture(1, "running")],
				}),
				nodeFixture("review-broken", "error", { error: "spawn admission failed: no such model" }),
				nodeFixture("report", "pending", { instances: [] }),
			],
			events: [
				eventFixture(1, "run_started"),
				eventFixture(2, "milestone", { milestone: "paused" }),
				eventFixture(3, "spawned", { node: "report" }),
			],
		});
		const check = checkTaskSuccess(factory, good, ledger);
		expect(check.ok).toBe(false);
		expect(check.problems).toContain("report node started despite the escalation pause");
	});

	it("checks the dry-run rejection answer", () => {
		const factory = byKind("dry-run-reject");
		const good = answer({
			rejected: true,
			children: 0,
			message: "node 'broken-source' references unknown subagent 'no-such-subagent-entry'",
		});
		expect(checkTaskSuccess(factory, good, null).ok).toBe(true);
		const bad = answer({ rejected: false, children: 2, message: "no error" });
		expect(checkTaskSuccess(factory, bad, null).ok).toBe(false);
		expect(checkTaskSuccess(factory, null, null).problems).toContain("no ANSWER line in the parent's final text");
	});
});

// ---------------------------------------------------------------------------
// Replay checker fixtures.
// ---------------------------------------------------------------------------

function eventFixture(seq: number, kind: string, extra: Partial<FactoryLedgerEvent> = {}): FactoryLedgerEvent {
	return { seq, kind, stage: "delivered", ...extra };
}

function instanceFixture(
	index: number,
	status: string,
	extra: Partial<FactoryLedgerInstance> = {},
): FactoryLedgerInstance {
	return { index, status, attempt: 1, child: "child-1", duration_ms: null, ...extra };
}

function nodeFixture(id: string, status: string, extra: Partial<FactoryLedgerNode> = {}): FactoryLedgerNode {
	return { id, status, lifecycle: "task", attempts: 1, instances: [instanceFixture(-1, status)], ...extra };
}

function statusLedgerFixture(overrides: Partial<FactoryStatusLedger> = {}): FactoryStatusLedger {
	return {
		run_id: "run-1",
		spec_id: "factory-dag-eval-review-sweep",
		name: null,
		state: "done",
		nodes: [],
		events: [],
		elapsed_ms: 12_345,
		usage: { spawns: 0, settled: 0, tool_uses: 0, max_parallel: 8, running: 0 },
		...overrides,
	};
}

function reviewSweepLedger(): FactoryStatusLedger {
	return statusLedgerFixture({
		state: "done",
		nodes: [
			nodeFixture("files", "done", { instances: [instanceFixture(-1, "done", { duration_ms: 4_000 })] }),
			nodeFixture("review", "done", {
				instances: REVIEW_FILES.map((_, index) => instanceFixture(index, "done", { duration_ms: 8_000 })),
			}),
			nodeFixture("report", "done", {
				instances: [instanceFixture(-1, "done", { duration_ms: 3_000 })],
				answer_preview: '```json\n{"issues": ["AUDIT-A1","AUDIT-B1","AUDIT-C1","AUDIT-D1"]}\n```',
			}),
		],
		events: [
			eventFixture(1, "run_started", { detail: "3 nodes, max_parallel 8" }),
			eventFixture(2, "node_ready", { node: "files" }),
			eventFixture(3, "spawned", { node: "files", instance: -1 }),
			eventFixture(4, "settled", { node: "files", instance: -1, status: "done", duration_ms: 4_000 }),
			eventFixture(5, "answer_captured", { node: "files", instance: -1 }),
			eventFixture(6, "node_ready", { node: "review" }),
			...REVIEW_FILES.flatMap((_, index) => [
				eventFixture(7 + index * 3, "spawned", { node: "review", instance: index }),
				eventFixture(8 + index * 3, "settled", {
					node: "review",
					instance: index,
					status: "done",
					duration_ms: 8_000,
				}),
				eventFixture(9 + index * 3, "answer_captured", { node: "review", instance: index }),
			]),
			eventFixture(19, "node_ready", { node: "report" }),
			eventFixture(20, "spawned", { node: "report", instance: -1 }),
			eventFixture(21, "settled", { node: "report", instance: -1, status: "done", duration_ms: 3_000 }),
			eventFixture(22, "answer_captured", { node: "report", instance: -1 }),
			eventFixture(23, "milestone", { milestone: "finished" }),
		],
		usage: { spawns: 6, settled: 6, tool_uses: 6, max_parallel: 8, running: 0 },
	});
}

function residentLedger(): FactoryStatusLedger {
	return statusLedgerFixture({
		spec_id: "factory-dag-eval-resident-watcher",
		state: "stopped",
		nodes: [
			nodeFixture("watcher", "cancelled", {
				lifecycle: "resident",
				instances: [instanceFixture(-1, "cancelled")],
			}),
			nodeFixture("task-a", "done", { instances: [instanceFixture(-1, "done", { duration_ms: 5_000 })] }),
			nodeFixture("task-b", "done", { instances: [instanceFixture(-1, "done", { duration_ms: 4_000 })] }),
		],
		events: [
			eventFixture(1, "run_started"),
			eventFixture(2, "node_ready", { node: "watcher" }),
			eventFixture(3, "spawned", { node: "watcher", instance: -1 }),
			eventFixture(4, "node_ready", { node: "task-a" }),
			eventFixture(5, "spawned", { node: "task-a", instance: -1 }),
			eventFixture(6, "settled", { node: "task-a", instance: -1, status: "done", duration_ms: 5_000 }),
			eventFixture(7, "answer_captured", { node: "task-a", instance: -1 }),
			eventFixture(8, "node_ready", { node: "task-b" }),
			eventFixture(9, "spawned", { node: "task-b", instance: -1 }),
			eventFixture(10, "settled", { node: "task-b", instance: -1, status: "done", duration_ms: 4_000 }),
			eventFixture(11, "answer_captured", { node: "task-b", instance: -1 }),
			eventFixture(12, "milestone", { milestone: "finished" }),
			eventFixture(13, "node_cancelled", { node: "watcher" }),
			eventFixture(14, "cancelled", { node: "watcher", instance: -1 }),
			eventFixture(15, "run_stopped", { detail: "stopped; 1 node(s) cancelled" }),
		],
		usage: { spawns: 3, settled: 2, tool_uses: 2, max_parallel: 8, running: 0 },
	});
}

function escalationLedger(): FactoryStatusLedger {
	return statusLedgerFixture({
		spec_id: "factory-dag-eval-review-fail",
		state: "paused",
		nodes: [
			nodeFixture("files", "done", { instances: [instanceFixture(-1, "done", { duration_ms: 4_000 })] }),
			nodeFixture("review", "running", {
				instances: [
					instanceFixture(0, "running"),
					instanceFixture(1, "running"),
					instanceFixture(2, "running"),
					instanceFixture(3, "running"),
				],
			}),
			nodeFixture("review-broken", "error", {
				instances: [instanceFixture(-1, "error", { error: "spawn admission failed: no such model" })],
				error: "spawn admission failed: no such model",
			}),
			nodeFixture("report", "pending", { instances: [] }),
		],
		events: [
			eventFixture(1, "run_started"),
			eventFixture(2, "node_ready", { node: "files" }),
			eventFixture(3, "spawned", { node: "files", instance: -1 }),
			eventFixture(4, "settled", { node: "files", instance: -1, status: "done", duration_ms: 4_000 }),
			eventFixture(5, "node_ready", { node: "review" }),
			eventFixture(6, "spawned", { node: "review", instance: 0 }),
			eventFixture(7, "spawned", { node: "review", instance: 1 }),
			eventFixture(8, "spawned", { node: "review", instance: 2 }),
			eventFixture(9, "spawned", { node: "review", instance: 3 }),
			eventFixture(10, "node_ready", { node: "review-broken" }),
			eventFixture(11, "settled", {
				node: "review-broken",
				instance: -1,
				status: "error",
				error: "spawn admission failed: no such model",
			}),
			eventFixture(12, "node_error", { node: "review-broken", error: "spawn admission failed" }),
			eventFixture(13, "milestone", { milestone: "paused" }),
		],
		usage: { spawns: 5, settled: 1, tool_uses: 1, max_parallel: 8, running: 4 },
	});
}

describe("replay checker", () => {
	it("accepts the three reference ledgers with stable identities and full accounting", () => {
		for (const ledger of [reviewSweepLedger(), residentLedger(), escalationLedger()]) {
			const result = checkReplayLedger(ledger);
			expect(result.problems).toEqual([]);
			expect(result.ok).toBe(true);
		}
	});

	it("rejects a ledger that does not match the status shape", () => {
		const result = checkReplayLedger({ hello: "world" });
		expect(result.ok).toBe(false);
		expect(result.problems[0]).toContain("shape");
	});

	it("flags a settled-done event without a duration", () => {
		const ledger = reviewSweepLedger();
		const settled = ledger.events.find((event) => event.kind === "settled");
		if (!settled) throw new Error("fixture lost its settled event");
		settled.duration_ms = undefined;
		const result = checkReplayLedger(ledger);
		expect(result.ok).toBe(false);
		expect(result.problems.some((problem) => problem.includes("without a duration_ms"))).toBe(true);
	});

	it("flags non-increasing and duplicate event seqs", () => {
		const ledger = reviewSweepLedger();
		ledger.events[1]!.seq = 1;
		const result = checkReplayLedger(ledger);
		expect(result.ok).toBe(false);
		expect(result.problems.some((problem) => problem.includes("non-increasing seq"))).toBe(true);
	});

	it("flags unknown event kinds and stages", () => {
		const ledger = reviewSweepLedger();
		ledger.events[0] = eventFixture(1, "teleported");
		const kindResult = checkReplayLedger(ledger);
		expect(kindResult.problems.some((problem) => problem.includes("unknown kind"))).toBe(true);
		const staged = reviewSweepLedger();
		staged.events[0] = eventFixture(1, "run_started", { stage: "vaporized" });
		const stageResult = checkReplayLedger(staged);
		expect(stageResult.problems.some((problem) => problem.includes("unknown stage"))).toBe(true);
	});

	it("flags usage counts that disagree with the event stream", () => {
		const ledger = reviewSweepLedger();
		ledger.usage = { spawns: 99, settled: 6, tool_uses: 6, max_parallel: 8, running: 0 };
		const result = checkReplayLedger(ledger);
		expect(result.problems.some((problem) => problem.includes("usage.spawns"))).toBe(true);
	});

	it("flags a done run without a finished milestone", () => {
		const ledger = reviewSweepLedger();
		ledger.events = ledger.events.filter((event) => event.kind !== "milestone");
		const result = checkReplayLedger(ledger);
		expect(result.problems.some((problem) => problem.includes("finished milestone"))).toBe(true);
	});

	it("flags a spawned instance that never settles on a completed run", () => {
		const ledger = reviewSweepLedger();
		// Drop the report node's settle+answer events and mark it done: unaccounted spawn.
		ledger.events = ledger.events.filter(
			(event) => !(event.node === "report" && ["settled", "answer_captured"].includes(event.kind)),
		);
		const result = checkReplayLedger(ledger);
		expect(result.problems.some((problem) => problem.includes("never settled or cancelled"))).toBe(true);
	});

	it("notes a truncated event window instead of asserting counts", () => {
		const ledger = reviewSweepLedger();
		ledger.events = ledger.events.slice(10);
		ledger.events = ledger.events.map((event, index) => ({ ...event, seq: 11 + index }));
		const result = checkReplayLedger(ledger);
		expect(result.problems.some((problem) => problem.includes("truncated"))).toBe(true);
		expect(result.problems.some((problem) => problem.includes("usage.spawns"))).toBe(false);
	});

	it("runs over a saved report.json and a bare ledger", () => {
		const trial = (ledger: FactoryStatusLedger | null): FactoryDagEvalTrialResult =>
			({
				factory: "review-sweep",
				arm: "factory",
				trial: 1,
				ledger,
			}) as FactoryDagEvalTrialResult;
		const report = runReplayChecks({ trials: [trial(reviewSweepLedger()), trial(null)] });
		expect(report.ok).toBe(true);
		expect(report.ledgers).toHaveLength(1);
		const bare = runReplayChecks(residentLedger());
		expect(bare.ok).toBe(true);
		expect(bare.ledgers[0]?.id).toBe("ledger");
	});
});

describe("args and report rendering", () => {
	it("parses args with defaults and clamps the width", () => {
		const defaults = parseEvalArgs([]);
		expect(defaults).not.toHaveProperty("error");
		if ("error" in defaults) throw new Error("unreachable");
		expect(defaults.model).toBe("internal/glm-5.2-fast");
		expect(defaults.factories).toEqual(["review-sweep", "builder", "resident-watcher"]);
		expect(defaults.width).toBe(6);
		expect(defaults.outDir).toContain("factory-dag-eval-reports/");
		const clamped = parseEvalArgs(["--width", "99", "--factories", "builder,review-sweep", "--trials", "3"]);
		if ("error" in clamped) throw new Error("unreachable");
		expect(clamped.width).toBe(12);
		expect(clamped.factories).toEqual(["builder", "review-sweep"]);
		expect(clamped.trials).toBe(3);
		expect(parseEvalArgs(["--nope"])).toEqual({ error: "Unknown argument: --nope" });
	});

	it("renders the markdown table, pair comparison, and verdict rules", () => {
		const config: EvalConfig = {
			model: "internal/glm-5.2-fast",
			factories: ["review-sweep"],
			width: 4,
			trials: 1,
			timeoutMinutes: 20,
			outDir: "out",
		};
		const result = (
			arm: "factory" | "baseline",
			contextTokens: number | null,
			ok: boolean,
		): FactoryDagEvalTrialResult => ({
			factory: "review-sweep",
			arm,
			trial: 1,
			model: config.model,
			taskSuccess: ok,
			problems: ok ? [] : ["planted issue AUDIT-A1 missing from the ANSWER line"],
			state: "done",
			wallMs: 12_345,
			contextTokens,
			totalTokens: 9_999,
			declaredFanIn: 4,
			queueLatencyMs: null,
			teardownLatencyMs: null,
			declaredBudgetMs: 900_000,
			budgetOvershootMs: 0,
			elapsedMs: 100_000,
			spawns: 6,
			settled: 6,
			replayOk: arm === "factory" ? true : null,
			replayProblems: [],
			answer: null,
			ledger: null,
			verdict: ok ? "pass" : "fail",
		});
		const markdown = renderMarkdownReport(
			[result("factory", 10_000, true), result("baseline", 20_000, false)],
			config,
		);
		expect(markdown).toContain("# Factory DAG capability eval report");
		expect(markdown).toContain("| review-sweep | factory | 1 | ok | done |");
		expect(markdown).toContain("| review-sweep | baseline | 1 | failed | done |");
		expect(markdown).toContain("| review-sweep | 10000 | 20000 | yes | no |");
		expect(markdown).toContain("no task-specific orchestration code in factory prompts: PASS");
		expect(markdown).toContain("declared failure policy matches observed behavior (escalation): (not run)");
		expect(markdown).toContain("total budget overshoot: 0 ms (PASS)");
		expect(markdown).toContain("- review-sweep/baseline/trial 1: planted issue AUDIT-A1 missing");
	});
});
