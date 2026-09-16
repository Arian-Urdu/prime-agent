"""Validation helpers for swarm DAG specifications.

A continual-harness ``swarm`` entry stores a declarative DAG of subagent
nodes in ``arguments["dag"]``. This module implements the write-time dry
run: a validator that checks the whole graph before anything is stored, a
canonicalizer that applies defaults, and a stable topological sort used
for cycle detection. Execution (run/status/stop) lands in a follow-up PR;
nothing here spawns nodes.
"""

from __future__ import annotations

import copy
import heapq
import re
from typing import Any

FAILURE_POLICIES: tuple[str, ...] = ("fail_fast", "continue", "escalate")
PORT_TYPES: tuple[str, ...] = ("text", "json")
LIFECYCLES: tuple[str, ...] = ("task", "resident")
MAX_NODES = 1024
MAX_RETRIES = 10
MAX_PARALLEL_MIN = 1
MAX_PARALLEL_MAX = 64
FOREACH_MAX_MIN = 1
FOREACH_MAX_MAX = 256
RUN_FAILURE_POLICY_DEFAULT = "escalate"
RUN_MAX_PARALLEL_DEFAULT = 8
NODE_LIFECYCLE_DEFAULT = "task"
NODE_RETRIES_DEFAULT = 0

_NODE_ID_PATTERN = re.compile(r"[a-z0-9][a-z0-9-]{0,63}")


def _is_int(value: Any) -> bool:
    """True for real integers; booleans are not accepted as ints."""
    return isinstance(value, int) and not isinstance(value, bool)


def _is_positive_int(value: Any) -> bool:
    return _is_int(value) and value > 0


def _is_nonempty_str(value: Any) -> bool:
    return isinstance(value, str) and value != ""


def _valid_node_id(value: Any) -> bool:
    return _is_nonempty_str(value) and _NODE_ID_PATTERN.fullmatch(value) is not None


def _port_list(node: dict[str, Any], key: str) -> list[Any]:
    """Return the node's inputs/outputs list, or [] when absent or malformed."""
    raw = node.get(key)
    return raw if isinstance(raw, list) else []


def _port_names(node: dict[str, Any], key: str) -> list[Any]:
    return [entry.get("name") if isinstance(entry, dict) else None for entry in _port_list(node, key)]


def _declared_port_types(node: dict[str, Any], key: str) -> dict[str, str]:
    """Map port name to type for well-formed entries of the node's port list."""
    ports: dict[str, str] = {}
    for entry in _port_list(node, key):
        if isinstance(entry, dict):
            name, port_type = entry.get("name"), entry.get("type")
            if _is_nonempty_str(name) and port_type in PORT_TYPES:
                ports[name] = port_type
    return ports


def _input_sources(node: dict[str, Any]) -> list[str]:
    """Source node ids referenced by the node's inputs."""
    sources: list[str] = []
    for inp in _port_list(node, "inputs"):
        if not isinstance(inp, dict):
            continue
        source = inp.get("from")
        if isinstance(source, str) and "." in source:
            sources.append(source.partition(".")[0])
    return sources


def _cycle_check_nodes(nodes_by_id: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """Build a cleaned graph for the cycle check.

    Edges already reported as errors (unknown targets, self edges, and
    resident targets) are dropped so the cycle check reports each problem
    once instead of duplicating those errors.
    """
    def edge_ok(node_id: str, dep: str) -> bool:
        return (
            dep in nodes_by_id
            and dep != node_id
            and nodes_by_id[dep].get("lifecycle", NODE_LIFECYCLE_DEFAULT) != "resident"
        )

    cleaned: list[dict[str, Any]] = []
    for node_id, node in nodes_by_id.items():
        entry: dict[str, Any] = {"id": node_id}
        entry["depends_on"] = [dep for dep in _port_list(node, "depends_on") if edge_ok(node_id, dep)]
        kept_inputs = [
            {"from": inp["from"]}
            for inp in _port_list(node, "inputs")
            if isinstance(inp, dict)
            and isinstance(inp.get("from"), str)
            and "." in inp["from"]
            and edge_ok(node_id, inp["from"].partition(".")[0])
        ]
        if kept_inputs:
            entry["inputs"] = kept_inputs
        cleaned.append(entry)
    return cleaned


def validate_swarm_spec(dag: Any) -> list[str]:
    """Dry-run validation for a swarm DAG.

    Returns a list of human-readable error sentences; an empty list means
    the specification is valid. Every rule checked here is enforced before
    a swarm entry is stored, so an invalid DAG never reaches the store.
    """
    errors: list[str] = []
    if not isinstance(dag, dict):
        return ["swarm dag must be a JSON object"]

    run = dag.get("run")
    if run is not None:
        if not isinstance(run, dict):
            errors.append("run must be an object")
            run = None
    run_budget = run.get("budget_ms") if isinstance(run, dict) else None
    if run_budget is not None and not _is_positive_int(run_budget):
        errors.append("run budget_ms must be a positive integer")
        run_budget = None
    run_policy = run.get("failure_policy") if isinstance(run, dict) else None
    if run_policy is not None and run_policy not in FAILURE_POLICIES:
        errors.append(f"run failure_policy must be one of {list(FAILURE_POLICIES)}, got {run_policy!r}")
    max_parallel = run.get("max_parallel") if isinstance(run, dict) else None
    if max_parallel is not None and not (
        _is_int(max_parallel) and MAX_PARALLEL_MIN <= max_parallel <= MAX_PARALLEL_MAX
    ):
        errors.append(f"run max_parallel must be an integer between {MAX_PARALLEL_MIN} and {MAX_PARALLEL_MAX}")

    nodes = dag.get("nodes")
    if not isinstance(nodes, list):
        errors.append("swarm dag requires a nodes list")
        return errors
    if not 1 <= len(nodes) <= MAX_NODES:
        errors.append(f"swarm dag must declare between 1 and {MAX_NODES} nodes, got {len(nodes)}")
        return errors

    seen_ids: set[str] = set()
    nodes_by_id: dict[str, dict[str, Any]] = {}
    for index, node in enumerate(nodes):
        if not isinstance(node, dict):
            errors.append(f"nodes[{index}] must be an object")
            continue
        node_id = node.get("id")
        if not _is_nonempty_str(node_id):
            errors.append(f"nodes[{index}] requires a non-empty id")
        elif not _valid_node_id(node_id):
            errors.append(f"nodes[{index}] id must match ^[a-z0-9][a-z0-9-]{{0,63}}$, got {node_id!r}")
        elif node_id in seen_ids:
            errors.append(f"nodes[{index}] duplicates node id {node_id!r}")
        else:
            seen_ids.add(node_id)
            nodes_by_id[node_id] = node

    for node_id, node in nodes_by_id.items():
        ref = node_id
        lifecycle = node.get("lifecycle", NODE_LIFECYCLE_DEFAULT)
        if lifecycle not in LIFECYCLES:
            errors.append(f"node {ref} lifecycle must be 'task' or 'resident', got {lifecycle!r}")
        is_resident = lifecycle == "resident"

        subagent = node.get("subagent")
        if _is_nonempty_str(subagent):
            pass  # Harness subagent entry id or title; resolved at run time.
        elif isinstance(subagent, dict):
            if not _is_nonempty_str(subagent.get("prompt")):
                errors.append(f"node {ref} inline subagent requires a non-empty prompt")
            for key in ("name", "model", "thinking"):
                value = subagent.get(key)
                if value is not None and not _is_nonempty_str(value):
                    errors.append(f"node {ref} inline subagent {key} must be a non-empty string when provided")
        else:
            errors.append(
                f"node {ref} requires a subagent: a harness subagent id/title string "
                "or an inline object with a prompt"
            )

        budget = node.get("budget_ms")
        if budget is not None:
            if not _is_positive_int(budget):
                errors.append(f"node {ref} budget_ms must be a positive integer")
            elif run_budget is not None and budget > run_budget:
                errors.append(f"node {ref} budget_ms {budget} exceeds the run budget_ms {run_budget}")

        retries = node.get("retries")
        if retries is not None and not (_is_int(retries) and 0 <= retries <= MAX_RETRIES):
            errors.append(f"node {ref} retries must be an integer between 0 and {MAX_RETRIES}")

        policy = node.get("failure_policy")
        if policy is not None and policy not in FAILURE_POLICIES:
            errors.append(f"node {ref} failure_policy must be one of {list(FAILURE_POLICIES)}, got {policy!r}")

        outputs = node.get("outputs")
        if outputs is not None and not isinstance(outputs, list):
            errors.append(f"node {ref} outputs must be a list")
        elif is_resident and isinstance(outputs, list) and outputs:
            errors.append(f"resident node {ref} cannot declare outputs")
        reported_duplicate_outputs: set[str] = set()
        for index, out in enumerate(_port_list(node, "outputs")):
            if not isinstance(out, dict):
                errors.append(f"node {ref} outputs[{index}] must be an object")
                continue
            name, port_type = out.get("name"), out.get("type")
            if not _is_nonempty_str(name):
                errors.append(f"node {ref} outputs[{index}] requires a non-empty name")
            elif _port_names(node, "outputs").count(name) > 1 and name not in reported_duplicate_outputs:
                reported_duplicate_outputs.add(name)
                errors.append(f"node {ref} declares duplicate output name {name!r}")
            if port_type not in PORT_TYPES:
                errors.append(f"node {ref} output {name!r} type must be 'text' or 'json'")

        inputs = node.get("inputs")
        if inputs is not None and not isinstance(inputs, list):
            errors.append(f"node {ref} inputs must be a list")
        reported_duplicate_inputs: set[str] = set()
        for index, inp in enumerate(_port_list(node, "inputs")):
            if not isinstance(inp, dict):
                errors.append(f"node {ref} inputs[{index}] must be an object")
                continue
            name, port_type, source = inp.get("name"), inp.get("type"), inp.get("from")
            if not _is_nonempty_str(name):
                errors.append(f"node {ref} inputs[{index}] requires a non-empty name")
            elif _port_names(node, "inputs").count(name) > 1 and name not in reported_duplicate_inputs:
                reported_duplicate_inputs.add(name)
                errors.append(f"node {ref} declares duplicate input name {name!r}")
            if port_type not in PORT_TYPES:
                errors.append(f"node {ref} input {name!r} type must be 'text' or 'json'")
            if not isinstance(source, str) or "." not in source:
                errors.append(
                    f"node {ref} input {name!r} requires a 'from' reference of the form '<node_id>.<output_name>'"
                )
                continue
            src_id, _, src_output = source.partition(".")
            if src_id not in nodes_by_id:
                errors.append(f"node {ref} input {name!r} references unknown node {src_id!r}")
                continue
            src = nodes_by_id[src_id]
            if src.get("lifecycle", NODE_LIFECYCLE_DEFAULT) == "resident":
                errors.append(f"node {ref} input {name!r} cannot read from resident node {src_id!r}")
                continue
            src_output_types = _declared_port_types(src, "outputs")
            if src_output not in src_output_types:
                errors.append(
                    f"node {ref} input {name!r} references output {src_output!r} "
                    f"that node {src_id!r} does not declare"
                )
            elif port_type in PORT_TYPES and src_output_types[src_output] != port_type:
                errors.append(
                    f"node {ref} input {name!r} of type {port_type!r} cannot read from "
                    f"output {src_output!r} of type {src_output_types[src_output]!r}"
                )

        depends_on = node.get("depends_on")
        if depends_on is not None:
            if not isinstance(depends_on, list):
                errors.append(f"node {ref} depends_on must be a list of node ids")
            else:
                for dep in depends_on:
                    if not _is_nonempty_str(dep):
                        errors.append(f"node {ref} depends_on entries must be non-empty node id strings")
                    elif dep == ref:
                        errors.append(f"node {ref} cannot depend on itself")
                    elif dep not in nodes_by_id:
                        errors.append(f"node {ref} depends on unknown node {dep!r}")
                    elif nodes_by_id[dep].get("lifecycle", NODE_LIFECYCLE_DEFAULT) == "resident":
                        errors.append(f"node {ref} cannot depend on resident node {dep!r}")

        foreach = node.get("foreach")
        if foreach is not None:
            if is_resident:
                errors.append(f"resident node {ref} cannot use foreach")
            if not isinstance(foreach, dict):
                errors.append(f"node {ref} foreach must be an object")
            else:
                over = foreach.get("over")
                if not _is_nonempty_str(over):
                    errors.append(f"node {ref} foreach.over must be a non-empty input name")
                else:
                    declared_inputs = _declared_port_types(node, "inputs")
                    if over not in declared_inputs:
                        errors.append(
                            f"node {ref} foreach.over must name one of this node's inputs, got {over!r}"
                        )
                    elif declared_inputs[over] != "json":
                        errors.append(f"node {ref} foreach.over input {over!r} must have type 'json'")
                foreach_max = foreach.get("max")
                if not (_is_int(foreach_max) and FOREACH_MAX_MIN <= foreach_max <= FOREACH_MAX_MAX):
                    errors.append(
                        f"node {ref} foreach.max must be an integer between {FOREACH_MAX_MIN} and {FOREACH_MAX_MAX}"
                    )

    # The graph must be acyclic over effective dependencies (depends_on plus
    # every inputs[].from source); edges already reported above are dropped.
    try:
        topological_order(_cycle_check_nodes(nodes_by_id))
    except ValueError as exc:
        errors.append(str(exc))
    return errors


def canonicalize_swarm_spec(dag: Any) -> dict[str, Any]:
    """Apply defaults and normalize a validated DAG into a clean dict.

    Raises ``ValueError`` with the joined error list when the DAG is
    invalid. Defaults: run failure_policy 'escalate', run max_parallel 8,
    node lifecycle 'task', node retries 0, and node failure_policy copied
    from the run policy.
    """
    errors = validate_swarm_spec(dag)
    if errors:
        raise ValueError("; ".join(errors))
    assert isinstance(dag, dict)  # validated above
    run_in = dag.get("run") if isinstance(dag.get("run"), dict) else {}
    run_policy = run_in.get("failure_policy", RUN_FAILURE_POLICY_DEFAULT)
    run: dict[str, Any] = {
        "failure_policy": run_policy,
        "max_parallel": run_in.get("max_parallel", RUN_MAX_PARALLEL_DEFAULT),
    }
    if "budget_ms" in run_in:
        run["budget_ms"] = run_in["budget_ms"]
    nodes_out: list[dict[str, Any]] = []
    for node in dag["nodes"]:
        node_out: dict[str, Any] = {
            "id": node["id"],
            "subagent": copy.deepcopy(node["subagent"]),
            "lifecycle": node.get("lifecycle", NODE_LIFECYCLE_DEFAULT),
            "retries": node.get("retries", NODE_RETRIES_DEFAULT),
            "failure_policy": node.get("failure_policy", run_policy),
        }
        if "budget_ms" in node:
            node_out["budget_ms"] = node["budget_ms"]
        if "depends_on" in node:
            # Deduplicate while preserving order.
            node_out["depends_on"] = list(dict.fromkeys(node.get("depends_on") or []))
        if "inputs" in node:
            node_out["inputs"] = copy.deepcopy(node["inputs"])
        if "outputs" in node:
            node_out["outputs"] = copy.deepcopy(node["outputs"])
        if "foreach" in node:
            node_out["foreach"] = copy.deepcopy(node["foreach"])
        nodes_out.append(node_out)
    return {"run": run, "nodes": nodes_out}


def topological_order(nodes: list[dict[str, Any]]) -> list[str]:
    """Return node ids in a dependency-respecting order.

    Edges are the effective dependencies: ``depends_on`` plus every
    ``inputs[].from`` source node. Raises ``ValueError`` on a duplicate id,
    an unknown dependency, or a cycle. The order is stable: among ready
    nodes, input order wins.
    """
    if not isinstance(nodes, list):
        raise ValueError("nodes must be a list")
    index_of: dict[str, int] = {}
    for index, node in enumerate(nodes):
        if not isinstance(node, dict):
            raise ValueError(f"nodes[{index}] must be an object")
        node_id = node.get("id")
        if not isinstance(node_id, str) or not node_id:
            raise ValueError(f"nodes[{index}] requires a non-empty id")
        if node_id in index_of:
            raise ValueError(f"duplicate node id {node_id!r}")
        index_of[node_id] = index

    deps: dict[str, set[str]] = {}
    for node in nodes:
        node_id = node["id"]
        edges: set[str] = set()
        depends_on = node.get("depends_on")
        if depends_on is not None:
            if not isinstance(depends_on, list):
                raise ValueError(f"node {node_id!r} depends_on must be a list of node ids")
            for dep in depends_on:
                if not isinstance(dep, str) or not dep:
                    raise ValueError(f"node {node_id!r} depends_on entries must be non-empty node id strings")
                edges.add(dep)
        inputs = node.get("inputs")
        if inputs is not None:
            if not isinstance(inputs, list):
                raise ValueError(f"node {node_id!r} inputs must be a list")
            for inp in inputs:
                if not isinstance(inp, dict):
                    raise ValueError(f"node {node_id!r} inputs entries must be objects")
                source = inp.get("from")
                if not isinstance(source, str) or "." not in source:
                    raise ValueError(
                        f"node {node_id!r} inputs require a 'from' reference of the form '<node_id>.<output_name>'"
                    )
                edges.add(source.partition(".")[0])
        deps[node_id] = edges

    for node_id, edges in deps.items():
        for dep in edges:
            if dep not in index_of:
                raise ValueError(f"node {node_id!r} depends on unknown node {dep!r}")

    remaining = {node_id: len(edges) for node_id, edges in deps.items()}
    dependents: dict[str, list[str]] = {node_id: [] for node_id in index_of}
    for node_id, edges in deps.items():
        for dep in edges:
            dependents[dep].append(node_id)
    ready = [(index_of[node_id], node_id) for node_id, count in remaining.items() if count == 0]
    heapq.heapify(ready)
    order: list[str] = []
    while ready:
        _, current = heapq.heappop(ready)
        order.append(current)
        for dependent in dependents[current]:
            remaining[dependent] -= 1
            if remaining[dependent] == 0:
                heapq.heappush(ready, (index_of[dependent], dependent))
    if len(order) != len(index_of):
        stuck = sorted(node_id for node_id, count in remaining.items() if count > 0)
        raise ValueError(f"the swarm graph contains a cycle involving nodes: {', '.join(stuck)}")
    return order


__all__ = [
    "canonicalize_swarm_spec",
    "topological_order",
    "validate_swarm_spec",
]
