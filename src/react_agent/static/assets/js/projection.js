/* Pure projections over a run's durable event stream.
 *
 * Everything the workspace view renders on the left — the execution plan, the
 * touched-file list, the metrics, the crash/resume boundaries — is derived
 * here rather than fetched. There is no plan endpoint because there is no plan
 * *event*: the runtime has no approval gate, so this is an honest projection of
 * what the run did, not a record of what it promised to do.
 *
 * Kept free of DOM and fetch so it can be exercised directly under Node.
 */

const MODEL_KINDS = new Set([
  "model_started",
  "model_completed",
  "model_failed",
  "model_abandoned",
]);

const TOOL_KINDS = new Set([
  "tool_planned",
  "tool_started",
  "tool_claimed",
  "tool_completed",
  "tool_reused",
]);

const LIFECYCLE_KINDS = new Set([
  "run_started",
  "run_resumed",
  "run_forked",
  "run_result_ready",
  "run_completed",
  "run_aborted",
  "run_cancel_requested",
  "session_committed",
]);

const PROBLEM_KINDS = new Set([
  "model_failed",
  "model_abandoned",
  "run_aborted",
  "budget_exhausted",
  "loop_detected",
  "reconciliation_required",
  "workspace_diverged",
]);

/** A readable label for one tool call, from durable facts only. */
const TOOL_LABELS = {
  list_workspace_files: "Survey the repository",
  read_workspace_file: "Read a source file",
  write_workspace_file: "Write a source file",
  calculate_expression: "Evaluate an expression",
};

export function eventGroup(kind) {
  if (PROBLEM_KINDS.has(kind)) return "bad";
  if (MODEL_KINDS.has(kind)) return "model";
  if (TOOL_KINDS.has(kind)) return "tool";
  if (kind === "cost_recorded" || kind === "cost_adjusted") return "cost";
  if (kind.startsWith("workspace_") || kind === "checkpoint") return "workspace";
  if (LIFECYCLE_KINDS.has(kind)) return "life";
  return "life";
}

function data(event) {
  return event && typeof event.data === "object" && event.data !== null ? event.data : {};
}

function asInt(value) {
  return Number.isSafeInteger(value) ? value : null;
}

/**
 * Group the log into per-step "plan" entries.
 *
 * A step is one model decision plus whatever tool calls it produced. That is
 * the real unit of work in a ReAct loop, and it is the only grouping the log
 * actually supports.
 */
export function projectPlan(events) {
  const steps = new Map();

  const stepFor = (index) => {
    if (!steps.has(index)) {
      steps.set(index, {
        step: index,
        state: "pending",
        calls: [],
        modelSequence: null,
        costMicros: null,
        currency: null,
        inputTokens: 0,
        outputTokens: 0,
        firstSequence: null,
        lastSequence: null,
      });
    }
    return steps.get(index);
  };

  for (const event of events) {
    const index = asInt(event.step);
    if (index === null) continue;
    const entry = stepFor(index);
    entry.firstSequence ??= event.sequence;
    entry.lastSequence = event.sequence;

    const payload = data(event);
    switch (event.kind) {
      case "model_started":
        entry.state = entry.state === "done" ? entry.state : "active";
        break;
      case "model_completed": {
        entry.modelSequence = event.sequence;
        const usage = payload.usage && typeof payload.usage === "object" ? payload.usage : payload;
        entry.inputTokens = asInt(usage.input_tokens) ?? entry.inputTokens;
        entry.outputTokens = asInt(usage.output_tokens) ?? entry.outputTokens;
        if (asInt(payload.tool_calls) === 0) entry.state = "done";
        break;
      }
      case "model_failed":
      case "model_abandoned":
        entry.state = "failed";
        break;
      case "cost_recorded":
        entry.costMicros = asInt(payload.amount_micros);
        entry.currency = typeof payload.currency === "string" ? payload.currency : null;
        break;
      case "tool_planned":
        entry.calls.push({
          callKey: event.call_key,
          toolName: payload.tool_name ?? event.tool_name ?? null,
          plannedSequence: event.sequence,
          startedSequence: null,
          completedSequence: null,
          attempts: 0,
          state: "pending",
          executed: false,
          isError: false,
        });
        entry.state = entry.state === "failed" ? entry.state : "active";
        break;
      case "tool_started":
      case "tool_claimed": {
        const call = entry.calls.find((item) => item.callKey === event.call_key);
        if (call) {
          call.startedSequence ??= event.sequence;
          call.attempts += 1;
          call.state = "active";
        }
        break;
      }
      case "tool_completed": {
        const call = entry.calls.find((item) => item.callKey === event.call_key);
        if (call) {
          call.completedSequence = event.sequence;
          call.executed = payload.executed === true;
          call.isError = payload.is_error === true || payload.status === "error";
          call.state = call.isError ? "failed" : "done";
        }
        break;
      }
      default:
        break;
    }
  }

  // A step whose calls have all finished is done, unless the model failed.
  for (const entry of steps.values()) {
    if (entry.state === "failed") continue;
    if (entry.calls.length && entry.calls.every((call) => call.state === "done")) {
      entry.state = "done";
    }
  }

  return [...steps.values()]
    .sort((left, right) => left.step - right.step)
    .map((entry) => ({ ...entry, label: planLabel(entry) }));
}

function planLabel(entry) {
  if (!entry.calls.length) {
    return entry.state === "done" ? "Report the result" : "Decide the next action";
  }
  const names = [...new Set(entry.calls.map((call) => call.toolName).filter(Boolean))];
  if (!names.length) return "Run a tool";
  if (names.length > 1) return `Run ${names.length} tools`;
  return TOOL_LABELS[names[0]] ?? names[0];
}

/**
 * Which tool calls moved the workspace tree, from checkpoint tree ids alone.
 *
 * This is the client-side twin of `react_agent.patch.patch_origins`. It exists
 * so the live view can show attribution while a run is still going, before the
 * patch endpoint has a final checkpoint to diff against.
 */
export function projectWorkspaceWrites(events) {
  const opening = new Map();
  const writes = [];
  for (const event of events) {
    if (event.kind !== "workspace_checkpointed") continue;
    const payload = data(event);
    const callKey = payload.call_key;
    if (!callKey) continue;
    if (payload.phase === "before_tool") {
      opening.set(callKey, payload);
    } else if (payload.phase === "after_tool") {
      const before = opening.get(callKey);
      opening.delete(callKey);
      if (before && before.tree_id !== payload.tree_id) {
        writes.push({
          callKey,
          beforeTree: before.tree_id,
          afterTree: payload.tree_id,
          beforeCommit: before.commit_id,
          afterCommit: payload.commit_id,
          paths: Array.isArray(payload.diff?.paths) ? payload.diff.paths : [],
          sequence: event.sequence,
        });
      }
    }
  }
  return writes;
}

/**
 * Execution boundaries: where a run was interrupted and picked up again.
 *
 * More than one execution id means the run survived something. That is the
 * single most load-bearing fact this console displays, so it is projected
 * explicitly rather than inferred from a status string.
 */
export function projectExecutions(events) {
  const executions = [];
  for (const event of events) {
    if (!event.execution_id) continue;
    const last = executions[executions.length - 1];
    if (!last || last.executionId !== event.execution_id) {
      executions.push({
        executionId: event.execution_id,
        firstSequence: event.sequence,
        lastSequence: event.sequence,
        // A boundary that is not the first execution is a resume: the previous
        // execution stopped without reaching a terminal event.
        resumed: executions.length > 0,
      });
    } else {
      last.lastSequence = event.sequence;
    }
  }
  return executions;
}

/** Roll the log up into the numbers shown in the metric strip. */
export function projectMetrics(events) {
  let modelCalls = 0;
  let toolExecutions = 0;
  let inputTokens = 0;
  let outputTokens = 0;
  let costMicros = 0;
  let costKnown = true;
  let currency = null;
  let step = 0;
  let cursor = 0;
  let terminal = null;

  for (const event of events) {
    cursor = Math.max(cursor, asInt(event.sequence) ?? 0);
    step = Math.max(step, asInt(event.step) ?? 0);
    const payload = data(event);
    if (event.kind === "model_started") modelCalls += 1;
    if (event.kind === "model_abandoned") costKnown = false;
    if (event.kind === "tool_completed" && payload.executed === true) toolExecutions += 1;
    if (event.kind === "model_completed") {
      const usage = payload.usage && typeof payload.usage === "object" ? payload.usage : payload;
      inputTokens += asInt(usage.input_tokens) ?? 0;
      outputTokens += asInt(usage.output_tokens) ?? 0;
    }
    if (event.kind === "cost_recorded" || event.kind === "cost_adjusted") {
      const amount = asInt(payload.amount_micros);
      if (amount === null) costKnown = false;
      else costMicros += amount;
      if (typeof payload.currency === "string") currency = payload.currency;
    }
    if (event.kind === "run_completed" || event.kind === "run_aborted") {
      terminal = payload.status ?? event.kind;
    }
  }

  return {
    step,
    cursor,
    modelCalls,
    toolExecutions,
    tokens: inputTokens + outputTokens,
    inputTokens,
    outputTokens,
    // An interrupted attempt makes the bill unknowable, and the ledger reports
    // unknown rather than rounding it to zero. The UI must not round it either.
    costMicros: costKnown ? costMicros : null,
    currency,
    terminal,
  };
}

/** Files the run touched, with per-file write counts, for the live view. */
export function projectTouchedFiles(events) {
  const files = new Map();
  for (const write of projectWorkspaceWrites(events)) {
    for (const path of write.paths) {
      const entry = files.get(path) ?? { path, writes: 0, lastCallKey: null };
      entry.writes += 1;
      entry.lastCallKey = write.callKey;
      files.set(path, entry);
    }
  }
  return [...files.values()].sort((left, right) => left.path.localeCompare(right.path));
}

/** A short, human-readable detail line for one timeline row. */
export function eventDetail(event) {
  const payload = data(event);
  const parts = [];
  const name = payload.tool_name ?? event.tool_name;
  if (name) parts.push(name);
  if (event.call_key) parts.push(event.call_key);
  if (payload.phase) parts.push(payload.phase);
  if (typeof payload.tree_id === "string") parts.push(`tree ${payload.tree_id.slice(0, 8)}`);
  if (asInt(payload.amount_micros) !== null) {
    parts.push(`${(payload.amount_micros / 1_000_000).toFixed(6)} ${payload.currency ?? ""}`.trim());
  }
  if (payload.status && !name) parts.push(String(payload.status));
  if (payload.outcome && payload.outcome !== payload.status) parts.push(String(payload.outcome));
  if (asInt(payload.attempt) !== null && payload.attempt > 1) parts.push(`attempt ${payload.attempt}`);
  return parts.join(" · ");
}
