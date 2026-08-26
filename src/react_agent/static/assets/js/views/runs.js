/* Runs board: every run in the session, with the facts that distinguish them.
 *
 * The two columns worth arguing for are `exec` and `cost`. Competing products
 * treat a failed run as disposable — discard the worktree, start again — so
 * none of them needs a column for "this run was interrupted and continued".
 * And an interrupted model attempt makes the bill genuinely unknowable, so
 * cost is allowed to read `unknown` rather than be rounded down to zero.
 */

import { api } from "../api.js";
import { el, badge, dot, emptyState } from "../dom.js";
import { count, money, shortId, statusLabel, statusTone } from "../format.js";

/* How many runs get their patch materialized for the board.
 *
 * The run snapshot carries a workspace diff, but it is measured against the
 * Session baseline and therefore accumulates across runs — showing it as "what
 * this run changed" would be wrong. The correct number needs the run's own
 * checkpoint range, which means one request each, so it is capped. */
const PATCH_FETCH_LIMIT = 20;

function patchCell(patch) {
  if (!patch) return el("span", { class: "faint", text: "—" });
  if (!patch.files_changed) return el("span", { class: "faint", text: "no change" });
  return el("span", { class: "file-delta" }, [
    el("span", { class: "delta-add", text: `+${patch.additions}` }),
    " ",
    el("span", { class: "delta-del", text: `−${patch.deletions}` }),
    el("span", { class: "faint", text: `  ${patch.files_changed}f` }),
  ]);
}

function runRow(snapshot, patch, navigate) {
  const label = statusLabel(snapshot);
  const tone = statusTone(snapshot.status, snapshot.terminal);
  const live = !snapshot.terminal;
  const executions = Array.isArray(snapshot.executions) ? snapshot.executions.length : 1;
  const open = () => navigate(`/workspace/${snapshot.run_id}`);

  return el(
    "tr",
    {
      tabindex: "0",
      onClick: open,
      onKeydown: (event) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          open();
        }
      },
    },
    [
      el("td", {}, [
        el("span", { class: "row gap-3" }, [
          dot(tone, live),
          el("code", { text: shortId(snapshot.run_id, 12) }),
        ]),
      ]),
      el("td", {}, [badge(label, tone)]),
      el("td", { class: "num-cell", text: count(snapshot.last_step ?? 0) }),
      el("td", { class: "num-cell", text: count(snapshot.counts?.tool_executions ?? 0) }),
      el("td", { class: "num-cell", text: count(snapshot.usage?.total_tokens ?? 0) }),
      el("td", { class: "num-cell" }, [
        el("span", {
          style: snapshot.cost_microunits === null ? "color:var(--warn)" : "",
          text: money(snapshot.cost_microunits, snapshot.currency),
        }),
      ]),
      el("td", {}, [patchCell(patch)]),
      el("td", { class: "num-cell" }, [
        executions > 1
          ? badge(`${executions} · resumed`, "warn")
          : el("span", { class: "faint", text: String(executions) }),
      ]),
      el("td", { class: "num-cell faint", text: `#${snapshot.last_sequence ?? 0}` }),
    ],
  );
}

export async function renderRuns(root, { navigate, config }) {
  const sessionId = config?.demo_session_id;
  if (!sessionId) {
    root.replaceChildren(
      emptyState("No session", "Demo mode is off, so there is no session to enumerate."),
    );
    return;
  }

  let payload;
  try {
    payload = await api.sessionRuns(sessionId);
  } catch (error) {
    root.replaceChildren(emptyState("Could not load runs", error.message));
    return;
  }

  if (!payload.runs.length) {
    root.replaceChildren(
      emptyState("No runs yet", "Dispatch a task from the Tasks view and it appears here while it executes."),
    );
    return;
  }

  const resumed = payload.runs.filter(
    (run) => Array.isArray(run.executions) && run.executions.length > 1,
  ).length;

  const fetched = payload.runs.slice(0, PATCH_FETCH_LIMIT);
  const patches = new Map(
    (await Promise.all(
      fetched.map(async (run) => {
        try {
          return [run.run_id, await api.patch(run.run_id)];
        } catch {
          // 409 while a run has not checkpointed yet is expected, not an error.
          return [run.run_id, null];
        }
      }),
    )),
  );

  root.replaceChildren(
    el("div", { class: "view-scroll", style: "padding:0" }, [
      el("div", { style: "padding:16px 20px 12px" }, [
        el("h1", { text: "Runs" }),
        el("p", {
          class: "muted",
          style: "font-size:12px;margin-top:2px",
          text:
            `${payload.runs.length} run(s) in session ${sessionId}` +
            (resumed ? ` · ${resumed} survived an interruption and continued.` : ".") +
            (payload.runs.length > PATCH_FETCH_LIMIT
              ? ` Patch sizes shown for the first ${PATCH_FETCH_LIMIT}.`
              : ""),
        }),
      ]),
      el("table", { class: "table" }, [
        el("thead", {}, [
          el("tr", {}, [
            el("th", { text: "Run" }),
            el("th", { text: "State" }),
            el("th", { class: "num-cell", text: "Steps" }),
            el("th", { class: "num-cell", text: "Tools" }),
            el("th", { class: "num-cell", text: "Tokens" }),
            el("th", { class: "num-cell", text: "Cost" }),
            el("th", { text: "Patch" }),
            el("th", { class: "num-cell", text: "Exec" }),
            el("th", { class: "num-cell", text: "Seq" }),
          ]),
        ]),
        el(
          "tbody",
          {},
          payload.runs.map((snapshot) =>
            runRow(snapshot, patches.get(snapshot.run_id) ?? null, navigate),
          ),
        ),
      ]),
    ]),
  );
}
