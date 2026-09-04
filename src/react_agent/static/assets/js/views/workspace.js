/* The task workspace: plan and files · patch · evidence.
 *
 * Left is what the run did, centre is what it produced, right is why. The
 * right column is the one no competing console has: it turns a diff from
 * "trust me" into "here are the durable events that caused this".
 */

import { api } from "../api.js";
import { el, badge, dot, emptyState, mount, toast, announce } from "../dom.js";
import { count, money, shortId, splitPath, statusLabel, statusTone } from "../format.js";
import { followRun } from "../sse.js";
import { renderPatch, selectLine } from "../components/diff.js";
import { renderEvidence } from "../components/evidence.js";
import { projectMetrics, projectPlan, projectTouchedFiles } from "../projection.js";

const PLAN_STATE_CLASS = {
  done: "plan-step plan-step-done",
  active: "plan-step plan-step-active",
  failed: "plan-step plan-step-failed",
  pending: "plan-step",
};

export function createWorkspaceView(root, { navigate }) {
  let stream = null;
  const state = {
    runId: null,
    events: [],
    snapshot: null,
    patch: null,
    integrity: null,
    selectedFile: null,
    selectedLine: null,
    filterPath: null,
    connection: "idle",
  };

  const nodes = buildShell();
  root.replaceChildren(nodes.workspace);

  function buildShell() {
    const planBody = el("div", { class: "pane-body" });
    const filesBody = el("div", { class: "stack" });
    const patchBody = el("div", { class: "pane-body", style: "padding:16px 20px" });
    const evidenceBody = el("div", { class: "pane-body" });
    const metrics = el("div", { class: "metrics" });
    const taskHead = el("div", { class: "pane-header" });
    const patchHead = el("div", { class: "pane-header" });
    const footer = el("div", { class: "pane-footer" });

    const workspace = el("div", { class: "workspace" }, [
      el("section", { class: "pane pane-left", "aria-label": "Plan and files" }, [
        taskHead,
        planBody,
      ]),
      el("section", { class: "pane", "aria-label": "Patch" }, [
        patchHead,
        metrics,
        patchBody,
        footer,
      ]),
      el("aside", { class: "pane pane-right", "aria-label": "Evidence" }, [
        el("div", { class: "pane-header" }, [
          el("h3", { text: "Evidence" }),
          el("span", { class: "faint", style: "font-size:11px", text: "from durable events" }),
        ]),
        evidenceBody,
      ]),
    ]);

    planBody.append(filesBody);

    // Delegated once, not per render: highlighting the clicked line is pure
    // presentation, so it stays out of diff.js and out of the render path.
    patchBody.addEventListener("click", (event) => {
      const row = event.target.closest?.(".diff-line");
      if (row) selectLine(patchBody, row);
    });

    return { workspace, taskHead, planBody, filesBody, patchHead, patchBody, evidenceBody, metrics, footer };
  }

  function renderHeader() {
    const snapshot = state.snapshot;
    const label = snapshot ? statusLabel(snapshot) : state.connection;
    const tone = snapshot ? statusTone(snapshot.status, snapshot.terminal) : "run";
    const live = !snapshot?.terminal;

    mount(nodes.taskHead, [
      el("div", { class: "row gap-3 grow", style: "min-width:0" }, [
        dot(tone, live),
        el("code", { class: "truncate", text: shortId(state.runId, 12) }),
      ]),
      badge(label, tone),
    ]);

    const executions = Array.isArray(snapshot?.executions) ? snapshot.executions.length : 1;
    mount(nodes.patchHead, [
      el("div", { class: "row gap-4 grow", style: "min-width:0" }, [
        el("h3", { text: "Patch" }),
        state.patch
          ? el("span", { class: "file-delta" }, [
              el("span", { class: "delta-add", text: `+${state.patch.additions}` }),
              " ",
              el("span", { class: "delta-del", text: `−${state.patch.deletions}` }),
              el("span", { class: "faint", text: `  ${state.patch.files_changed} file(s)` }),
            ])
          : null,
        state.filterPath
          ? el("button", {
              class: "btn btn-sm btn-ghost",
              text: `filtered: ${splitPath(state.filterPath).name} ✕`,
              onClick: () => {
                state.filterPath = null;
                renderPatchPane();
                renderFiles();
                renderHeader();
              },
            })
          : null,
      ]),
      state.integrity
        ? badge(
            state.integrity.verified ? "chain verified" : "chain broken",
            state.integrity.verified ? "ok" : "bad",
          )
        : null,
      executions > 1 ? badge(`${executions} executions`, "warn") : null,
      el("span", { class: "faint", style: "font-size:11px", text: connectionLabel() }),
    ]);
  }

  function connectionLabel() {
    return (
      { live: "streaming", connecting: "connecting…", reconnecting: "reconnecting…", ended: "stream closed", error: "stream lost" }[
        state.connection
      ] ?? ""
    );
  }

  function renderMetrics() {
    const metrics = projectMetrics(state.events);
    const cells = [
      ["Step", count(metrics.step)],
      ["Model", count(metrics.modelCalls)],
      ["Tools", count(metrics.toolExecutions)],
      ["Tokens", count(metrics.tokens)],
      ["Durable", `#${metrics.cursor}`],
      ["Cost", money(metrics.costMicros, metrics.currency)],
    ];
    mount(
      nodes.metrics,
      cells.map(([label, value]) =>
        el("div", { class: "metric" }, [
          el("span", { class: "metric-label", text: label }),
          el("span", {
            class: "metric-value",
            style: value === "unknown" ? "color:var(--warn)" : "",
            text: String(value),
          }),
        ]),
      ),
    );
  }

  function renderPlan() {
    const steps = projectPlan(state.events);
    const list = el("ul", { class: "plan" });
    for (const step of steps) {
      const tools = step.calls.map((call) => call.toolName).filter(Boolean);
      list.append(
        el(
          "li",
          {
            class: PLAN_STATE_CLASS[step.state] ?? PLAN_STATE_CLASS.pending,
            role: "button",
            tabindex: "0",
            onClick: () => navigate(`/evidence/${state.runId}#step-${step.step}`),
          },
          [
            el("span", {
              class: "plan-mark",
              text: step.state === "done" ? "✓" : step.state === "failed" ? "!" : String(step.step),
            }),
            el("div", { style: "min-width:0" }, [
              el("div", { class: "plan-label", text: step.label }),
              el("div", { class: "plan-meta truncate" }, [
                tools.length ? `${tools.join(", ")} · ` : "",
                step.modelSequence ? `#${step.modelSequence}` : "",
                step.costMicros !== null ? ` · ${money(step.costMicros, step.currency)}` : "",
              ]),
            ]),
          ],
        ),
      );
    }

    mount(nodes.planBody, [
      el("div", { class: "pane-section", style: "padding:10px 0 6px" }, [
        el("h3", { text: "Execution plan", style: "padding:0 20px 6px" }),
        // Named honestly: there is no approval gate in this runtime, so this is
        // a projection of what ran, not a plan the operator signed off on.
        el("p", {
          class: "faint",
          style: "padding:0 20px 8px;font-size:10.5px;line-height:1.45",
          text: "Derived from the event log — progress, not a pre-approved plan.",
        }),
        list,
      ]),
      nodes.filesBody,
    ]);
  }

  function renderFiles() {
    const fromPatch = state.patch?.files ?? [];
    const live = projectTouchedFiles(state.events);
    const rows = fromPatch.length
      ? fromPatch.map((file) =>
          fileRow(file.path, `+${file.additions} −${file.deletions}`, file.attribution),
        )
      : live.map((file) => fileRow(file.path, `${file.writes} write(s)`, "exact"));

    mount(nodes.filesBody, [
      el("div", { class: "pane-section", style: "padding:10px 0" }, [
        el("h3", { text: `Files changed${rows.length ? ` · ${rows.length}` : ""}`, style: "padding:0 20px 6px" }),
        rows.length
          ? el("div", {}, rows)
          : el("p", {
              class: "faint",
              style: "padding:0 20px;font-size:11.5px;line-height:1.5",
              text: "Nothing written yet. Reads leave the tree unchanged.",
            }),
      ]),
    ]);
  }

  function fileRow(path, delta, attribution) {
    const { dir, name } = splitPath(path);
    return el(
      "button",
      {
        class: "file-row",
        "aria-selected": state.filterPath === path ? "true" : "false",
        onClick: () => {
          state.filterPath = state.filterPath === path ? null : path;
          renderPatchPane();
          renderFiles();
          renderHeader();
        },
      },
      [
        el("span", { class: "file-path grow truncate" }, [
          dir ? el("span", { class: "file-dir", text: dir }) : null,
          el("span", { text: name }),
        ]),
        attribution === "shared" ? badge("shared", "warn") : null,
        el("span", { class: "file-delta faint", text: delta }),
      ],
    );
  }

  function onSelect(file, line) {
    state.selectedFile = file;
    state.selectedLine = line;
    mount(nodes.evidenceBody, [
      renderEvidence({ file, line, integrity: state.integrity }),
    ]);
    announce(`Evidence for ${file.path}`);
  }

  function renderPatchPane() {
    if (!state.patch) {
      mount(nodes.patchBody, [
        emptyState(
          "Waiting for the first checkpoint",
          "The patch is rebuilt from Git once the run commits a workspace checkpoint. The event log stores tree ids, never file contents.",
        ),
      ]);
      return;
    }
    mount(nodes.patchBody, [
      renderPatch(state.patch, {
        filterPath: state.filterPath,
        onSelect,
      }),
    ]);
  }

  function renderFooter() {
    const terminal = state.snapshot?.terminal;
    const hasPatch = (state.patch?.files_changed ?? 0) > 0;
    mount(nodes.footer, [
      el("div", { class: "grow faint", style: "font-size:11.5px" }, [
        state.snapshot?.answer
          ? el("span", { class: "truncate", text: state.snapshot.answer })
          : el("span", { text: terminal ? "Run finished." : "Run in progress…" }),
      ]),
      terminal
        ? null
        : el("button", {
            class: "btn btn-sm btn-danger",
            text: "Cancel",
            onClick: async (event) => {
              event.currentTarget.disabled = true;
              try {
                await api.cancel(state.runId);
              } catch (error) {
                toast(error.message, "bad");
              }
            },
          }),
      el("button", {
        class: "btn btn-sm",
        text: "Timeline",
        onClick: () => navigate(`/evidence/${state.runId}`),
      }),
      el("button", {
        class: "btn btn-sm btn-primary",
        text: "Create pull request",
        disabled: !hasPatch || !terminal,
        title: hasPatch
          ? "Offline demo: no Git remote is configured, so this reports the branch and patch instead of pushing."
          : "There is nothing to open a pull request for.",
        onClick: () => {
          toast(
            `Branch witness/${shortId(state.runId, 8)} holds ${state.patch.files_changed} file(s), ` +
              `+${state.patch.additions} −${state.patch.deletions}. No Git remote is configured in demo mode, ` +
              "so nothing was pushed.",
          );
        },
      }),
    ]);
  }

  async function loadDerived() {
    const [snapshot, integrity, patch] = await Promise.allSettled([
      api.run(state.runId),
      api.integrity(state.runId),
      api.patch(state.runId),
    ]);
    if (snapshot.status === "fulfilled") state.snapshot = snapshot.value;
    if (integrity.status === "fulfilled") state.integrity = integrity.value;
    // A 409 here is expected while the run has not checkpointed yet.
    state.patch = patch.status === "fulfilled" ? patch.value : null;

    renderHeader();
    renderFiles();
    renderPatchPane();
    renderFooter();
    if (state.selectedFile && state.patch) {
      const refreshed = state.patch.files.find((file) => file.path === state.selectedFile.path);
      if (refreshed) onSelect(refreshed, state.selectedLine);
    } else {
      mount(nodes.evidenceBody, [renderEvidence({ integrity: state.integrity })]);
    }
  }

  return {
    async open(runId) {
      this.close();
      Object.assign(state, {
        runId,
        events: [],
        snapshot: null,
        patch: null,
        integrity: null,
        selectedFile: null,
        selectedLine: null,
        filterPath: null,
        connection: "connecting",
      });
      renderHeader();
      renderPlan();
      renderFiles();
      renderMetrics();
      renderPatchPane();
      renderFooter();
      mount(nodes.evidenceBody, [renderEvidence({})]);

      let settleTimer = null;
      const settle = () => {
        clearTimeout(settleTimer);
        settleTimer = setTimeout(() => void loadDerived(), 260);
      };

      stream = followRun(runId, {
        onEvent: (event) => {
          if (!event.durable) return;
          state.events.push(event);
          renderMetrics();
          renderPlan();
          if (event.kind === "workspace_checkpointed" || event.terminal) settle();
        },
        onStatus: (status) => {
          state.connection = status;
          renderHeader();
          if (status === "ended") settle();
        },
      });

      await loadDerived();
    },

    close() {
      stream?.close();
      stream = null;
    },
  };
}
