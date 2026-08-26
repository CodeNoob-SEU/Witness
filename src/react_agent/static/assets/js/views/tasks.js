/* Tasks view: the console's entry point.
 *
 * The unit of work here is a task with acceptance criteria and a target
 * repository — not a message in a chat box. That distinction is the whole
 * difference between an agent runtime and a chat window with tools attached.
 */

import { api } from "../api.js";
import { el, badge, emptyState, mount, toast } from "../dom.js";

function taskCard(task, onDispatch) {
  return el("div", { class: "task-card" }, [
    el("div", { class: "row gap-4", style: "margin-bottom:4px" }, [
      el("h2", { class: "grow truncate", text: task.title }),
      ...task.labels.map((label) =>
        badge(label, label === "safety" || label === "expected-refusal" ? "warn" : ""),
      ),
    ]),
    el("p", {
      class: "muted",
      style: "font-size:12px;line-height:1.5;margin-bottom:10px",
      text: task.summary,
    }),
    el("div", { style: "margin-bottom:12px" }, [
      el("h3", { text: "Acceptance", style: "margin-bottom:4px" }),
      el(
        "ul",
        { class: "muted", style: "margin:0;padding-left:18px;font-size:12px;line-height:1.6" },
        task.acceptance.map((item) => el("li", { text: item })),
      ),
    ]),
    el("div", { class: "row gap-4" }, [
      el("button", {
        class: "btn btn-primary",
        text: "Dispatch",
        onClick: (event) => onDispatch(task, event.currentTarget),
      }),
      el("code", { class: "faint truncate", text: task.id }),
    ]),
  ]);
}

export async function renderTasks(root, { navigate, config }) {
  if (!config?.demo) {
    root.replaceChildren(
      emptyState(
        "Demo mode is off",
        "Start the server with REACT_AGENT_DEMO=1 to load the seeded repository and its task catalog.",
      ),
    );
    return;
  }

  let payload;
  try {
    payload = await api.tasks();
  } catch (error) {
    root.replaceChildren(emptyState("Could not load tasks", error.message));
    return;
  }

  const onDispatch = async (task, button) => {
    button.disabled = true;
    button.textContent = "Dispatching…";
    try {
      const handle = await api.startTask(task.id);
      navigate(`/workspace/${handle.run_id}`);
    } catch (error) {
      button.disabled = false;
      button.textContent = "Dispatch";
      // A Session owns at most one live Run. That is the invariant that makes
      // fencing and single-writer recovery tractable, so the console explains
      // it and offers the live run rather than presenting a bare conflict.
      const active = error.status === 409 && /active run '([0-9a-f]+)'/.exec(error.message);
      if (active) {
        showActiveRunNotice(active[1]);
        return;
      }
      toast(`Could not dispatch: ${error.message}`, "bad");
    }
  };

  const showActiveRunNotice = (runId) => {
    const banner = el("div", { class: "banner banner-warn" }, [
      el("div", { class: "grow" }, [
        el("div", { class: "banner-title", text: "This session already has a live run" }),
        el("div", {
          text:
            "A session owns at most one running task at a time — that single-writer rule is what " +
            "lets a crashed run be fenced off and resumed safely. Finish or cancel the live run first.",
        }),
      ]),
      el("button", {
        class: "btn btn-sm",
        text: "Open it",
        onClick: () => navigate(`/workspace/${runId}`),
      }),
    ]);
    const host = root.querySelector("#dispatch-notice");
    if (host) mount(host, [banner]);
  };

  root.replaceChildren(
    el("div", { class: "view-scroll" }, [
      el("div", { class: "view-wide stack gap-6" }, [
        el("div", {}, [
          el("h1", { text: "Tasks" }),
          el("p", {
            class: "muted",
            style: "font-size:12px;margin-top:2px",
            text: `Repository-level work items against ${payload.repository}. Each one runs in its own isolated Git worktree.`,
          }),
        ]),
        el("div", { id: "dispatch-notice" }),
        el("div", {}, payload.tasks.map((task) => taskCard(task, onDispatch))),
      ]),
    ]),
  );
}
