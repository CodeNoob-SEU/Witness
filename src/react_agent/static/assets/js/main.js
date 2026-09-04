/* Router and shell wiring.
 *
 * Hash routing keeps the console a single static asset with no server-side
 * route table, which is what lets the whole UI ship inside the Python wheel.
 */

import { api } from "./api.js";
import { el, qs, toast } from "./dom.js";
import { renderTasks } from "./views/tasks.js";
import { renderRuns } from "./views/runs.js";
import { createWorkspaceView } from "./views/workspace.js";
import { renderEvidenceView } from "./views/evidence.js";
import { renderRecovery } from "./views/recovery.js";
import { renderEvals } from "./views/evals.js";
import { renderSettings } from "./views/settings.js";

const outlet = qs("#outlet");
const railLinks = [...document.querySelectorAll(".rail-link")];

let config = null;
let workspaceView = null;
let activeSection = null;

function navigate(path) {
  const next = `#${path}`;
  if (window.location.hash === next) route();
  else window.location.hash = next;
}

function parseRoute() {
  const raw = window.location.hash.replace(/^#/, "") || "/tasks";
  const [pathPart] = raw.split("#");
  const parts = pathPart.split("/").filter(Boolean);
  return { section: parts[0] ?? "tasks", param: parts[1] ?? null };
}

function setActiveRail(section) {
  for (const link of railLinks) {
    const owns = link.dataset.section === section;
    if (owns) link.setAttribute("aria-current", "page");
    else link.removeAttribute("aria-current");
  }
}

function teardown() {
  if (workspaceView) {
    workspaceView.close();
    workspaceView = null;
  }
}

async function route() {
  const { section, param } = parseRoute();
  // The workspace view owns a live stream; only rebuild it when we leave.
  if (section !== activeSection || section === "workspace") teardown();
  activeSection = section;
  setActiveRail(section === "evidence" ? "runs" : section);

  try {
    switch (section) {
      case "tasks":
        await renderTasks(outlet, { navigate, config });
        break;
      case "runs":
        await renderRuns(outlet, { navigate, config });
        break;
      case "workspace":
        if (!param) return navigate("/runs");
        workspaceView = createWorkspaceView(outlet, { navigate });
        await workspaceView.open(param);
        break;
      case "evidence":
        if (!param) return navigate("/runs");
        await renderEvidenceView(outlet, { runId: param, navigate });
        break;
      case "recovery":
        await renderRecovery(outlet);
        break;
      case "evals":
        await renderEvals(outlet);
        break;
      case "settings":
        renderSettings(outlet, { config });
        break;
      default:
        navigate("/tasks");
    }
  } catch (error) {
    toast(error.message ?? "Something went wrong.", "bad");
  }
}

function renderTopbar() {
  const repo = qs("#repo-chip");
  if (!repo) return;
  repo.replaceChildren(
    el("span", { class: "faint", text: "repo" }),
    el("code", { text: config?.repository ?? "not configured" }),
    config?.demo ? el("span", { class: "badge badge-accent", text: "demo" }) : null,
    config?.journal === "in_memory"
      ? el("span", { class: "badge badge-warn", text: "in-memory" })
      : config?.journal === "postgres"
        ? el("span", { class: "badge badge-ok", text: "postgres" })
        : null,
  );
}

/* Theme. Dark is the default; the toggle exists because an audit trail is
 * sometimes read in a bright room. */
function initTheme() {
  const stored = localStorage.getItem("witness-theme");
  if (stored === "light") document.documentElement.dataset.theme = "light";
  qs("#theme-toggle")?.addEventListener("click", () => {
    const isLight = document.documentElement.dataset.theme === "light";
    if (isLight) delete document.documentElement.dataset.theme;
    else document.documentElement.dataset.theme = "light";
    localStorage.setItem("witness-theme", isLight ? "dark" : "light");
  });
}

/* Keyboard navigation. `g` then a letter is the convention these tools share;
 * it signals the product expects to be lived in rather than clicked through. */
function initKeyboard() {
  let pendingG = false;
  window.addEventListener("keydown", (event) => {
    const target = event.target;
    if (target instanceof HTMLElement && /^(INPUT|TEXTAREA|SELECT)$/.test(target.tagName)) return;
    if (event.metaKey || event.ctrlKey || event.altKey) return;

    if (pendingG) {
      pendingG = false;
      const destination = {
        t: "/tasks",
        r: "/runs",
        c: "/recovery",
        e: "/evals",
        s: "/settings",
      }[event.key];
      if (destination) {
        event.preventDefault();
        navigate(destination);
      }
      return;
    }
    if (event.key === "g") {
      pendingG = true;
      setTimeout(() => (pendingG = false), 1200);
      return;
    }
    if (event.key === "?") {
      event.preventDefault();
      toast("g t tasks · g r runs · g c recovery · g e evals · g s settings");
    }
  });
}

async function boot() {
  initTheme();
  initKeyboard();
  try {
    config = await api.console();
  } catch (error) {
    toast(`Could not read runtime configuration: ${error.message}`, "bad");
  }
  renderTopbar();
  window.addEventListener("hashchange", () => void route());
  await route();
}

void boot();
