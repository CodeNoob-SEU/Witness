/* Settings view.
 *
 * Read-only on purpose. The values here describe guarantees the runtime
 * actually enforces; a form that let an operator type a different number would
 * imply the runtime reads it back, which it does not. Where a boundary is
 * *not* enforced — network egress, for one — this view says so rather than
 * leaving a reassuring blank.
 */

import { el, badge, emptyState } from "../dom.js";

function section(title, description, rows) {
  return el("div", { class: "card" }, [
    el("div", { class: "card-head" }, [
      el("h3", { text: title }),
      description ? el("span", { class: "faint", style: "font-size:11px", text: description }) : null,
    ]),
    el(
      "div",
      { class: "card-body stack gap-4" },
      rows.map(([label, value, tone]) =>
        el("div", { class: "row gap-5", style: "align-items:flex-start" }, [
          el("div", { style: "width:150px;flex:0 0 auto" }, [
            el("span", { class: "faint", style: "font-size:11px;text-transform:uppercase;letter-spacing:0.05em", text: label }),
          ]),
          el("div", { class: "grow", style: "min-width:0;font-size:12px;line-height:1.5" }, [
            typeof value === "string" ? el("span", { text: value }) : value,
          ]),
          tone ? badge(tone[0], tone[1]) : null,
        ]),
      ),
    ),
  ]);
}

export function renderSettings(root, { config }) {
  if (!config) {
    root.replaceChildren(emptyState("No configuration", "The console could not read /api/console."));
    return;
  }

  const policy = config.workspace_policy ?? {};

  root.replaceChildren(
    el("div", { class: "view-scroll" }, [
      el("div", { class: "view-wide stack gap-6" }, [
        el("div", {}, [
          el("h1", { text: "Settings" }),
          el("p", {
            class: "muted",
            style: "font-size:12px;margin-top:2px",
            text: "What this runtime enforces, and what it does not.",
          }),
        ]),

        section("Execution", null, [
          ["Model", el("code", { text: config.model ?? "unknown" })],
          [
            "Mode",
            config.demo
              ? "Demo — a deterministic scripted provider replaces the LLM. Everything else (runtime, worktree isolation, durable log) is the real thing."
              : "Live — calls a configured OpenAI-compatible endpoint.",
            config.demo ? ["demo", "accent"] : ["live", "ok"],
          ],
          [
            "Journal",
            config.journal === "postgres"
              ? "PostgreSQL — durable across process restarts, with leases and fencing tokens."
              : "In-memory — durable within this process only. Restarting the server loses every run.",
            config.journal === "postgres" ? ["durable", "ok"] : ["ephemeral", "warn"],
          ],
          ["Repository", el("code", { text: config.repository ?? "—" })],
          ["Session", el("code", { text: config.demo_session_id ?? "—" })],
        ]),

        section("Workspace boundary", "enforced by the tool layer", [
          ["Isolation", policy.isolation ?? "—", ["enforced", "ok"]],
          ["Writes", policy.writes ?? "—", ["enforced", "ok"]],
          ["Denied", policy.denied ?? "—", ["enforced", "ok"]],
          [
            "Network",
            (policy.network ?? "—") +
              " — the registered tools make no outbound requests, but this runtime does not sandbox egress the way a container-based agent would.",
            ["not enforced", "warn"],
          ],
        ]),

        section("Correctness invariants", "properties the runtime holds, not options", [
          ["Intent first", "A model call or tool execution commits its intent before any side effect can happen."],
          ["Log is truth", "Snapshots are a cache. Deleting one still allows exact reconstruction from sequence 1."],
          [
            "Observability is not correctness",
            "OpenTelemetry consumes a safe projection of committed events. Sampling or export failure cannot affect execution or recovery.",
          ],
          [
            "Unknown is not zero",
            "An interrupted attempt's spend is recorded as unknown. Neither the ledger nor this console rounds it to 0.",
          ],
        ]),
      ]),
    ]),
  );
}
