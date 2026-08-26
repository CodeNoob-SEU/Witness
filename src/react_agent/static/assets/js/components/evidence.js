/* The evidence panel.
 *
 * This is the view the rest of the product exists to make possible: for a given
 * change, show the durable events that produced it. Every field here is a real
 * sequence number, tree id, or ledger amount from this run's log — nothing is
 * summarised by a model, and nothing is inferred from tool arguments (which the
 * default METADATA debug exposure keeps out of the log entirely).
 */

import { el, badge, kv } from "../dom.js";
import { money, shortId } from "../format.js";

function originBlock(origin, index, total) {
  const links = [];

  const link = (sequence, kind, note) =>
    sequence === null || sequence === undefined
      ? null
      : el("div", { class: "chain-link" }, [
          el("span", { class: "chain-seq", text: String(sequence) }),
          el("div", { class: "chain-body" }, [
            el("div", { class: "chain-kind", text: kind }),
            note ? el("div", { class: "chain-note", text: note }) : null,
          ]),
        ]);

  links.push(link(origin.model_sequence, "model_completed", "the decision to make this edit"));
  links.push(link(origin.planned_sequence, "tool_planned", origin.tool_name ?? undefined));
  links.push(
    link(
      origin.started_sequence,
      "tool_started",
      origin.attempts > 1
        ? `${origin.attempts} attempts — same idempotency key`
        : "side effect may have happened from here",
    ),
  );
  links.push(link(origin.completed_sequence, "tool_completed", "result committed"));

  return el("div", { class: "pane-section" }, [
    total > 1
      ? el("div", { class: "row gap-3", style: "margin-bottom:8px" }, [
          badge(`write ${index + 1} of ${total}`, "warn"),
        ])
      : null,
    el("div", { class: "stack", style: "margin-bottom:12px" }, links.filter(Boolean)),
    kv([
      ["Call", origin.call_key],
      ["Tool", origin.tool_name],
      ["Step", origin.step],
      ["Cost", money(origin.cost_micros, origin.currency)],
      ["Tree", `${shortId(origin.before_tree)} → ${shortId(origin.after_tree)}`],
      ["Execution", shortId(origin.execution_id, 12)],
    ]),
  ]);
}

function integrityBlock(integrity) {
  if (!integrity) return null;
  const tone = integrity.verified ? "ok" : "bad";
  return el("div", { class: "pane-section" }, [
    el("div", { class: "row gap-3", style: "margin-bottom:8px" }, [
      badge(integrity.verified ? "chain verified" : "chain broken", tone),
      integrity.resumed ? badge(`${integrity.executions} executions`, "warn") : null,
    ]),
    el("p", {
      class: "faint",
      style: "font-size:11.5px;line-height:1.5;margin-bottom:8px",
      text: integrity.verified
        ? `Hashes link every event from sequence ${integrity.first_sequence} to ${integrity.last_sequence}. Nothing was inserted, removed, or backfilled after the fact.`
        : `Verification failed (${integrity.reason ?? "unknown"}). Treat this run's history as untrustworthy.`,
    }),
    integrity.resumed
      ? el("p", {
          class: "faint",
          style: "font-size:11.5px;line-height:1.5",
          text: "More than one execution means this run was interrupted and continued from durable facts — not restarted from scratch.",
        })
      : null,
  ]);
}

/**
 * Render the panel for a selected file (and optionally a line).
 *
 * `line` is accepted so the header can name the exact spot the user clicked,
 * but attribution itself stays per-file. Claiming line-level provenance for a
 * whole-file rewrite would be precision the log cannot support.
 */
export function renderEvidence({ file, line, integrity }) {
  if (!file) {
    return el("div", { class: "stack" }, [
      integrityBlock(integrity),
      el("div", {
        class: "evidence-empty",
        text: "Select a line in the patch to see which durable events produced it.",
      }),
    ]);
  }

  const origins = file.origins ?? [];
  const header = el("div", { class: "pane-section" }, [
    el("div", { class: "file-path truncate", text: file.path }),
    el("div", { class: "faint", style: "font-size:11px;margin-top:2px" }, [
      line?.new_line
        ? `line ${line.new_line}`
        : line?.old_line
          ? `removed line ${line.old_line}`
          : `+${file.additions} −${file.deletions}`,
    ]),
  ]);

  if (!origins.length) {
    return el("div", { class: "stack" }, [
      header,
      el("div", { class: "pane-section" }, [
        el("div", { class: "banner banner-warn" }, [
          el("div", {}, [
            el("div", { class: "banner-title", text: "No attributable write" }),
            el("div", {
              text: "No checkpoint pair in this run's log shows a tree change for this path. The change exists in the diff but the log cannot say which call made it.",
            }),
          ]),
        ]),
      ]),
      integrityBlock(integrity),
    ]);
  }

  return el("div", { class: "stack" }, [
    header,
    ...origins.map((origin, index) => originBlock(origin, index, origins.length)),
    integrityBlock(integrity),
  ]);
}
