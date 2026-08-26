/* Evals view.
 *
 * The scoreboard is graded against the resulting worktree, not against the
 * model's account of itself, and the caveat travels with the numbers rather
 * than living in a footnote — a pass rate produced by a deterministic
 * reference model measures the harness, and presenting it as anything else
 * would be the exact "autonomy illusion" this product argues against.
 */

import { api } from "../api.js";
import { el, badge, emptyState } from "../dom.js";
import { count, duration, money } from "../format.js";

export async function renderEvals(root) {
  root.replaceChildren(
    el("div", { class: "view-scroll" }, [el("p", { class: "faint", text: "Running the suite…" })]),
  );

  let report;
  try {
    report = await api.evals();
  } catch (error) {
    root.replaceChildren(emptyState("Could not run the suite", error.message));
    return;
  }

  const rows = report.outcomes.map((outcome) =>
    el("tr", { style: "cursor:default" }, [
      el("td", {}, [el("code", { text: outcome.task })]),
      el("td", {}, [badge(outcome.passed ? "pass" : "fail", outcome.passed ? "ok" : "bad")]),
      el("td", { class: "muted", style: "font-size:11.5px", text: outcome.detail }),
      el("td", { class: "num-cell", text: count(outcome.model_calls) }),
      el("td", { class: "num-cell", text: count(outcome.tool_executions) }),
      el("td", { class: "num-cell", text: count(outcome.input_tokens + outcome.output_tokens) }),
      el("td", {
        class: "num-cell",
        style: outcome.cost_micros === null ? "color:var(--warn)" : "",
        text: money(outcome.cost_micros, outcome.currency),
      }),
      el("td", { class: "num-cell faint", text: duration(outcome.duration_s) }),
    ]),
  );

  root.replaceChildren(
    el("div", { class: "view-scroll", style: "padding:0" }, [
      el("div", { class: "stack gap-5", style: "padding:16px 20px" }, [
        el("div", {}, [
          el("h1", { text: "Evaluations" }),
          el("p", {
            class: "muted",
            style: "font-size:12px;margin-top:2px",
            text: `${report.suite} · ${report.passed}/${report.tasks} passed (${Math.round(report.pass_rate * 100)}%)`,
          }),
        ]),
        el("div", { class: "banner" }, [
          el("div", {}, [
            el("div", { class: "banner-title", text: "How these numbers are produced" }),
            el("div", { text: report.caveat }),
          ]),
        ]),
        el("div", { class: "metrics", style: "border:1px solid var(--line);border-radius:5px" }, [
          ["Tasks", count(report.tasks)],
          ["Passed", count(report.passed)],
          ["Pass rate", `${Math.round(report.pass_rate * 100)}%`],
          ["Input tokens", count(report.total_input_tokens)],
          ["Output tokens", count(report.total_output_tokens)],
          ["Total cost", money(report.total_cost_micros, null)],
        ].map(([label, value]) =>
          el("div", { class: "metric" }, [
            el("span", { class: "metric-label", text: label }),
            el("span", {
              class: "metric-value",
              style: value === "unknown" ? "color:var(--warn)" : "",
              text: String(value),
            }),
          ]),
        )),
      ]),
      el("table", { class: "table" }, [
        el("thead", {}, [
          el("tr", {}, [
            el("th", { text: "Task" }),
            el("th", { text: "Result" }),
            el("th", { text: "Criterion" }),
            el("th", { class: "num-cell", text: "Model" }),
            el("th", { class: "num-cell", text: "Tools" }),
            el("th", { class: "num-cell", text: "Tokens" }),
            el("th", { class: "num-cell", text: "Cost" }),
            el("th", { class: "num-cell", text: "Time" }),
          ]),
        ]),
        el("tbody", {}, rows),
      ]),
    ]),
  );
}
