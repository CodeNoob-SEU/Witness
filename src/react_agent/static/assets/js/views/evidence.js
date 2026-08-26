/* Evidence view: the raw chain, its integrity, and the fork control.
 *
 * The timeline is deliberately unsummarised. Everywhere else the console
 * projects the log into something friendlier; here it shows the log, because
 * an audit surface that only ever shows an interpretation is not an audit
 * surface.
 */

import { api } from "../api.js";
import { el, badge, emptyState, kv, mount, toast } from "../dom.js";
import { count, money, shortId } from "../format.js";
import { renderTimeline } from "../components/timeline.js";
import { projectExecutions, projectMetrics } from "../projection.js";

async function collectEvents(runId) {
  // The events endpoint is SSE even for history; read it once with follow=false.
  const response = await fetch(
    `/api/runs/${encodeURIComponent(runId)}/events?after_sequence=0&follow=false`,
    { headers: { accept: "text/event-stream" } },
  );
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  const text = await response.text();
  const events = [];
  for (const block of text.split(/(?:\r\n|\r|\n){2}/)) {
    const lines = block.split(/\r\n|\n|\r/).filter((line) => line.startsWith("data:"));
    if (!lines.length) continue;
    const raw = lines.map((line) => line.slice(5).replace(/^ /, "")).join("\n");
    if (raw === "[DONE]") continue;
    try {
      const payload = JSON.parse(raw);
      if (payload && payload.kind && payload.sequence) events.push(payload);
    } catch {
      /* Heartbeats and comments are not events. */
    }
  }
  return events.sort((left, right) => left.sequence - right.sequence);
}

function integrityBanner(integrity) {
  if (!integrity) return null;
  if (!integrity.verified) {
    return el("div", { class: "banner banner-bad" }, [
      el("div", {}, [
        el("div", { class: "banner-title", text: "Hash chain does not verify" }),
        el("div", {
          text: `Verification failed with ${integrity.reason ?? "an unknown error"}. This run's history cannot be trusted.`,
        }),
      ]),
    ]);
  }
  if (integrity.resumed) {
    return el("div", { class: "banner banner-warn" }, [
      el("div", {}, [
        el("div", {
          class: "banner-title",
          text: `Interrupted and continued — ${integrity.executions} executions`,
        }),
        el("div", {
          text:
            `This run stopped mid-flight and a later execution picked it up from durable facts. ` +
            `The chain still verifies from sequence ${integrity.first_sequence} to ${integrity.last_sequence}, ` +
            `so nothing was patched up after the fact.`,
        }),
      ]),
    ]);
  }
  return el("div", { class: "banner banner-ok" }, [
    el("div", {}, [
      el("div", { class: "banner-title", text: "Hash chain verified" }),
      el("div", {
        text: `${integrity.events} events link from sequence ${integrity.first_sequence} to ${integrity.last_sequence} with no gap, insertion, or backfill.`,
      }),
    ]),
  ]);
}

export async function renderEvidenceView(root, { runId, navigate }) {
  root.replaceChildren(el("div", { class: "view-scroll" }, [el("p", { class: "faint", text: "Loading chain…" })]));

  let events = [];
  let integrity = null;
  try {
    [events, integrity] = await Promise.all([collectEvents(runId), api.integrity(runId)]);
  } catch (error) {
    root.replaceChildren(emptyState("Could not load the chain", error.message));
    return;
  }

  if (!events.length) {
    root.replaceChildren(emptyState("Empty chain", "This run has no durable events."));
    return;
  }

  const metrics = projectMetrics(events);
  const executions = projectExecutions(events);
  const detail = el("div", { class: "card" }, [
    el("div", { class: "card-head" }, [el("h3", { text: "Selected event" })]),
    el("div", { class: "card-body" }, [
      el("p", { class: "faint", style: "font-size:12px", text: "Pick a row to inspect its payload." }),
    ]),
  ]);

  const onSelect = (event) => {
    mount(detail, [
      el("div", { class: "card-head" }, [
        el("h3", { text: `Event #${event.sequence}` }),
        badge(event.kind, "accent"),
      ]),
      el("div", { class: "card-body stack gap-5" }, [
        kv([
          ["Kind", event.kind],
          ["Step", event.step],
          ["Call", event.call_key],
          ["Execution", shortId(event.execution_id, 12)],
          ["Checkpoint", event.safe_checkpoint ? "safe" : "no"],
          ["Terminal", event.terminal ? "yes" : "no"],
        ]),
        el("pre", {
          class: "mono",
          style:
            "margin:0;padding:10px;background:var(--surface-sunken);border:1px solid var(--line);" +
            "border-radius:5px;overflow:auto;max-height:280px;white-space:pre-wrap;word-break:break-word",
          text: JSON.stringify(event.data ?? {}, null, 2),
        }),
      ]),
    ]);
  };

  const forkButton = el("button", {
    class: "btn btn-sm",
    text: "Fork from last safe checkpoint",
    onClick: async (clicked) => {
      clicked.currentTarget.disabled = true;
      try {
        const handle = await api.fork(runId);
        toast("Forked. The new run replays this history and diverges from there.");
        navigate(`/workspace/${handle.run_id}`);
      } catch (error) {
        toast(error.message, "bad");
        clicked.currentTarget.disabled = false;
      }
    },
  });

  root.replaceChildren(
    el("div", { class: "view-scroll", style: "padding:0" }, [
      el("div", { class: "stack gap-5", style: "padding:16px 20px" }, [
        el("div", { class: "row gap-4" }, [
          el("div", { class: "grow" }, [
            el("h1", { text: "Evidence" }),
            el("p", { class: "muted", style: "font-size:12px;margin-top:2px" }, [
              el("code", { text: runId }),
            ]),
          ]),
          forkButton,
          el("button", {
            class: "btn btn-sm",
            text: "Back to patch",
            onClick: () => navigate(`/workspace/${runId}`),
          }),
        ]),
        integrityBanner(integrity),
        el("div", { class: "metrics", style: "border:1px solid var(--line);border-radius:5px" }, [
          ["Events", count(events.length)],
          ["Executions", count(executions.length)],
          ["Steps", count(metrics.step)],
          ["Tools", count(metrics.toolExecutions)],
          ["Tokens", count(metrics.tokens)],
          ["Cost", money(metrics.costMicros, metrics.currency)],
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
        detail,
      ]),
      el("div", { style: "border-top:1px solid var(--line)" }, [
        renderTimeline(events, { onSelect }),
      ]),
    ]),
  );
}
