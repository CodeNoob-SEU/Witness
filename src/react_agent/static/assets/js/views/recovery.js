/* Recovery view: a real crash-and-resume run, replayed from a recording.
 *
 * This is the console's argument. Every competing product treats a failed run
 * as disposable — discard the worktree, start over — so none of them has a
 * screen for "this run was interrupted and continued". This one does, and it
 * is backed by an actual `SIGKILL` against an actual PostgreSQL journal rather
 * than by a simulation.
 *
 * The recording is produced by `scripts/record_chaos.py`, which runs the
 * unmodified `examples/chaos_resume.py`. When no recording has been generated,
 * this view says exactly that instead of showing something invented.
 */

import { el, badge, kv, mount } from "../dom.js";
import { count, shortId } from "../format.js";
import { renderTimeline } from "../components/timeline.js";

const RECORDINGS = "/assets/recordings";

async function loadIndex() {
  const response = await fetch(`${RECORDINGS}/index.json`, {
    headers: { accept: "application/json" },
  });
  if (!response.ok) return [];
  return response.json();
}

async function loadRecording(name) {
  const response = await fetch(`${RECORDINGS}/${encodeURIComponent(name)}.json`, {
    headers: { accept: "application/json" },
  });
  if (!response.ok) throw new Error(`Recording ${name} is missing.`);
  return response.json();
}

function notRecordedYet() {
  return el("div", { class: "view-scroll" }, [
    el("div", { class: "view-wide stack gap-6" }, [
      el("div", {}, [
        el("h1", { text: "Recovery" }),
        el("p", {
          class: "muted",
          style: "font-size:12px;margin-top:2px",
          text: "Crash, takeover, and fencing — shown from a real recorded run.",
        }),
      ]),
      el("div", { class: "banner banner-warn" }, [
        el("div", {}, [
          el("div", { class: "banner-title", text: "No recording has been generated yet" }),
          el("div", {
            style: "margin-bottom:8px",
            text:
              "A crash recording needs a journal that survives the process, so it cannot be " +
              "produced against the in-memory journal this demo runs on. Point the recorder at " +
              "a PostgreSQL 16+ database and it will run the real scenario — real child " +
              "processes, a real SIGKILL — and write the resulting chain here.",
          }),
          el("pre", {
            class: "mono",
            style:
              "margin:0;padding:10px;background:var(--surface-sunken);border:1px solid var(--line);" +
              "border-radius:5px;overflow:auto;white-space:pre-wrap",
            text:
              "export REACT_AGENT_POSTGRES_DSN=postgresql://user:pass@host:5432/db\n" +
              "uv run python scripts/record_chaos.py",
          }),
        ]),
      ]),
      el("div", { class: "card" }, [
        el("div", { class: "card-head" }, [el("h3", { text: "What the recording will show" })]),
        el(
          "div",
          { class: "card-body stack gap-4", style: "font-size:12px;line-height:1.6" },
          [
            el("div", {}, [
              el("strong", { text: "The kill lands after the point of no return. " }),
              "Worker A is killed immediately after PostgreSQL commits tool_started — the exact moment a side effect may already have happened.",
            ]),
            el("div", {}, [
              el("strong", { text: "The work continues rather than restarting. " }),
              "Worker B resumes from durable facts alone and reaches the final answer.",
            ]),
            el("div", {}, [
              el("strong", { text: "The side effect is deduplicable. " }),
              "Both attempts share one stable idempotency key, so a real service can collapse them.",
            ]),
            el("div", {}, [
              el("strong", { text: "The history was not repaired afterwards. " }),
              "The hash chain still verifies from sequence 1, so nothing was inserted or back-dated to make the recovery look clean.",
            ]),
          ],
        ),
      ]),
    ]),
  ]);
}

function sideEffectsCard(recording) {
  if (!recording.side_effects?.length) return null;
  const keys = new Set(recording.side_effects.map((row) => row.idempotency_key));
  return el("div", { class: "card" }, [
    el("div", { class: "card-head" }, [
      el("h3", { text: "Side effects actually performed" }),
      keys.size === 1
        ? badge("one idempotency key", "ok")
        : badge(`${keys.size} keys`, "warn"),
    ]),
    el("div", { class: "card-body stack gap-4" }, [
      el("table", { class: "table", style: "font-size:11.5px" }, [
        el("thead", {}, [
          el("tr", {}, [
            el("th", { class: "num-cell", text: "Attempt" }),
            el("th", { text: "Idempotency key" }),
            el("th", { text: "Marker" }),
          ]),
        ]),
        el(
          "tbody",
          {},
          recording.side_effects.map((row) =>
            el("tr", { style: "cursor:default" }, [
              el("td", { class: "num-cell", text: row.attempt }),
              el("td", {}, [el("code", { text: row.idempotency_key })]),
              el("td", { class: "muted", text: row.marker }),
            ]),
          ),
        ),
      ]),
      el("p", {
        class: "faint",
        style: "font-size:11.5px;line-height:1.55;margin:0",
        text:
          keys.size === 1
            ? "The tool ran twice — once before the crash, once after the resume — under a single stable key. A downstream service can dedupe on that key. A tool that could not make this promise would stop and wait for an operator instead of retrying."
            : "The attempts do not share a key, so they cannot be deduplicated downstream.",
      }),
    ]),
  ]);
}

function crashBanner(recording) {
  const integrity = recording.integrity ?? {};
  return el("div", { class: "banner banner-warn" }, [
    el("div", {}, [
      el("div", {
        class: "banner-title",
        text: `Interrupted and continued — ${integrity.executions} executions`,
      }),
      el("div", { text: recording.summary }),
    ]),
  ]);
}

function integrityBanner(integrity) {
  const ok = integrity?.verified;
  return el("div", { class: `banner banner-${ok ? "ok" : "bad"}` }, [
    el("div", {}, [
      el("div", {
        class: "banner-title",
        text: ok ? "Hash chain verified after the crash" : "Hash chain does not verify",
      }),
      el("div", {
        text: ok
          ? `All ${integrity.events} events still link from sequence ${integrity.first_sequence} to ${integrity.last_sequence}. The recovery did not backfill, repair, or re-date any part of the history to make itself look clean.`
          : `Verification failed with ${integrity.reason ?? "an unknown error"}.`,
      }),
    ]),
  ]);
}

export async function renderRecovery(root) {
  root.replaceChildren(
    el("div", { class: "view-scroll" }, [el("p", { class: "faint", text: "Loading recordings…" })]),
  );

  let index = [];
  try {
    index = await loadIndex();
  } catch {
    index = [];
  }
  if (!index.length) {
    root.replaceChildren(notRecordedYet());
    return;
  }

  const recording = await loadRecording(index[0].name);
  const integrity = recording.integrity ?? {};
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
          ["Step", event.step],
          ["Call", event.call_key],
          ["Execution", shortId(event.execution_id, 12)],
          ["Checkpoint", event.safe_checkpoint ? "safe" : "no"],
        ]),
        el("pre", {
          class: "mono",
          style:
            "margin:0;padding:10px;background:var(--surface-sunken);border:1px solid var(--line);" +
            "border-radius:5px;overflow:auto;max-height:260px;white-space:pre-wrap;word-break:break-word",
          text: JSON.stringify(event.data ?? {}, null, 2),
        }),
      ]),
    ]);
  };

  root.replaceChildren(
    el("div", { class: "view-scroll", style: "padding:0" }, [
      el("div", { class: "stack gap-5", style: "padding:16px 20px" }, [
        el("div", { class: "row gap-4" }, [
          el("div", { class: "grow" }, [
            el("h1", { text: recording.title }),
            el("p", { class: "muted", style: "font-size:12px;margin-top:2px" }, [
              "Recorded from ",
              el("code", { text: recording.source }),
              " · run ",
              el("code", { text: shortId(recording.run_id, 12) }),
            ]),
          ]),
          // Labelled, not implied: this is a replay of a real run, and the UI
          // should never let that read as something happening live.
          badge("recorded run", "accent"),
        ]),
        crashBanner(recording),
        integrityBanner(integrity),
        el("div", { class: "metrics", style: "border:1px solid var(--line);border-radius:5px" }, [
          ["Events", count(integrity.events)],
          ["Executions", count(integrity.executions)],
          ["Status", recording.status ?? "—"],
          ["Stop reason", recording.stop_reason ?? "—"],
        ].map(([label, value]) =>
          el("div", { class: "metric" }, [
            el("span", { class: "metric-label", text: label }),
            el("span", { class: "metric-value", text: String(value) }),
          ]),
        )),
        sideEffectsCard(recording),
        detail,
      ]),
      el("div", { style: "border-top:1px solid var(--line)" }, [
        renderTimeline(recording.events ?? [], { onSelect }),
      ]),
    ]),
  );
}
